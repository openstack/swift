# Copyright (c) 2010-2012 OpenStack, LLC.
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

import cPickle as pickle
import json
import os
import unittest
from gzip import GzipFile
from shutil import rmtree
from time import time
from distutils.dir_util import mkpath

from eventlet import spawn, Timeout, listen

from swift.obj import updater as object_updater, server as object_server
from swift.obj.server import ASYNCDIR
from swift.common.ring import RingData
from swift.common import utils
from swift.common.utils import hash_path, normalize_timestamp, mkdirs, \
    write_pickle
from test.unit import FakeLogger


class TestObjectUpdater(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = os.path.join(os.path.dirname(__file__),
                                    'object_updater')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        pickle.dump(
            RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                     [{'id': 0, 'ip': '127.0.0.1', 'port': 1, 'device': 'sda1',
                       'zone': 0},
                      {'id': 1, 'ip': '127.0.0.1', 'port': 1, 'device': 'sda1',
                       'zone': 2}], 30),
            GzipFile(os.path.join(self.testdir, 'container.ring.gz'), 'wb'))
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)
        os.mkdir(os.path.join(self.sda1, 'tmp'))

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_creation(self):
        cu = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '2',
            'node_timeout': '5'})
        self.assert_(hasattr(cu, 'logger'))
        self.assert_(cu.logger is not None)
        self.assertEquals(cu.devices, self.devices_dir)
        self.assertEquals(cu.interval, 1)
        self.assertEquals(cu.concurrency, 2)
        self.assertEquals(cu.node_timeout, 5)
        self.assert_(cu.get_container_ring() is not None)

    def test_object_sweep(self):
        prefix_dir = os.path.join(self.sda1, ASYNCDIR, 'abc')
        mkpath(prefix_dir)

        objects = {
            'a': [1089.3, 18.37, 12.83, 1.3],
            'b': [49.4, 49.3, 49.2, 49.1],
            'c': [109984.123],
        }

        expected = set()
        for o, timestamps in objects.iteritems():
            ohash = hash_path('account', 'container', o)
            for t in timestamps:
                o_path = os.path.join(prefix_dir, ohash + '-' +
                                      normalize_timestamp(t))
                if t == timestamps[0]:
                    expected.add(o_path)
                write_pickle({}, o_path)

        seen = set()

        class MockObjectUpdater(object_updater.ObjectUpdater):
            def process_object_update(self, update_path, device):
                seen.add(update_path)
                os.unlink(update_path)

        cu = MockObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '5'})
        cu.object_sweep(self.sda1)
        self.assert_(not os.path.exists(prefix_dir))
        self.assertEqual(expected, seen)

    def test_run_once(self):
        cu = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'})
        cu.run_once()
        async_dir = os.path.join(self.sda1, object_server.ASYNCDIR)
        os.mkdir(async_dir)
        cu.run_once()
        self.assert_(os.path.exists(async_dir))

        odd_dir = os.path.join(async_dir, 'not really supposed to be here')
        os.mkdir(odd_dir)
        cu.run_once()
        self.assert_(os.path.exists(async_dir))
        self.assert_(not os.path.exists(odd_dir))

        ohash = hash_path('a', 'c', 'o')
        odir = os.path.join(async_dir, ohash[-3:])
        mkdirs(odir)
        older_op_path = os.path.join(
            odir,
            '%s-%s' % (ohash, normalize_timestamp(time() - 1)))
        op_path = os.path.join(
            odir,
            '%s-%s' % (ohash, normalize_timestamp(time())))
        for path in (op_path, older_op_path):
            with open(path, 'wb') as async_pending:
                pickle.dump({'op': 'PUT', 'account': 'a', 'container': 'c',
                             'obj': 'o', 'headers': {
                            'X-Container-Timestamp': normalize_timestamp(0)}},
                            async_pending)
        cu.logger = FakeLogger()
        cu.run_once()
        self.assert_(not os.path.exists(older_op_path))
        self.assert_(os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
                         {'failures': 1, 'unlinks': 1})

        bindsock = listen(('127.0.0.1', 0))

        def accepter(sock, return_code):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEquals(inc.readline(),
                                      'PUT /sda1/0/a/c/o HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = inc.readline()
                    self.assert_('x-container-timestamp' in headers)
            except BaseException, err:
                return err
            return None

        def accept(return_codes):
            codes = iter(return_codes)
            try:
                events = []
                for x in xrange(len(return_codes)):
                    with Timeout(3):
                        sock, addr = bindsock.accept()
                        events.append(
                            spawn(accepter, sock, codes.next()))
                for event in events:
                    err = event.wait()
                    if err:
                        raise err
            except BaseException, err:
                return err
            return None

        event = spawn(accept, [201, 500])
        for dev in cu.get_container_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]

        cu.logger = FakeLogger()
        cu.run_once()
        err = event.wait()
        if err:
            raise err
        self.assert_(os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
                         {'failures': 1})

        event = spawn(accept, [201])
        cu.logger = FakeLogger()
        cu.run_once()
        err = event.wait()
        if err:
            raise err
        self.assert_(not os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
                         {'unlinks': 1, 'successes': 1})


if __name__ == '__main__':
    unittest.main()
