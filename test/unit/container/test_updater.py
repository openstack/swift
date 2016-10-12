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

import six.moves.cPickle as pickle
import mock
import os
import unittest
from contextlib import closing
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp
from test.unit import FakeLogger

from eventlet import spawn, Timeout, listen

from swift.common import utils
from swift.container import updater as container_updater
from swift.container.backend import ContainerBroker, DATADIR
from swift.common.ring import RingData
from swift.common.utils import normalize_timestamp


class TestContainerUpdater(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_container_updater')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        ring_file = os.path.join(self.testdir, 'account.ring.gz')
        with closing(GzipFile(ring_file, 'wb')) as f:
            pickle.dump(
                RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                         [{'id': 0, 'ip': '127.0.0.1', 'port': 12345,
                           'device': 'sda1', 'zone': 0},
                          {'id': 1, 'ip': '127.0.0.1', 'port': 12345,
                           'device': 'sda1', 'zone': 2}], 30),
                f)
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def _get_container_updater(self, conf_updates=None):
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15',
            'account_suppression_time': 0
        }
        if conf_updates:
            conf.update(conf_updates)
        return container_updater.ContainerUpdater(conf)

    def test_creation(self):
        cu = self._get_container_updater({'concurrency': '2',
                                          'node_timeout': '5.5'})
        self.assertTrue(hasattr(cu, 'logger'))
        self.assertTrue(cu.logger is not None)
        self.assertEqual(cu.devices, self.devices_dir)
        self.assertEqual(cu.interval, 1)
        self.assertEqual(cu.concurrency, 2)
        self.assertEqual(cu.node_timeout, 5.5)
        self.assertEqual(cu.account_suppression_time, 0)
        self.assertTrue(cu.get_account_ring() is not None)

    @mock.patch.object(container_updater, 'ismount')
    @mock.patch.object(container_updater.ContainerUpdater, 'container_sweep')
    def test_run_once_with_device_unmounted(self, mock_sweep, mock_ismount):

        mock_ismount.return_value = False
        cu = self._get_container_updater()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        partition_dir = os.path.join(containers_dir, "a")
        os.mkdir(partition_dir)

        cu.run_once()
        self.assertTrue(os.path.exists(containers_dir))  # sanity check

        # only called if a partition dir exists
        self.assertTrue(mock_sweep.called)

        mock_sweep.reset_mock()
        cu = self._get_container_updater({'mount_check': 'true'})
        cu.logger = FakeLogger()
        cu.run_once()
        log_lines = cu.logger.get_lines_for_level('warning')
        self.assertTrue(len(log_lines) > 0)
        msg = 'sda1 is not mounted'
        self.assertEqual(log_lines[0], msg)
        # Ensure that the container_sweep did not run
        self.assertFalse(mock_sweep.called)

    def test_run_once(self):
        cu = self._get_container_updater()
        cu.run_once()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        cu.run_once()
        self.assertTrue(os.path.exists(containers_dir))
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'), account='a',
                             container='c')
        cb.initialize(normalize_timestamp(1), 0)
        cu.run_once()
        info = cb.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        cb.put_object('o', normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940')
        cu.run_once()
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        def accept(sock, addr, return_code):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     'PUT /sda1/0/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = inc.readline()
                    self.assertTrue('x-put-timestamp' in headers)
                    self.assertTrue('x-delete-timestamp' in headers)
                    self.assertTrue('x-object-count' in headers)
                    self.assertTrue('x-bytes-used' in headers)
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None
        bindsock = listen(('127.0.0.1', 0))

        def spawn_accepts():
            events = []
            for _junk in range(2):
                sock, addr = bindsock.accept()
                events.append(spawn(accept, sock, addr, 201))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]
        cu.run_once()
        for event in spawned.wait():
            err = event.wait()
            if err:
                raise err
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_object_count'], 1)
        self.assertEqual(info['reported_bytes_used'], 3)

    @mock.patch('os.listdir')
    def test_listdir_with_exception(self, mock_listdir):
        e = OSError('permission_denied')
        mock_listdir.side_effect = e
        cu = self._get_container_updater()
        cu.logger = FakeLogger()
        paths = cu.get_paths()
        self.assertEqual(paths, [])
        log_lines = cu.logger.get_lines_for_level('error')
        msg = ('ERROR:  Failed to get paths to drive partitions: '
               'permission_denied')
        self.assertEqual(log_lines[0], msg)

    @mock.patch('os.listdir', return_value=['foo', 'bar'])
    def test_listdir_without_exception(self, mock_listdir):
        cu = self._get_container_updater()
        cu.logger = FakeLogger()
        path = cu._listdir('foo/bar/')
        self.assertEqual(path, ['foo', 'bar'])
        log_lines = cu.logger.get_lines_for_level('error')
        self.assertEqual(len(log_lines), 0)

    def test_unicode(self):
        cu = self._get_container_updater()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'), account='a',
                             container='\xce\xa9')
        cb.initialize(normalize_timestamp(1), 0)
        cb.put_object('\xce\xa9', normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940')

        def accept(sock, addr):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 201 OK\r\nContent-Length: 0\r\n\r\n')
                    out.flush()
                    inc.read()
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None

        bindsock = listen(('127.0.0.1', 0))

        def spawn_accepts():
            events = []
            for _junk in range(2):
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    events.append(spawn(accept, sock, addr))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]
        cu.run_once()
        for event in spawned.wait():
            err = event.wait()
            if err:
                raise err
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_object_count'], 1)
        self.assertEqual(info['reported_bytes_used'], 3)

if __name__ == '__main__':
    unittest.main()
