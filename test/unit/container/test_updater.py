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

import pickle
from unittest import mock
import os
import unittest
from contextlib import closing
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp
from test.debug_logger import debug_logger
from test.unit import mock_check_drive

from eventlet import spawn, Timeout

from swift.common import exceptions, utils
from swift.container import updater as container_updater
from swift.container.backend import ContainerBroker, DATADIR
from swift.common.ring import RingData
from swift.common.utils import normalize_timestamp

from test import listen_zero


class TestContainerUpdater(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b'startcap'
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_container_updater')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        ring_file = os.path.join(self.testdir, 'account.ring.gz')
        with closing(GzipFile(ring_file, 'wb')) as f:
            pickle.dump(
                RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
                         [{'id': 0, 'ip': '127.0.0.2', 'port': 12345,
                           'replication_ip': '127.0.0.1',
                           # replication_port may be overridden in tests but
                           # include here for completeness...
                           'replication_port': 67890,
                           'device': 'sda1', 'zone': 0},
                          {'id': 1, 'ip': '127.0.0.2', 'port': 12345,
                           'replication_ip': '127.0.0.1',
                           'replication_port': 67890,
                           'device': 'sda1', 'zone': 2}], 30),
                f)
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)
        self.logger = debug_logger('test')

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
        return container_updater.ContainerUpdater(conf, logger=self.logger)

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

    def test_conf_params(self):
        # defaults
        daemon = container_updater.ContainerUpdater({})
        self.assertEqual(daemon.devices, '/srv/node')
        self.assertEqual(daemon.mount_check, True)
        self.assertEqual(daemon.swift_dir, '/etc/swift')
        self.assertEqual(daemon.interval, 300)
        self.assertEqual(daemon.concurrency, 4)
        self.assertEqual(daemon.max_containers_per_second, 50.0)

        # non-defaults
        conf = {
            'devices': '/some/where/else',
            'mount_check': 'huh?',
            'swift_dir': '/not/here',
            'interval': '600.1',
            'concurrency': '2',
            'containers_per_second': '10.5',
        }
        daemon = container_updater.ContainerUpdater(conf)
        self.assertEqual(daemon.devices, '/some/where/else')
        self.assertEqual(daemon.mount_check, False)
        self.assertEqual(daemon.swift_dir, '/not/here')
        self.assertEqual(daemon.interval, 600.1)
        self.assertEqual(daemon.concurrency, 2)
        self.assertEqual(daemon.max_containers_per_second, 10.5)

        # check deprecated option
        daemon = container_updater.ContainerUpdater({'slowdown': '0.04'})
        self.assertEqual(daemon.max_containers_per_second, 20.0)

        def check_bad(conf):
            with self.assertRaises(ValueError):
                container_updater.ContainerUpdater(conf)

        check_bad({'interval': 'foo'})
        check_bad({'concurrency': 'bar'})
        check_bad({'concurrency': '1.0'})
        check_bad({'slowdown': 'baz'})
        check_bad({'containers_per_second': 'quux'})

    @mock.patch.object(container_updater.ContainerUpdater, 'container_sweep')
    def test_run_once_with_device_unmounted(self, mock_sweep):
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
        with mock_check_drive():
            cu.run_once()
        log_lines = self.logger.get_lines_for_level('warning')
        self.assertGreater(len(log_lines), 0)
        msg = '%s is not mounted' % self.sda1
        self.assertEqual(log_lines[0], msg)
        # Ensure that the container_sweep did not run
        self.assertFalse(mock_sweep.called)

    @mock.patch('swift.container.updater.dump_recon_cache')
    def test_run_once_with_get_info_timeout(self, mock_dump_recon):
        cu = self._get_container_updater()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        db_file = os.path.join(subdir, 'hash.db')
        cb = ContainerBroker(db_file, account='a', container='c')
        cb.initialize(normalize_timestamp(1), 0)

        timeout = exceptions.LockTimeout(10, db_file)
        timeout.cancel()
        with mock.patch('swift.container.updater.ContainerBroker.get_info',
                        side_effect=timeout):
            cu.run_once()
        log_lines = self.logger.get_lines_for_level('info')
        self.assertIn('Failed to get container info (Lock timeout: '
                      '10 seconds: %s); skipping.' % db_file, log_lines)

    @mock.patch('swift.container.updater.dump_recon_cache')
    @mock.patch('swift.container.updater.ContainerUpdater.process_container',
                side_effect=Exception('Boom!'))
    def test_error_in_process(self, mock_process, mock_dump_recon):
        cu = self._get_container_updater()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'), account='a',
                             container='c', pending_timeout=1)
        cb.initialize(normalize_timestamp(1), 0)

        cu.run_once()

        log_lines = self.logger.get_lines_for_level('error')
        self.assertTrue(log_lines)
        self.assertIn('Error processing container ', log_lines[0])
        self.assertIn('devices/sda1/containers/subdir/hash.db', log_lines[0])
        self.assertIn('Boom!', log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertEqual(1, len(mock_dump_recon.mock_calls))

    @mock.patch('swift.container.updater.dump_recon_cache')
    def test_run_once(self, mock_recon):
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
        self.assertTrue(cb.is_root_container())
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
                    out.write(b'HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     b'PUT /sda1/0/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0].lower()] = \
                            line.split(b':')[1].strip()
                        line = inc.readline()
                    self.assertIn(b'x-put-timestamp', headers)
                    self.assertIn(b'x-delete-timestamp', headers)
                    self.assertIn(b'x-object-count', headers)
                    self.assertIn(b'x-bytes-used', headers)
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None
        bindsock = listen_zero()

        def spawn_accepts():
            events = []
            for _junk in range(2):
                sock, addr = bindsock.accept()
                events.append(spawn(accept, sock, addr, 201))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['replication_port'] = bindsock.getsockname()[1]
        cu.run_once()
        with Timeout(5):
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
        paths = cu.get_paths()
        self.assertEqual(paths, [])
        log_lines = self.logger.get_lines_for_level('error')
        msg = ('ERROR:  Failed to get paths to drive partitions: '
               'permission_denied')
        self.assertEqual(log_lines[0], msg)

    @mock.patch('os.listdir', return_value=['foo', 'bar'])
    def test_listdir_without_exception(self, mock_listdir):
        cu = self._get_container_updater()
        path = cu._listdir('foo/bar/')
        self.assertEqual(path, ['foo', 'bar'])
        log_lines = self.logger.get_lines_for_level('error')
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
        obj_name = u'\N{GREEK CAPITAL LETTER OMEGA}'
        cb.put_object(obj_name, normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940')

        def accept(sock, addr):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write(b'HTTP/1.1 201 OK\r\nContent-Length: 0\r\n\r\n')
                    out.flush()
                    inc.read()
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None

        bindsock = listen_zero()

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
                dev['replication_port'] = bindsock.getsockname()[1]
        cu.run_once()
        with Timeout(5):
            for event in spawned.wait():
                err = event.wait()
                if err:
                    raise err
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_object_count'], 1)
        self.assertEqual(info['reported_bytes_used'], 3)

    def test_old_style_shard_container(self):
        cu = self._get_container_updater()
        cu.run_once()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        cu.run_once()
        self.assertTrue(os.path.exists(containers_dir))
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'),
                             account='.shards_a', container='c')
        cb.initialize(normalize_timestamp(1), 0)
        cb.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(cb.is_root_container())
        cu.run_once()
        info = cb.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)
        self.assertEqual(info['reported_put_timestamp'], '0')
        self.assertEqual(info['reported_delete_timestamp'], '0')
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        cb.put_object('o', normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940')
        # Fake us having already reported *bad* stats under swift 2.18.0
        cb.reported('0', '0', 1, 3)

        # Should fail with a bunch of connection-refused
        cu.run_once()
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_put_timestamp'], '0')
        self.assertEqual(info['reported_delete_timestamp'], '0')
        self.assertEqual(info['reported_object_count'], 1)
        self.assertEqual(info['reported_bytes_used'], 3)

        def accept(sock, addr, return_code):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write(b'HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     b'PUT /sda1/2/.shards_a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0].lower()] = \
                            line.split(b':')[1].strip()
                        line = inc.readline()
                    self.assertIn(b'x-put-timestamp', headers)
                    self.assertIn(b'x-delete-timestamp', headers)
                    self.assertIn(b'x-object-count', headers)
                    self.assertIn(b'x-bytes-used', headers)
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None
        bindsock = listen_zero()

        def spawn_accepts():
            events = []
            for _junk in range(2):
                sock, addr = bindsock.accept()
                events.append(spawn(accept, sock, addr, 201))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['replication_port'] = bindsock.getsockname()[1]
        cu.run_once()
        with Timeout(5):
            for event in spawned.wait():
                err = event.wait()
                if err:
                    raise err
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_put_timestamp'], '0000000001.00000')
        self.assertEqual(info['reported_delete_timestamp'], '0')
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

    def test_shard_container(self):
        cu = self._get_container_updater()
        cu.run_once()
        containers_dir = os.path.join(self.sda1, DATADIR)
        os.mkdir(containers_dir)
        cu.run_once()
        self.assertTrue(os.path.exists(containers_dir))
        subdir = os.path.join(containers_dir, 'subdir')
        os.mkdir(subdir)
        cb = ContainerBroker(os.path.join(subdir, 'hash.db'),
                             account='.shards_a', container='c')
        cb.initialize(normalize_timestamp(1), 0)
        cb.set_sharding_sysmeta('Quoted-Root', 'a/c')
        self.assertFalse(cb.is_root_container())
        cu.run_once()
        info = cb.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)
        self.assertEqual(info['reported_put_timestamp'], '0')
        self.assertEqual(info['reported_delete_timestamp'], '0')
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        cb.put_object('o', normalize_timestamp(2), 3, 'text/plain',
                      '68b329da9893e34099c7d8ad5cb9c940')
        # Fake us having already reported *bad* stats under swift 2.18.0
        cb.reported('0', '0', 1, 3)

        # Should fail with a bunch of connection-refused
        cu.run_once()
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_put_timestamp'], '0')
        self.assertEqual(info['reported_delete_timestamp'], '0')
        self.assertEqual(info['reported_object_count'], 1)
        self.assertEqual(info['reported_bytes_used'], 3)

        def accept(sock, addr, return_code):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write(b'HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     b'PUT /sda1/2/.shards_a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0].lower()] = \
                            line.split(b':')[1].strip()
                        line = inc.readline()
                    self.assertIn(b'x-put-timestamp', headers)
                    self.assertIn(b'x-delete-timestamp', headers)
                    self.assertIn(b'x-object-count', headers)
                    self.assertIn(b'x-bytes-used', headers)
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None
        bindsock = listen_zero()

        def spawn_accepts():
            events = []
            for _junk in range(2):
                sock, addr = bindsock.accept()
                events.append(spawn(accept, sock, addr, 201))
            return events

        spawned = spawn(spawn_accepts)
        for dev in cu.get_account_ring().devs:
            if dev is not None:
                dev['replication_port'] = bindsock.getsockname()[1]
        cu.run_once()
        with Timeout(5):
            for event in spawned.wait():
                err = event.wait()
                if err:
                    raise err
        info = cb.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 3)
        self.assertEqual(info['reported_put_timestamp'], '0000000001.00000')
        self.assertEqual(info['reported_delete_timestamp'], '0')
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)


if __name__ == '__main__':
    unittest.main()
