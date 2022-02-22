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
from queue import PriorityQueue

import eventlet
import six.moves.cPickle as pickle
import mock
import os
import unittest
import random
import itertools
from collections import Counter
from contextlib import closing
from gzip import GzipFile
from tempfile import mkdtemp
from shutil import rmtree

from swift.common.exceptions import ConnectionTimeout
from test import listen_zero
from test.debug_logger import debug_logger
from test.unit import (
    make_timestamp_iter, patch_policies, mocked_http_conn)
from time import time
from distutils.dir_util import mkpath

from eventlet import spawn, Timeout

from swift.obj import updater as object_updater
from swift.obj.diskfile import (
    ASYNCDIR_BASE, get_async_dir, DiskFileManager, get_tmp_dir)
from swift.common.ring import RingData
from swift.common import utils
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import bytes_to_wsgi
from swift.common.utils import hash_path, normalize_timestamp, mkdirs
from swift.common.storage_policy import StoragePolicy, POLICIES


class MockPool(object):
    def __init__(self, *a, **kw):
        pass

    def spawn(self, func, *args, **kwargs):
        func(*args, **kwargs)

    def waitall(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a, **kw):
        pass


_mocked_policies = [StoragePolicy(0, 'zero', False),
                    StoragePolicy(1, 'one', True)]


@patch_policies(_mocked_policies)
class TestObjectUpdater(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b''
        self.testdir = mkdtemp()
        ring_file = os.path.join(self.testdir, 'container.ring.gz')
        with closing(GzipFile(ring_file, 'wb')) as f:
            pickle.dump(
                RingData([[0, 1, 2, 0, 1, 2],
                          [1, 2, 0, 1, 2, 0],
                          [2, 3, 1, 2, 3, 1]],
                         [{'id': 0, 'ip': '127.0.0.2', 'port': 1,
                           'replication_ip': '127.0.0.1',
                           # replication_port may be overridden in tests but
                           # include here for completeness...
                           'replication_port': 67890,
                           'device': 'sda1', 'zone': 0},
                          {'id': 1, 'ip': '127.0.0.2', 'port': 1,
                           'replication_ip': '127.0.0.1',
                           'replication_port': 67890,
                           'device': 'sda1', 'zone': 2},
                          {'id': 2, 'ip': '127.0.0.2', 'port': 1,
                           'replication_ip': '127.0.0.1',
                           'replication_port': 67890,
                           'device': 'sda1', 'zone': 4},
                          {'id': 3, 'ip': '127.0.0.2', 'port': 1,
                           'replication_ip': '127.0.0.1',
                           'replication_port': 67890,
                           'device': 'sda1', 'zone': 6}], 30),
                f)
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)
        for policy in POLICIES:
            os.mkdir(os.path.join(self.sda1, get_tmp_dir(policy)))
        self.logger = debug_logger()
        self.ts_iter = make_timestamp_iter()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_creation(self):
        ou = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '2',
            'node_timeout': '5.5'})
        self.assertTrue(hasattr(ou, 'logger'))
        self.assertTrue(ou.logger is not None)
        self.assertEqual(ou.devices, self.devices_dir)
        self.assertEqual(ou.interval, 1)
        self.assertEqual(ou.concurrency, 2)
        self.assertEqual(ou.node_timeout, 5.5)
        self.assertTrue(ou.get_container_ring() is not None)

    def test_conf_params(self):
        # defaults
        daemon = object_updater.ObjectUpdater({}, logger=self.logger)
        self.assertEqual(daemon.devices, '/srv/node')
        self.assertEqual(daemon.mount_check, True)
        self.assertEqual(daemon.swift_dir, '/etc/swift')
        self.assertEqual(daemon.interval, 300)
        self.assertEqual(daemon.concurrency, 8)
        self.assertEqual(daemon.updater_workers, 1)
        self.assertEqual(daemon.max_objects_per_second, 50.0)
        self.assertEqual(daemon.max_objects_per_container_per_second, 0.0)
        self.assertEqual(daemon.per_container_ratelimit_buckets, 1000)
        self.assertEqual(daemon.max_deferred_updates, 10000)

        # non-defaults
        conf = {
            'devices': '/some/where/else',
            'mount_check': 'huh?',
            'swift_dir': '/not/here',
            'interval': '600.1',
            'concurrency': '2',
            'updater_workers': '3',
            'objects_per_second': '10.5',
            'max_objects_per_container_per_second': '1.2',
            'per_container_ratelimit_buckets': '100',
            'max_deferred_updates': '0',
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.assertEqual(daemon.devices, '/some/where/else')
        self.assertEqual(daemon.mount_check, False)
        self.assertEqual(daemon.swift_dir, '/not/here')
        self.assertEqual(daemon.interval, 600.1)
        self.assertEqual(daemon.concurrency, 2)
        self.assertEqual(daemon.updater_workers, 3)
        self.assertEqual(daemon.max_objects_per_second, 10.5)
        self.assertEqual(daemon.max_objects_per_container_per_second, 1.2)
        self.assertEqual(daemon.per_container_ratelimit_buckets, 100)
        self.assertEqual(daemon.max_deferred_updates, 0)

        # check deprecated option
        daemon = object_updater.ObjectUpdater({'slowdown': '0.04'},
                                              logger=self.logger)
        self.assertEqual(daemon.max_objects_per_second, 20.0)

        def check_bad(conf):
            with self.assertRaises(ValueError):
                object_updater.ObjectUpdater(conf, logger=self.logger)

        check_bad({'interval': 'foo'})
        check_bad({'concurrency': 'bar'})
        check_bad({'concurrency': '1.0'})
        check_bad({'slowdown': 'baz'})
        check_bad({'objects_per_second': 'quux'})
        check_bad({'max_objects_per_container_per_second': '-0.1'})
        check_bad({'max_objects_per_container_per_second': 'auto'})
        check_bad({'per_container_ratelimit_buckets': '1.2'})
        check_bad({'per_container_ratelimit_buckets': '0'})
        check_bad({'per_container_ratelimit_buckets': '-1'})
        check_bad({'per_container_ratelimit_buckets': 'auto'})
        check_bad({'max_deferred_updates': '-1'})
        check_bad({'max_deferred_updates': '1.1'})
        check_bad({'max_deferred_updates': 'auto'})

    @mock.patch('os.listdir')
    def test_listdir_with_exception(self, mock_listdir):
        e = OSError('permission_denied')
        mock_listdir.side_effect = e
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        paths = daemon._listdir('foo/bar')
        self.assertEqual([], paths)
        log_lines = self.logger.get_lines_for_level('error')
        msg = ('ERROR: Unable to access foo/bar: permission_denied')
        self.assertEqual(log_lines[0], msg)

    @mock.patch('os.listdir', return_value=['foo', 'bar'])
    def test_listdir_without_exception(self, mock_listdir):
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        path = daemon._listdir('foo/bar/')
        log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(len(log_lines), 0)
        self.assertEqual(path, ['foo', 'bar'])

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_object_sweep(self, mock_recon):
        def check_with_idx(policy_index, warn, should_skip):
            if int(policy_index) > 0:
                asyncdir = os.path.join(self.sda1,
                                        ASYNCDIR_BASE + "-" + policy_index)
            else:
                asyncdir = os.path.join(self.sda1, ASYNCDIR_BASE)

            prefix_dir = os.path.join(asyncdir, 'abc')
            mkpath(prefix_dir)

            # A non-directory where directory is expected should just be
            # skipped, but should not stop processing of subsequent
            # directories.
            not_dirs = (
                os.path.join(self.sda1, 'not_a_dir'),
                os.path.join(self.sda1,
                             ASYNCDIR_BASE + '-' + 'twentington'),
                os.path.join(self.sda1,
                             ASYNCDIR_BASE + '-' + str(
                                 int(policy_index) + 100)))

            for not_dir in not_dirs:
                with open(not_dir, 'w'):
                    pass

            objects = {
                'a': [1089.3, 18.37, 12.83, 1.3],
                'b': [49.4, 49.3, 49.2, 49.1],
                'c': [109984.123],
            }

            expected = set()
            for o, timestamps in objects.items():
                ohash = hash_path('account', 'container', o)
                for t in timestamps:
                    o_path = os.path.join(prefix_dir, ohash + '-' +
                                          normalize_timestamp(t))
                    if t == timestamps[0]:
                        expected.add((o_path, int(policy_index)))
                    self._write_dummy_pickle(o_path, 'account', 'container', o)

            seen = set()

            class MockObjectUpdater(object_updater.ObjectUpdater):
                def process_object_update(self, update_path, policy, **kwargs):
                    seen.add((update_path, int(policy)))
                    os.unlink(update_path)

            ou = MockObjectUpdater({
                'devices': self.devices_dir,
                'mount_check': 'false',
                'swift_dir': self.testdir,
                'interval': '1',
                'concurrency': '1',
                'node_timeout': '5'})
            ou.logger = mock_logger = mock.MagicMock()
            ou.object_sweep(self.sda1)
            self.assertEqual(mock_logger.warning.call_count, warn)
            self.assertTrue(
                os.path.exists(os.path.join(self.sda1, 'not_a_dir')))
            if should_skip:
                # if we were supposed to skip over the dir, we didn't process
                # anything at all
                self.assertEqual(set(), seen)
            else:
                self.assertEqual(expected, seen)

            # test cleanup: the tempdir gets cleaned up between runs, but this
            # way we can be called multiple times in a single test method
            for not_dir in not_dirs:
                os.unlink(not_dir)

        # first check with valid policies
        for pol in POLICIES:
            check_with_idx(str(pol.idx), 0, should_skip=False)
        # now check with a bogus async dir policy and make sure we get
        # a warning indicating that the '99' policy isn't valid
        check_with_idx('99', 1, should_skip=True)

    def test_sweep_logs(self):
        asyncdir = os.path.join(self.sda1, ASYNCDIR_BASE)
        prefix_dir = os.path.join(asyncdir, 'abc')
        mkpath(prefix_dir)

        for o, t in [('abc', 123), ('def', 234), ('ghi', 345),
                     ('jkl', 456), ('mno', 567)]:
            ohash = hash_path('account', 'container', o)
            o_path = os.path.join(prefix_dir, ohash + '-' +
                                  normalize_timestamp(t))
            self._write_dummy_pickle(o_path, 'account', 'container', o)

        class MockObjectUpdater(object_updater.ObjectUpdater):
            def process_object_update(self, update_path, **kwargs):
                os.unlink(update_path)
                self.stats.successes += 1
                self.stats.unlinks += 1

        logger = debug_logger()
        ou = MockObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'report_interval': '10.0',
            'node_timeout': '5'}, logger=logger)

        now = [time()]

        def mock_time_function():
            rv = now[0]
            now[0] += 4
            return rv

        # With 10s between updates, time() advancing 4s every time we look,
        # and 5 async_pendings on disk, we should get at least two progress
        # lines. (time is incremented by 4 each time the update app iter yields
        # and each time the elapsed time is sampled)
        with mock.patch('swift.obj.updater.time',
                        mock.MagicMock(time=mock_time_function)), \
                mock.patch.object(object_updater, 'ContextPool', MockPool):
            ou.object_sweep(self.sda1)

        info_lines = logger.get_lines_for_level('info')
        self.assertEqual(4, len(info_lines))
        self.assertIn("sweep starting", info_lines[0])
        self.assertIn(self.sda1, info_lines[0])

        self.assertIn("sweep progress", info_lines[1])
        # the space ensures it's a positive number
        self.assertIn(
            "2 successes, 0 failures, 0 quarantines, 2 unlinks, 0 errors, "
            "0 redirects",
            info_lines[1])
        self.assertIn(self.sda1, info_lines[1])

        self.assertIn("sweep progress", info_lines[2])
        self.assertIn(
            "4 successes, 0 failures, 0 quarantines, 4 unlinks, 0 errors, "
            "0 redirects",
            info_lines[2])
        self.assertIn(self.sda1, info_lines[2])

        self.assertIn("sweep complete", info_lines[3])
        self.assertIn(
            "5 successes, 0 failures, 0 quarantines, 5 unlinks, 0 errors, "
            "0 redirects",
            info_lines[3])
        self.assertIn(self.sda1, info_lines[3])

    def test_sweep_logs_multiple_policies(self):
        for policy in _mocked_policies:
            asyncdir = os.path.join(self.sda1, get_async_dir(policy.idx))
            prefix_dir = os.path.join(asyncdir, 'abc')
            mkpath(prefix_dir)

            for o, t in [('abc', 123), ('def', 234), ('ghi', 345)]:
                ohash = hash_path('account', 'container%d' % policy.idx, o)
                o_path = os.path.join(prefix_dir, ohash + '-' +
                                      normalize_timestamp(t))
                self._write_dummy_pickle(o_path, 'account', 'container', o)

        class MockObjectUpdater(object_updater.ObjectUpdater):
            def process_object_update(self, update_path, **kwargs):
                os.unlink(update_path)
                self.stats.successes += 1
                self.stats.unlinks += 1

        logger = debug_logger()
        ou = MockObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'report_interval': '10.0',
            'node_timeout': '5'}, logger=logger)

        now = [time()]

        def mock_time():
            rv = now[0]
            now[0] += 0.01
            return rv

        with mock.patch('swift.obj.updater.time',
                        mock.MagicMock(time=mock_time)):
            ou.object_sweep(self.sda1)

        completion_lines = [l for l in logger.get_lines_for_level('info')
                            if "sweep complete" in l]

        self.assertEqual(len(completion_lines), 1)
        self.assertIn("sweep complete", completion_lines[0])
        self.assertIn(
            "6 successes, 0 failures, 0 quarantines, 6 unlinks, 0 errors, "
            "0 redirects",
            completion_lines[0])

    @mock.patch.object(object_updater, 'check_drive')
    def test_run_once_with_disk_unmounted(self, mock_check_drive):
        mock_check_drive.side_effect = ValueError
        ou = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'})
        ou.run_once()
        async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(async_dir)
        ou.run_once()
        self.assertTrue(os.path.exists(async_dir))
        # each run calls check_device
        self.assertEqual([
            mock.call(self.devices_dir, 'sda1', False),
            mock.call(self.devices_dir, 'sda1', False),
        ], mock_check_drive.mock_calls)
        mock_check_drive.reset_mock()

        ou = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'TrUe',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
        odd_dir = os.path.join(async_dir, 'not really supposed '
                               'to be here')
        os.mkdir(odd_dir)
        ou.run_once()
        self.assertTrue(os.path.exists(async_dir))
        self.assertTrue(os.path.exists(odd_dir))  # skipped - not mounted!
        self.assertEqual([
            mock.call(self.devices_dir, 'sda1', True),
        ], mock_check_drive.mock_calls)
        self.assertEqual(ou.logger.get_increment_counts(), {})

    @mock.patch('swift.obj.updater.dump_recon_cache')
    @mock.patch.object(object_updater, 'check_drive')
    def test_run_once(self, mock_check_drive, mock_dump_recon):
        mock_check_drive.side_effect = lambda r, d, mc: os.path.join(r, d)
        ou = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
        ou.run_once()
        self.assertEqual([], ou.logger.get_lines_for_level('error'))
        async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(async_dir)
        ou.run_once()
        self.assertTrue(os.path.exists(async_dir))
        # each run calls check_device
        self.assertEqual([
            mock.call(self.devices_dir, 'sda1', False),
            mock.call(self.devices_dir, 'sda1', False),
        ], mock_check_drive.mock_calls)
        mock_check_drive.reset_mock()
        self.assertEqual([], ou.logger.get_lines_for_level('error'))

        ou = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'TrUe',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
        odd_dir = os.path.join(async_dir, 'not really supposed '
                               'to be here')
        os.mkdir(odd_dir)
        ou.run_once()
        self.assertTrue(os.path.exists(async_dir))
        self.assertEqual([
            mock.call(self.devices_dir, 'sda1', True),
        ], mock_check_drive.mock_calls)
        self.assertEqual([], ou.logger.get_lines_for_level('error'))

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
                pickle.dump({'op': 'PUT', 'account': 'a',
                             'container': 'c',
                             'obj': 'o', 'headers': {
                                 'X-Container-Timestamp':
                                 normalize_timestamp(0)}},
                            async_pending)
        ou.run_once()
        self.assertTrue(not os.path.exists(older_op_path))
        self.assertTrue(os.path.exists(op_path))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'failures': 1, 'unlinks': 1})
        self.assertIsNone(pickle.load(open(op_path, 'rb')).get('successes'))
        self.assertEqual(
            ['ERROR with remote server 127.0.0.1:67890/sda1: '
             'Connection refused'] * 3,
            ou.logger.get_lines_for_level('error'))
        self.assertEqual([args for args, kw in ou.logger.log_dict['timing']], [
            ('updater.timing.status.500', mock.ANY),
        ] * 3)
        ou.logger.clear()

        bindsock = listen_zero()

        def accepter(sock, return_code):
            try:
                with Timeout(3):
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write(b'HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     b'PUT /sda1/0/a/c/o HTTP/1.1\r\n')
                    headers = HeaderKeyDict()
                    line = bytes_to_wsgi(inc.readline())
                    while line and line != '\r\n':
                        headers[line.split(':')[0]] = \
                            line.split(':')[1].strip()
                        line = bytes_to_wsgi(inc.readline())
                    self.assertIn('x-container-timestamp', headers)
                    self.assertIn('X-Backend-Storage-Policy-Index',
                                  headers)
            except BaseException as err:
                return err
            return None

        def accept(return_codes):
            try:
                events = []
                for code in return_codes:
                    with Timeout(3):
                        sock, addr = bindsock.accept()
                        events.append(
                            spawn(accepter, sock, code))
                for event in events:
                    err = event.wait()
                    if err:
                        raise err
            except BaseException as err:
                return err
            return None

        # only 1/3 updates succeeds
        event = spawn(accept, [201, 500, 500])
        for dev in ou.get_container_ring().devs:
            if dev is not None:
                dev['replication_port'] = bindsock.getsockname()[1]

        ou.logger._clear()
        ou.run_once()
        err = event.wait()
        if err:
            raise err
        self.assertTrue(os.path.exists(op_path))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0],
                         pickle.load(open(op_path, 'rb')).get('successes'))
        self.assertEqual([], ou.logger.get_lines_for_level('error'))
        self.assertEqual(
            sorted([args for args, kw in ou.logger.log_dict['timing']]),
            sorted([
                ('updater.timing.status.201', mock.ANY),
                ('updater.timing.status.500', mock.ANY),
                ('updater.timing.status.500', mock.ANY),
            ]))

        # only 1/2 updates succeeds
        event = spawn(accept, [404, 201])
        ou.logger.clear()
        ou.run_once()
        err = event.wait()
        if err:
            raise err
        self.assertTrue(os.path.exists(op_path))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0, 2],
                         pickle.load(open(op_path, 'rb')).get('successes'))
        self.assertEqual([], ou.logger.get_lines_for_level('error'))
        self.assertEqual(
            sorted([args for args, kw in ou.logger.log_dict['timing']]),
            sorted([
                ('updater.timing.status.404', mock.ANY),
                ('updater.timing.status.201', mock.ANY),
            ]))

        # final update has Timeout
        ou.logger.clear()
        with Timeout(99) as exc, \
                mock.patch('swift.obj.updater.http_connect') as mock_connect:
            mock_connect.return_value.getresponse.side_effect = exc
            ou.run_once()
        self.assertTrue(os.path.exists(op_path))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0, 2],
                         pickle.load(open(op_path, 'rb')).get('successes'))
        self.assertEqual([], ou.logger.get_lines_for_level('error'))
        self.assertIn(
            'Timeout waiting on remote server 127.0.0.1:%d/sda1: 99 seconds'
            % bindsock.getsockname()[1], ou.logger.get_lines_for_level('info'))
        self.assertEqual([args for args, kw in ou.logger.log_dict['timing']], [
            ('updater.timing.status.499', mock.ANY),
        ])

        # final update has ConnectionTimeout
        ou.logger.clear()
        with ConnectionTimeout(9) as exc, \
                mock.patch('swift.obj.updater.http_connect') as mock_connect:
            mock_connect.return_value.getresponse.side_effect = exc
            ou.run_once()
        self.assertTrue(os.path.exists(op_path))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0, 2],
                         pickle.load(open(op_path, 'rb')).get('successes'))
        self.assertEqual([], ou.logger.get_lines_for_level('error'))
        self.assertIn(
            'Timeout connecting to remote server 127.0.0.1:%d/sda1: 9 seconds'
            % bindsock.getsockname()[1], ou.logger.get_lines_for_level('info'))
        self.assertEqual([args for args, kw in ou.logger.log_dict['timing']], [
            ('updater.timing.status.500', mock.ANY),
        ])

        # final update succeeds
        event = spawn(accept, [201])
        ou.logger.clear()
        ou.run_once()
        err = event.wait()
        if err:
            raise err

        # we remove the async_pending and its containing suffix dir, but not
        # anything above that
        self.assertFalse(os.path.exists(op_path))
        self.assertFalse(os.path.exists(os.path.dirname(op_path)))
        self.assertTrue(os.path.exists(os.path.dirname(os.path.dirname(
            op_path))))
        self.assertEqual([], ou.logger.get_lines_for_level('error'))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'unlinks': 1, 'successes': 1})
        self.assertEqual([args for args, kw in ou.logger.log_dict['timing']], [
            ('updater.timing.status.201', mock.ANY),
        ])

    def test_obj_put_legacy_updates(self):
        ts = (normalize_timestamp(t) for t in
              itertools.count(int(time())))
        policy = POLICIES.get_by_index(0)
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        async_dir = os.path.join(self.sda1, get_async_dir(policy))
        os.mkdir(async_dir)

        account, container, obj = 'a', 'c', 'o'
        # write an async
        for op in ('PUT', 'DELETE'):
            self.logger._clear()
            daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
            dfmanager = DiskFileManager(conf, daemon.logger)
            # don't include storage-policy-index in headers_out pickle
            headers_out = HeaderKeyDict({
                'x-size': 0,
                'x-content-type': 'text/plain',
                'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                'x-timestamp': next(ts),
            })
            data = {'op': op, 'account': account, 'container': container,
                    'obj': obj, 'headers': headers_out}
            dfmanager.pickle_async_update(self.sda1, account, container, obj,
                                          data, next(ts), policy)

            request_log = []

            def capture(*args, **kwargs):
                request_log.append((args, kwargs))

            # run once
            fake_status_codes = [200, 200, 200]
            with mocked_http_conn(*fake_status_codes, give_connect=capture):
                daemon.run_once()
            self.assertEqual(len(fake_status_codes), len(request_log))
            for request_args, request_kwargs in request_log:
                ip, part, method, path, headers, qs, ssl = request_args
                self.assertEqual(method, op)
                self.assertEqual(headers['X-Backend-Storage-Policy-Index'],
                                 str(int(policy)))
            self.assertEqual(daemon.logger.get_increment_counts(),
                             {'successes': 1, 'unlinks': 1,
                              'async_pendings': 1})

    def _write_async_update(self, dfmanager, timestamp, policy,
                            headers=None, container_path=None):
        # write an async
        account, container, obj = 'a', 'c', 'o'
        op = 'PUT'
        headers_out = headers or {
            'x-size': 0,
            'x-content-type': 'text/plain',
            'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'x-timestamp': timestamp.internal,
            'X-Backend-Storage-Policy-Index': int(policy),
            'User-Agent': 'object-server %s' % os.getpid()
        }
        data = {'op': op, 'account': account, 'container': container,
                'obj': obj, 'headers': headers_out}
        if container_path:
            data['container_path'] = container_path
        dfmanager.pickle_async_update(self.sda1, account, container, obj,
                                      data, timestamp, policy)

    def test_obj_put_async_updates(self):
        policies = list(POLICIES)
        random.shuffle(policies)

        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)

        def do_test(headers_out, expected, container_path=None):
            # write an async
            dfmanager = DiskFileManager(conf, daemon.logger)
            self._write_async_update(dfmanager, next(self.ts_iter),
                                     policies[0], headers=headers_out,
                                     container_path=container_path)
            request_log = []

            def capture(*args, **kwargs):
                request_log.append((args, kwargs))

            # run once
            fake_status_codes = [
                200,  # object update success
                200,  # object update success
                200,  # object update conflict
            ]
            with mocked_http_conn(*fake_status_codes, give_connect=capture):
                daemon.run_once()
            self.assertEqual(len(fake_status_codes), len(request_log))
            for request_args, request_kwargs in request_log:
                ip, part, method, path, headers, qs, ssl = request_args
                self.assertEqual(method, 'PUT')
                self.assertDictEqual(expected, headers)
            self.assertEqual(
                daemon.logger.get_increment_counts(),
                {'successes': 1, 'unlinks': 1, 'async_pendings': 1})
            self.assertFalse(os.listdir(async_dir))
            daemon.logger.clear()

        ts = next(self.ts_iter)
        # use a dict rather than HeaderKeyDict so we can vary the case of the
        # pickled headers
        headers_out = {
            'x-size': 0,
            'x-content-type': 'text/plain',
            'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'x-timestamp': ts.normal,
            'X-Backend-Storage-Policy-Index': int(policies[0]),
            'User-Agent': 'object-server %s' % os.getpid()
        }
        expected = {
            'X-Size': '0',
            'X-Content-Type': 'text/plain',
            'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'X-Timestamp': ts.normal,
            'X-Backend-Storage-Policy-Index': str(int(policies[0])),
            'User-Agent': 'object-updater %s' % os.getpid(),
            'X-Backend-Accept-Redirect': 'true',
            'X-Backend-Accept-Quoted-Location': 'true',
        }
        # always expect X-Backend-Accept-Redirect and
        # X-Backend-Accept-Quoted-Location to be true
        do_test(headers_out, expected, container_path='.shards_a/shard_c')
        do_test(headers_out, expected)

        # ...unless they're already set
        expected['X-Backend-Accept-Redirect'] = 'false'
        expected['X-Backend-Accept-Quoted-Location'] = 'false'
        headers_out_2 = dict(headers_out)
        headers_out_2['X-Backend-Accept-Redirect'] = 'false'
        headers_out_2['X-Backend-Accept-Quoted-Location'] = 'false'
        do_test(headers_out_2, expected)

        # updater should add policy header if missing
        expected['X-Backend-Accept-Redirect'] = 'true'
        expected['X-Backend-Accept-Quoted-Location'] = 'true'
        headers_out['X-Backend-Storage-Policy-Index'] = None
        do_test(headers_out, expected)

        # updater should not overwrite a mismatched policy header
        headers_out['X-Backend-Storage-Policy-Index'] = int(policies[1])
        expected['X-Backend-Storage-Policy-Index'] = str(int(policies[1]))
        do_test(headers_out, expected)

        # check for case insensitivity
        headers_out['user-agent'] = headers_out.pop('User-Agent')
        headers_out['x-backend-storage-policy-index'] = headers_out.pop(
            'X-Backend-Storage-Policy-Index')
        do_test(headers_out, expected)

    def _check_update_requests(self, requests, timestamp, policy):
        # do some sanity checks on update request
        expected_headers = {
            'X-Size': '0',
            'X-Content-Type': 'text/plain',
            'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'X-Timestamp': timestamp.internal,
            'X-Backend-Storage-Policy-Index': str(int(policy)),
            'User-Agent': 'object-updater %s' % os.getpid(),
            'X-Backend-Accept-Redirect': 'true',
            'X-Backend-Accept-Quoted-Location': 'true'}
        for request in requests:
            self.assertEqual('PUT', request['method'])
            self.assertDictEqual(expected_headers, request['headers'])

    def test_obj_put_async_root_update_redirected(self):
        policies = list(POLICIES)
        random.shuffle(policies)
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)
        dfmanager = DiskFileManager(conf, daemon.logger)

        ts_obj = next(self.ts_iter)
        self._write_async_update(dfmanager, ts_obj, policies[0])

        # run once
        ts_redirect_1 = next(self.ts_iter)
        ts_redirect_2 = next(self.ts_iter)
        fake_responses = [
            # first round of update attempts, newest redirect should be chosen
            (200, {}),
            (301, {'Location': '/.shards_a/c_shard_new/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_2.internal}),
            (301, {'Location': '/.shards_a/c_shard_old/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_1.internal}),
            # second round of update attempts
            (200, {}),
            (200, {}),
            (200, {}),
        ]
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self._check_update_requests(conn.requests[:3], ts_obj, policies[0])
        self._check_update_requests(conn.requests[3:], ts_obj, policies[0])
        self.assertEqual(['/sda1/0/a/c/o'] * 3 +
                         ['/sda1/0/.shards_a/c_shard_new/o'] * 3,
                         [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 1, 'successes': 1,
             'unlinks': 1, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        self.assertFalse(os.listdir(async_dir))  # no async file

    def test_obj_put_async_root_update_redirected_previous_success(self):
        policies = list(POLICIES)
        random.shuffle(policies)
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)
        dfmanager = DiskFileManager(conf, daemon.logger)

        ts_obj = next(self.ts_iter)
        self._write_async_update(dfmanager, ts_obj, policies[0])
        orig_async_path, orig_async_data = self._check_async_file(async_dir)

        # run once
        with mocked_http_conn(
                507, 200, 507) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self._check_update_requests(conn.requests, ts_obj, policies[0])
        self.assertEqual(['/sda1/0/a/c/o'] * 3,
                         [req['path'] for req in conn.requests])
        self.assertEqual(
            {'failures': 1, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        async_path, async_data = self._check_async_file(async_dir)
        self.assertEqual(dict(orig_async_data, successes=[1]), async_data)

        # run again - expect 3 redirected updates despite previous success
        ts_redirect = next(self.ts_iter)
        resp_headers_1 = {'Location': '/.shards_a/c_shard_1/o',
                          'X-Backend-Redirect-Timestamp': ts_redirect.internal}
        fake_responses = (
            # 1st round of redirects, 2nd round of redirects
            [(301, resp_headers_1)] * 2 + [(200, {})] * 3)
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self._check_update_requests(conn.requests[:2], ts_obj, policies[0])
        self._check_update_requests(conn.requests[2:], ts_obj, policies[0])
        root_part = daemon.container_ring.get_part('a/c')
        shard_1_part = daemon.container_ring.get_part('.shards_a/c_shard_1')
        self.assertEqual(
            ['/sda1/%s/a/c/o' % root_part] * 2 +
            ['/sda1/%s/.shards_a/c_shard_1/o' % shard_1_part] * 3,
            [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 1, 'successes': 1, 'failures': 1, 'unlinks': 1,
             'async_pendings': 1},
            daemon.logger.get_increment_counts())
        self.assertFalse(os.listdir(async_dir))  # no async file

    def _check_async_file(self, async_dir):
        async_subdirs = os.listdir(async_dir)
        self.assertEqual([mock.ANY], async_subdirs)
        async_files = os.listdir(os.path.join(async_dir, async_subdirs[0]))
        self.assertEqual([mock.ANY], async_files)
        async_path = os.path.join(
            async_dir, async_subdirs[0], async_files[0])
        with open(async_path, 'rb') as fd:
            async_data = pickle.load(fd)
        return async_path, async_data

    def _check_obj_put_async_update_bad_redirect_headers(self, headers):
        policies = list(POLICIES)
        random.shuffle(policies)
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)
        dfmanager = DiskFileManager(conf, daemon.logger)

        ts_obj = next(self.ts_iter)
        self._write_async_update(dfmanager, ts_obj, policies[0])
        orig_async_path, orig_async_data = self._check_async_file(async_dir)

        fake_responses = [
            (301, headers),
            (301, headers),
            (301, headers),
        ]
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self._check_update_requests(conn.requests, ts_obj, policies[0])
        self.assertEqual(['/sda1/0/a/c/o'] * 3,
                         [req['path'] for req in conn.requests])
        self.assertEqual(
            {'failures': 1, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        # async file still intact
        async_path, async_data = self._check_async_file(async_dir)
        self.assertEqual(orig_async_path, async_path)
        self.assertEqual(orig_async_data, async_data)
        return daemon

    def test_obj_put_async_root_update_missing_location_header(self):
        headers = {
            'X-Backend-Redirect-Timestamp': next(self.ts_iter).internal}
        self._check_obj_put_async_update_bad_redirect_headers(headers)

    def test_obj_put_async_root_update_bad_location_header(self):
        headers = {
            'Location': 'bad bad bad',
            'X-Backend-Redirect-Timestamp': next(self.ts_iter).internal}
        daemon = self._check_obj_put_async_update_bad_redirect_headers(headers)
        error_lines = daemon.logger.get_lines_for_level('error')
        self.assertIn('Container update failed', error_lines[0])
        self.assertIn('Invalid path: bad%20bad%20bad', error_lines[0])

    def test_obj_put_async_shard_update_redirected_twice(self):
        policies = list(POLICIES)
        random.shuffle(policies)
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)
        dfmanager = DiskFileManager(conf, daemon.logger)

        ts_obj = next(self.ts_iter)
        self._write_async_update(dfmanager, ts_obj, policies[0],
                                 container_path='.shards_a/c_shard_older')
        orig_async_path, orig_async_data = self._check_async_file(async_dir)

        # run once
        ts_redirect_1 = next(self.ts_iter)
        ts_redirect_2 = next(self.ts_iter)
        ts_redirect_3 = next(self.ts_iter)
        fake_responses = [
            # 1st round of redirects, newest redirect should be chosen
            (301, {'Location': '/.shards_a/c_shard_old/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_1.internal}),
            (301, {'Location': '/.shards_a/c%5Fshard%5Fnew/o',
                   'X-Backend-Location-Is-Quoted': 'true',
                   'X-Backend-Redirect-Timestamp': ts_redirect_2.internal}),
            (301, {'Location': '/.shards_a/c%5Fshard%5Fold/o',
                   'X-Backend-Location-Is-Quoted': 'true',
                   'X-Backend-Redirect-Timestamp': ts_redirect_1.internal}),
            # 2nd round of redirects
            (301, {'Location': '/.shards_a/c_shard_newer/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_3.internal}),
            (301, {'Location': '/.shards_a/c_shard_newer/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_3.internal}),
            (301, {'Location': '/.shards_a/c_shard_newer/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_3.internal}),
        ]
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self._check_update_requests(conn.requests, ts_obj, policies[0])
        # only *one* set of redirected requests is attempted per cycle
        older_part = daemon.container_ring.get_part('.shards_a/c_shard_older')
        new_part = daemon.container_ring.get_part('.shards_a/c_shard_new')
        newer_part = daemon.container_ring.get_part('.shards_a/c_shard_newer')
        self.assertEqual(
            ['/sda1/%s/.shards_a/c_shard_older/o' % older_part] * 3 +
            ['/sda1/%s/.shards_a/c_shard_new/o' % new_part] * 3,
            [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 2, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        # update failed, we still have pending file with most recent redirect
        # response Location header value added to data
        async_path, async_data = self._check_async_file(async_dir)
        self.assertEqual(orig_async_path, async_path)
        self.assertEqual(
            dict(orig_async_data, container_path='.shards_a/c_shard_newer',
                 redirect_history=['.shards_a/c_shard_new',
                                   '.shards_a/c_shard_newer']),
            async_data)

        # next cycle, should get latest redirect from pickled async update
        fake_responses = [(200, {})] * 3
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self._check_update_requests(conn.requests, ts_obj, policies[0])
        self.assertEqual(
            ['/sda1/%s/.shards_a/c_shard_newer/o' % newer_part] * 3,
            [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 2, 'successes': 1, 'unlinks': 1,
             'async_pendings': 1},
            daemon.logger.get_increment_counts())
        self.assertFalse(os.listdir(async_dir))  # no async file

    def test_obj_put_async_update_redirection_loop(self):
        policies = list(POLICIES)
        random.shuffle(policies)
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)
        dfmanager = DiskFileManager(conf, daemon.logger)

        ts_obj = next(self.ts_iter)
        self._write_async_update(dfmanager, ts_obj, policies[0])
        orig_async_path, orig_async_data = self._check_async_file(async_dir)

        # run once
        ts_redirect = next(self.ts_iter)

        resp_headers_1 = {'Location': '/.shards_a/c_shard_1/o',
                          'X-Backend-Redirect-Timestamp': ts_redirect.internal}
        resp_headers_2 = {'Location': '/.shards_a/c_shard_2/o',
                          'X-Backend-Redirect-Timestamp': ts_redirect.internal}
        fake_responses = (
            # 1st round of redirects, 2nd round of redirects
            [(301, resp_headers_1)] * 3 + [(301, resp_headers_2)] * 3)
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()
        self._check_update_requests(conn.requests[:3], ts_obj, policies[0])
        self._check_update_requests(conn.requests[3:], ts_obj, policies[0])
        # only *one* set of redirected requests is attempted per cycle
        root_part = daemon.container_ring.get_part('a/c')
        shard_1_part = daemon.container_ring.get_part('.shards_a/c_shard_1')
        shard_2_part = daemon.container_ring.get_part('.shards_a/c_shard_2')
        shard_3_part = daemon.container_ring.get_part('.shards_a/c_shard_3')
        self.assertEqual(['/sda1/%s/a/c/o' % root_part] * 3 +
                         ['/sda1/%s/.shards_a/c_shard_1/o' % shard_1_part] * 3,
                         [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 2, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        # update failed, we still have pending file with most recent redirect
        # response Location header value added to data
        async_path, async_data = self._check_async_file(async_dir)
        self.assertEqual(orig_async_path, async_path)
        self.assertEqual(
            dict(orig_async_data, container_path='.shards_a/c_shard_2',
                 redirect_history=['.shards_a/c_shard_1',
                                   '.shards_a/c_shard_2']),
            async_data)

        # next cycle, more redirects! first is to previously visited location
        resp_headers_3 = {'Location': '/.shards_a/c_shard_3/o',
                          'X-Backend-Redirect-Timestamp': ts_redirect.internal}
        fake_responses = (
            # 1st round of redirects, 2nd round of redirects
            [(301, resp_headers_1)] * 3 + [(301, resp_headers_3)] * 3)
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()
        self._check_update_requests(conn.requests[:3], ts_obj, policies[0])
        self._check_update_requests(conn.requests[3:], ts_obj, policies[0])
        # first try the previously persisted container path, response to that
        # creates a loop so ignore and send to root
        self.assertEqual(
            ['/sda1/%s/.shards_a/c_shard_2/o' % shard_2_part] * 3 +
            ['/sda1/%s/a/c/o' % root_part] * 3,
            [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 4, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        # update failed, we still have pending file with most recent redirect
        # response Location header value from root added to persisted data
        async_path, async_data = self._check_async_file(async_dir)
        self.assertEqual(orig_async_path, async_path)
        # note: redirect_history was reset when falling back to root
        self.assertEqual(
            dict(orig_async_data, container_path='.shards_a/c_shard_3',
                 redirect_history=['.shards_a/c_shard_3']),
            async_data)

        # next cycle, more redirects! first is to a location visited previously
        # but not since last fall back to root, so that location IS tried;
        # second is to a location visited since last fall back to root so that
        # location is NOT tried
        fake_responses = (
            # 1st round of redirects, 2nd round of redirects
            [(301, resp_headers_1)] * 3 + [(301, resp_headers_3)] * 3)
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()
        self._check_update_requests(conn.requests, ts_obj, policies[0])
        self.assertEqual(
            ['/sda1/%s/.shards_a/c_shard_3/o' % shard_3_part] * 3 +
            ['/sda1/%s/.shards_a/c_shard_1/o' % shard_1_part] * 3,
            [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 6, 'async_pendings': 1},
            daemon.logger.get_increment_counts())
        # update failed, we still have pending file, but container_path is None
        # because most recent redirect location was a repeat
        async_path, async_data = self._check_async_file(async_dir)
        self.assertEqual(orig_async_path, async_path)
        self.assertEqual(
            dict(orig_async_data, container_path=None,
                 redirect_history=[]),
            async_data)

        # next cycle, persisted container path is None so update should go to
        # root, this time it succeeds
        fake_responses = [(200, {})] * 3
        fake_status_codes, fake_headers = zip(*fake_responses)
        with mocked_http_conn(
                *fake_status_codes, headers=fake_headers) as conn:
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()
        self._check_update_requests(conn.requests, ts_obj, policies[0])
        self.assertEqual(['/sda1/%s/a/c/o' % root_part] * 3,
                         [req['path'] for req in conn.requests])
        self.assertEqual(
            {'redirects': 6, 'successes': 1, 'unlinks': 1,
             'async_pendings': 1},
            daemon.logger.get_increment_counts())
        self.assertFalse(os.listdir(async_dir))  # no async file

    def test_obj_update_quarantine(self):
        policies = list(POLICIES)
        random.shuffle(policies)

        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)

        ohash = hash_path('a', 'c', 'o')
        odir = os.path.join(async_dir, ohash[-3:])
        mkdirs(odir)
        op_path = os.path.join(
            odir,
            '%s-%s' % (ohash, next(self.ts_iter).internal))
        with open(op_path, 'wb') as async_pending:
            async_pending.write(b'\xff')  # invalid pickle

        with mocked_http_conn():
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon.run_once()

        self.assertEqual(
            {'quarantines': 1},
            daemon.logger.get_increment_counts())
        self.assertFalse(os.listdir(async_dir))  # no asyncs

    def test_obj_update_gone_missing(self):
        # if you've got multiple updaters running (say, both a background
        # and foreground process), _load_update may get a file
        # that doesn't exist
        policies = list(POLICIES)
        random.shuffle(policies)

        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policies[0]))
        os.mkdir(async_dir)

        ohash = hash_path('a', 'c', 'o')
        odir = os.path.join(async_dir, ohash[-3:])
        mkdirs(odir)
        op_path = os.path.join(
            odir,
            '%s-%s' % (ohash, next(self.ts_iter).internal))

        self.assertEqual(os.listdir(async_dir), [ohash[-3:]])
        self.assertFalse(os.listdir(odir))
        with mocked_http_conn():
            with mock.patch('swift.obj.updater.dump_recon_cache'):
                daemon._load_update(self.sda1, op_path)
        self.assertEqual({}, daemon.logger.get_increment_counts())
        self.assertEqual(os.listdir(async_dir), [ohash[-3:]])
        self.assertFalse(os.listdir(odir))

    def _write_dummy_pickle(self, path, a, c, o, cp=None):
        update = {
            'op': 'PUT',
            'account': a,
            'container': c,
            'obj': o,
            'headers': {'X-Container-Timestamp': normalize_timestamp(0)}
        }
        if cp:
            update['container_path'] = cp
        with open(path, 'wb') as async_pending:
            pickle.dump(update, async_pending)

    def _make_async_pending_pickle(self, a, c, o, cp=None):
        ohash = hash_path(a, c, o)
        odir = os.path.join(self.async_dir, ohash[-3:])
        mkdirs(odir)
        path = os.path.join(
            odir,
            '%s-%s' % (ohash, normalize_timestamp(time())))
        self._write_dummy_pickle(path, a, c, o, cp)

    def _find_async_pending_files(self):
        found_files = []
        for root, dirs, files in os.walk(self.async_dir):
            found_files.extend(files)
        return found_files

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_per_container_rate_limit(self, mock_recon):
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'max_objects_per_container_per_second': 1,
            'max_deferred_updates': 0,  # do not re-iterate
            'concurrency': 1
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(self.async_dir)
        num_c1_files = 10
        for i in range(num_c1_files):
            obj_name = 'o%02d' % i
            self._make_async_pending_pickle('a', 'c1', obj_name)
        c1_part, _ = daemon.get_container_ring().get_nodes('a', 'c1')
        # make one more in a different container, with a container_path
        self._make_async_pending_pickle('a', 'c2', obj_name,
                                        cp='.shards_a/c2_shard')
        c2_part, _ = daemon.get_container_ring().get_nodes('.shards_a',
                                                           'c2_shard')
        expected_total = num_c1_files + 1
        self.assertEqual(expected_total,
                         len(self._find_async_pending_files()))
        expected_success = 2
        fake_status_codes = [200] * 3 * expected_success
        with mocked_http_conn(*fake_status_codes) as fake_conn:
            daemon.run_once()
        self.assertEqual(expected_success, daemon.stats.successes)
        expected_skipped = expected_total - expected_success
        self.assertEqual(expected_skipped, daemon.stats.skips)
        self.assertEqual(expected_skipped,
                         len(self._find_async_pending_files()))
        self.assertEqual(
            Counter(
                '/'.join(req['path'].split('/')[:5])
                for req in fake_conn.requests),
            {'/sda1/%s/a/c1' % c1_part: 3,
             '/sda1/%s/.shards_a/c2_shard' % c2_part: 3})
        info_lines = self.logger.get_lines_for_level('info')
        self.assertTrue(info_lines)
        self.assertIn('2 successes, 0 failures, 0 quarantines, 2 unlinks, '
                      '0 errors, 0 redirects, 9 skips, 9 deferrals, 0 drains',
                      info_lines[-1])
        self.assertEqual({'skips': 9, 'successes': 2, 'unlinks': 2,
                          'deferrals': 9},
                         self.logger.get_increment_counts())

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_per_container_rate_limit_unlimited(self, mock_recon):
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'max_objects_per_container_per_second': 0,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(self.async_dir)
        num_c1_files = 10
        for i in range(num_c1_files):
            obj_name = 'o%02d' % i
            self._make_async_pending_pickle('a', 'c1', obj_name)
        c1_part, _ = daemon.get_container_ring().get_nodes('a', 'c1')
        # make one more in a different container, with a container_path
        self._make_async_pending_pickle('a', 'c2', obj_name,
                                        cp='.shards_a/c2_shard')
        c2_part, _ = daemon.get_container_ring().get_nodes('.shards_a',
                                                           'c2_shard')
        expected_total = num_c1_files + 1
        self.assertEqual(expected_total,
                         len(self._find_async_pending_files()))
        fake_status_codes = [200] * 3 * expected_total
        with mocked_http_conn(*fake_status_codes):
            daemon.run_once()
        self.assertEqual(expected_total, daemon.stats.successes)
        self.assertEqual(0, daemon.stats.skips)
        self.assertEqual([], self._find_async_pending_files())
        info_lines = self.logger.get_lines_for_level('info')
        self.assertTrue(info_lines)
        self.assertIn('11 successes, 0 failures, 0 quarantines, 11 unlinks, '
                      '0 errors, 0 redirects, 0 skips, 0 deferrals, 0 drains',
                      info_lines[-1])
        self.assertEqual({'successes': 11, 'unlinks': 11},
                         self.logger.get_increment_counts())

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_per_container_rate_limit_some_limited(self, mock_recon):
        # simulate delays between buckets being fed so that only some updates
        # are skipped
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'max_objects_per_container_per_second': 10,
            'max_deferred_updates': 0,  # do not re-iterate
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(self.async_dir)
        # all updates for same container
        num_c1_files = 4
        for i in range(num_c1_files):
            obj_name = 'o%02d' % i
            self._make_async_pending_pickle('a', 'c1', obj_name)
        c1_part, _ = daemon.get_container_ring().get_nodes('a', 'c1')
        expected_total = num_c1_files
        self.assertEqual(expected_total,
                         len(self._find_async_pending_files()))
        # first one always succeeds, second is skipped because it is only 0.05s
        # behind the first, second succeeds because it is 0.11 behind the
        # first, fourth is skipped
        latencies = [0, 0.05, .051, 0]
        expected_success = 2
        fake_status_codes = [200] * 3 * expected_success

        contexts_fed_in = []

        def ratelimit_if(value):
            contexts_fed_in.append(value)
            # make each update delay before the iter being called again
            eventlet.sleep(latencies.pop(0))
            return False  # returning False overrides normal ratelimiting

        orig_rate_limited_iterator = utils.RateLimitedIterator

        def fake_rate_limited_iterator(*args, **kwargs):
            # insert our own rate limiting function
            kwargs['ratelimit_if'] = ratelimit_if
            return orig_rate_limited_iterator(*args, **kwargs)

        with mocked_http_conn(*fake_status_codes) as fake_conn, \
                mock.patch('swift.obj.updater.RateLimitedIterator',
                           fake_rate_limited_iterator):
            daemon.run_once()
        self.assertEqual(expected_success, daemon.stats.successes)
        expected_skipped = expected_total - expected_success
        self.assertEqual(expected_skipped, daemon.stats.skips)
        self.assertEqual(expected_skipped,
                         len(self._find_async_pending_files()))
        paths_fed_in = ['/sda1/%(part)s/%(account)s/%(container)s/%(obj)s'
                        % dict(ctx['update'], part=c1_part)
                        for ctx in contexts_fed_in]
        expected_update_paths = paths_fed_in[:1] * 3 + paths_fed_in[2:3] * 3
        actual_update_paths = [req['path'] for req in fake_conn.requests]
        self.assertEqual(expected_update_paths, actual_update_paths)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertTrue(info_lines)
        self.assertIn('2 successes, 0 failures, 0 quarantines, 2 unlinks, '
                      '0 errors, 0 redirects, 2 skips, 2 deferrals, 0 drains',
                      info_lines[-1])
        self.assertEqual({'skips': 2, 'successes': 2, 'unlinks': 2,
                          'deferrals': 2},
                         self.logger.get_increment_counts())

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_per_container_rate_limit_defer_2_skip_1(self, mock_recon):
        # limit length of deferral queue so that some defer and some skip
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'max_objects_per_container_per_second': 10,
            # only one bucket needed for test
            'per_container_ratelimit_buckets': 1,
            'max_deferred_updates': 1,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(self.async_dir)
        # all updates for same container
        num_c1_files = 4
        for i in range(num_c1_files):
            obj_name = 'o%02d' % i
            self._make_async_pending_pickle('a', 'c1', obj_name)
        c1_part, _ = daemon.get_container_ring().get_nodes('a', 'c1')
        expected_total = num_c1_files
        self.assertEqual(expected_total,
                         len(self._find_async_pending_files()))
        # first succeeds, second is deferred, third succeeds, fourth is
        # deferred and bumps second out of deferral queue, fourth is re-tried
        latencies = [0, 0.05, .051, 0, 0, .11]
        expected_success = 3

        contexts_fed_in = []
        captured_queues = []
        captured_skips_stats = []

        def ratelimit_if(value):
            contexts_fed_in.append(value)
            return False  # returning False overrides normal ratelimiting

        orig_rate_limited_iterator = utils.RateLimitedIterator

        def fake_rate_limited_iterator(*args, **kwargs):
            # insert our own rate limiting function
            kwargs['ratelimit_if'] = ratelimit_if
            return orig_rate_limited_iterator(*args, **kwargs)

        now = [time()]

        def fake_get_time(bucket_iter):
            captured_skips_stats.append(
                daemon.logger.get_increment_counts().get('skips', 0))
            captured_queues.append(list(bucket_iter.buckets[0].deque))
            # make each update delay before the iter being called again
            now[0] += latencies.pop(0)
            return now[0]

        captured_updates = []

        def fake_object_update(node, part, op, obj, *args, **kwargs):
            captured_updates.append((node, part, op, obj))
            return True, node['id'], False

        with mock.patch(
                'swift.obj.updater.BucketizedUpdateSkippingLimiter._get_time',
                fake_get_time), \
                mock.patch.object(daemon, 'object_update',
                                  fake_object_update), \
                mock.patch('swift.obj.updater.RateLimitedIterator',
                           fake_rate_limited_iterator):
            daemon.run_once()
        self.assertEqual(expected_success, daemon.stats.successes)
        expected_skipped = expected_total - expected_success
        self.assertEqual(expected_skipped, daemon.stats.skips)
        self.assertEqual(expected_skipped,
                         len(self._find_async_pending_files()))

        orig_iteration = contexts_fed_in[:num_c1_files]
        # we first capture every async fed in one by one
        objs_fed_in = [ctx['update']['obj'] for ctx in orig_iteration]
        self.assertEqual(num_c1_files, len(set(objs_fed_in)))
        # keep track of this order for context
        aorder = {ctx['update']['obj']: 'a%02d' % i
                  for i, ctx in enumerate(orig_iteration)}
        expected_drops = (1,)
        expected_updates_sent = []
        for i, obj in enumerate(objs_fed_in):
            if i in expected_drops:
                continue
            # triple replica, request to 3 nodes each obj!
            expected_updates_sent.extend([obj] * 3)

        actual_updates_sent = [
            utils.split_path(update[3], minsegs=3)[-1]
            for update in captured_updates
        ]
        self.assertEqual([aorder[o] for o in expected_updates_sent],
                         [aorder[o] for o in actual_updates_sent])

        self.assertEqual([0, 0, 0, 0, 1], captured_skips_stats)

        expected_deferrals = [
            [],
            [],
            [objs_fed_in[1]],
            [objs_fed_in[1]],
            [objs_fed_in[3]],
        ]
        self.assertEqual(
            expected_deferrals,
            [[ctx['update']['obj'] for ctx in q] for q in captured_queues])
        info_lines = self.logger.get_lines_for_level('info')
        self.assertTrue(info_lines)
        self.assertIn('3 successes, 0 failures, 0 quarantines, 3 unlinks, '
                      '0 errors, 0 redirects, 1 skips, 2 deferrals, 1 drains',
                      info_lines[-1])
        self.assertEqual(
            {'skips': 1, 'successes': 3, 'unlinks': 3, 'deferrals': 2,
             'drains': 1}, self.logger.get_increment_counts())

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_per_container_rate_limit_defer_3_skip_1(self, mock_recon):
        # limit length of deferral queue so that some defer and some skip
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'max_objects_per_container_per_second': 10,
            # only one bucket needed for test
            'per_container_ratelimit_buckets': 1,
            'max_deferred_updates': 2,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(self.async_dir)
        # all updates for same container
        num_c1_files = 5
        for i in range(num_c1_files):
            obj_name = 'o%02d' % i
            self._make_async_pending_pickle('a', 'c1', obj_name)
        c1_part, _ = daemon.get_container_ring().get_nodes('a', 'c1')
        expected_total = num_c1_files
        self.assertEqual(expected_total,
                         len(self._find_async_pending_files()))
        # indexes 0, 2 succeed; 1, 3, 4 deferred but 1 is bumped from deferral
        # queue by 4; 4, 3 are then drained
        latencies = [0, 0.05, .051, 0, 0, 0, .11, .01]
        expected_success = 4

        contexts_fed_in = []
        captured_queues = []
        captured_skips_stats = []

        def ratelimit_if(value):
            contexts_fed_in.append(value)
            return False  # returning False overrides normal ratelimiting

        orig_rate_limited_iterator = utils.RateLimitedIterator

        def fake_rate_limited_iterator(*args, **kwargs):
            # insert our own rate limiting function
            kwargs['ratelimit_if'] = ratelimit_if
            return orig_rate_limited_iterator(*args, **kwargs)

        now = [time()]

        def fake_get_time(bucket_iter):
            captured_skips_stats.append(
                daemon.logger.get_increment_counts().get('skips', 0))
            captured_queues.append(list(bucket_iter.buckets[0].deque))
            # make each update delay before the iter being called again
            now[0] += latencies.pop(0)
            return now[0]

        captured_updates = []

        def fake_object_update(node, part, op, obj, *args, **kwargs):
            captured_updates.append((node, part, op, obj))
            return True, node['id'], False

        with mock.patch(
                'swift.obj.updater.BucketizedUpdateSkippingLimiter._get_time',
                fake_get_time), \
                mock.patch.object(daemon, 'object_update',
                                  fake_object_update), \
                mock.patch('swift.obj.updater.RateLimitedIterator',
                           fake_rate_limited_iterator), \
                mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
            daemon.run_once()
        self.assertEqual(expected_success, daemon.stats.successes)
        expected_skipped = expected_total - expected_success
        self.assertEqual(expected_skipped, daemon.stats.skips)
        self.assertEqual(expected_skipped,
                         len(self._find_async_pending_files()))

        orig_iteration = contexts_fed_in[:num_c1_files]
        # we first capture every async fed in one by one
        objs_fed_in = [ctx['update']['obj'] for ctx in orig_iteration]
        self.assertEqual(num_c1_files, len(set(objs_fed_in)))
        # keep track of this order for context
        aorder = {ctx['update']['obj']: 'a%02d' % i
                  for i, ctx in enumerate(orig_iteration)}
        expected_updates_sent = []
        for index_sent in (0, 2, 4, 3):
            expected_updates_sent.extend(
                [contexts_fed_in[index_sent]['update']['obj']] * 3)
        actual_updates_sent = [
            utils.split_path(update[3], minsegs=3)[-1]
            for update in captured_updates
        ]
        self.assertEqual([aorder[o] for o in expected_updates_sent],
                         [aorder[o] for o in actual_updates_sent])

        self.assertEqual([0, 0, 0, 0, 0, 1, 1, 1], captured_skips_stats)

        expected_deferrals = [
            [],
            [],
            [objs_fed_in[1]],
            [objs_fed_in[1]],
            [objs_fed_in[1], objs_fed_in[3]],
            [objs_fed_in[3], objs_fed_in[4]],
            [objs_fed_in[3]],  # note: rightmost element is drained
            [objs_fed_in[3]],
        ]
        self.assertEqual(
            expected_deferrals,
            [[ctx['update']['obj'] for ctx in q] for q in captured_queues])
        actual_sleeps = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertEqual(2, len(actual_sleeps))
        self.assertAlmostEqual(0.1, actual_sleeps[0], 3)
        self.assertAlmostEqual(0.09, actual_sleeps[1], 3)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertTrue(info_lines)
        self.assertIn('4 successes, 0 failures, 0 quarantines, 4 unlinks, '
                      '0 errors, 0 redirects, 1 skips, 3 deferrals, 2 drains',
                      info_lines[-1])
        self.assertEqual(
            {'skips': 1, 'successes': 4, 'unlinks': 4, 'deferrals': 3,
             'drains': 2}, self.logger.get_increment_counts())

    @mock.patch('swift.obj.updater.dump_recon_cache')
    def test_per_container_rate_limit_unsent_deferrals(self, mock_recon):
        # make some updates defer until interval is reached and cycle
        # terminates
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'max_objects_per_container_per_second': 10,
            # only one bucket needed for test
            'per_container_ratelimit_buckets': 1,
            'max_deferred_updates': 5,
            'interval': 0.4,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(self.async_dir)
        # all updates for same container
        num_c1_files = 7
        for i in range(num_c1_files):
            obj_name = 'o%02d' % i
            self._make_async_pending_pickle('a', 'c1', obj_name)
        c1_part, _ = daemon.get_container_ring().get_nodes('a', 'c1')
        expected_total = num_c1_files
        self.assertEqual(expected_total,
                         len(self._find_async_pending_files()))
        # first pass: 0, 2 and 5 succeed, 1, 3, 4, 6 deferred
        # last 2 deferred items sent before interval elapses
        latencies = [0, .05, 0.051, 0, 0, .11, 0, 0,
                     0.1, 0, 0.1, 0]  # total 0.42
        expected_success = 5

        contexts_fed_in = []
        captured_queues = []
        captured_skips_stats = []

        def ratelimit_if(value):
            contexts_fed_in.append(value)
            return False  # returning False overrides normal ratelimiting

        orig_rate_limited_iterator = utils.RateLimitedIterator

        def fake_rate_limited_iterator(*args, **kwargs):
            # insert our own rate limiting function
            kwargs['ratelimit_if'] = ratelimit_if
            return orig_rate_limited_iterator(*args, **kwargs)

        start = time()
        now = [start]

        def fake_get_time(bucket_iter):
            if not captured_skips_stats:
                daemon.begin = now[0]
            captured_skips_stats.append(
                daemon.logger.get_increment_counts().get('skips', 0))
            captured_queues.append(list(bucket_iter.buckets[0].deque))
            # insert delay each time iter is called
            now[0] += latencies.pop(0)
            return now[0]

        captured_updates = []

        def fake_object_update(node, part, op, obj, *args, **kwargs):
            captured_updates.append((node, part, op, obj))
            return True, node['id'], False

        with mock.patch(
                'swift.obj.updater.BucketizedUpdateSkippingLimiter._get_time',
                fake_get_time), \
                mock.patch.object(daemon, 'object_update',
                                  fake_object_update), \
                mock.patch('swift.obj.updater.RateLimitedIterator',
                           fake_rate_limited_iterator), \
                mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
            daemon.run_once()
        self.assertEqual(expected_success, daemon.stats.successes)
        expected_skipped = expected_total - expected_success
        self.assertEqual(expected_skipped, daemon.stats.skips)
        self.assertEqual(expected_skipped,
                         len(self._find_async_pending_files()))

        expected_updates_sent = []
        for index_sent in (0, 2, 5, 6, 4):
            expected_updates_sent.extend(
                [contexts_fed_in[index_sent]['update']['obj']] * 3)

        actual_updates_sent = [
            utils.split_path(update[3], minsegs=3)[-1]
            for update in captured_updates
        ]
        self.assertEqual(expected_updates_sent, actual_updates_sent)

        # skips (un-drained deferrals) not reported until end of cycle
        self.assertEqual([0] * 12, captured_skips_stats)

        objs_fed_in = [ctx['update']['obj'] for ctx in contexts_fed_in]
        expected_deferrals = [
            # queue content before app_iter feeds next update_ctx
            [],
            [],
            [objs_fed_in[1]],
            [objs_fed_in[1]],
            [objs_fed_in[1], objs_fed_in[3]],
            [objs_fed_in[1], objs_fed_in[3], objs_fed_in[4]],
            [objs_fed_in[1], objs_fed_in[3], objs_fed_in[4]],
            # queue content before each update_ctx is drained from queue...
            # note: rightmost element is drained
            [objs_fed_in[1], objs_fed_in[3], objs_fed_in[4], objs_fed_in[6]],
            [objs_fed_in[1], objs_fed_in[3], objs_fed_in[4]],
            [objs_fed_in[1], objs_fed_in[3], objs_fed_in[4]],
            [objs_fed_in[1], objs_fed_in[3]],
            [objs_fed_in[1], objs_fed_in[3]],
        ]
        self.assertEqual(
            expected_deferrals,
            [[ctx['update']['obj'] for ctx in q] for q in captured_queues])
        actual_sleeps = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertEqual(2, len(actual_sleeps))
        self.assertAlmostEqual(0.1, actual_sleeps[0], 3)
        self.assertAlmostEqual(0.1, actual_sleeps[1], 3)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertTrue(info_lines)
        self.assertIn('5 successes, 0 failures, 0 quarantines, 5 unlinks, '
                      '0 errors, 0 redirects, 2 skips, 4 deferrals, 2 drains',
                      info_lines[-1])
        self.assertEqual(
            {'successes': 5, 'unlinks': 5, 'deferrals': 4, 'drains': 2},
            self.logger.get_increment_counts())
        self.assertEqual([('skips', 2)], self.logger.get_update_stats())


class TestObjectUpdaterFunctions(unittest.TestCase):
    def test_split_update_path(self):
        update = {
            'op': 'PUT',
            'account': 'a',
            'container': 'c',
            'obj': 'o',
            'headers': {
                'X-Container-Timestamp': normalize_timestamp(0),
            }
        }
        actual = object_updater.split_update_path(update)
        self.assertEqual(('a', 'c'), actual)

        update['container_path'] = None
        actual = object_updater.split_update_path(update)
        self.assertEqual(('a', 'c'), actual)

        update['container_path'] = '.shards_a/c_shard_n'
        actual = object_updater.split_update_path(update)
        self.assertEqual(('.shards_a', 'c_shard_n'), actual)


class TestBucketizedUpdateSkippingLimiter(unittest.TestCase):

    def setUp(self):
        self.logger = debug_logger()
        self.stats = object_updater.SweepStats()

    def test_init(self):
        it = object_updater.BucketizedUpdateSkippingLimiter(
            [3, 1], self.logger, self.stats, 1000, 10)
        self.assertEqual(1000, it.num_buckets)
        self.assertEqual(0.1, it.bucket_update_delta)
        self.assertEqual([3, 1], [x for x in it.iterator])

        # rate of 0 implies unlimited
        it = object_updater.BucketizedUpdateSkippingLimiter(
            iter([3, 1]), self.logger, self.stats, 9, 0)
        self.assertEqual(9, it.num_buckets)
        self.assertEqual(-1, it.bucket_update_delta)
        self.assertEqual([3, 1], [x for x in it.iterator])

        # num_buckets is collared at 1
        it = object_updater.BucketizedUpdateSkippingLimiter(
            iter([3, 1]), self.logger, self.stats, 0, 1)
        self.assertEqual(1, it.num_buckets)
        self.assertEqual(1, it.bucket_update_delta)
        self.assertEqual([3, 1], [x for x in it.iterator])

    def test_iteration_unlimited(self):
        # verify iteration at unlimited rate
        update_ctxs = [
            {'update': {'account': '%d' % i, 'container': '%s' % i}}
            for i in range(20)]
        it = object_updater.BucketizedUpdateSkippingLimiter(
            iter(update_ctxs), self.logger, self.stats, 9, 0)
        self.assertEqual(update_ctxs, [x for x in it])
        self.assertEqual(0, self.stats.skips)
        self.assertEqual(0, self.stats.drains)
        self.assertEqual(0, self.stats.deferrals)

    def test_iteration_ratelimited(self):
        # verify iteration at limited rate - single bucket
        update_ctxs = [
            {'update': {'account': '%d' % i, 'container': '%s' % i}}
            for i in range(2)]
        it = object_updater.BucketizedUpdateSkippingLimiter(
            iter(update_ctxs), self.logger, self.stats, 1, 0.1)
        # second update is skipped
        self.assertEqual(update_ctxs[:1], [x for x in it])
        self.assertEqual(1, self.stats.skips)
        self.assertEqual(0, self.stats.drains)
        self.assertEqual(1, self.stats.deferrals)

    def test_deferral_single_bucket(self):
        # verify deferral - single bucket
        now = time()
        update_ctxs = [
            {'update': {'account': '%d' % i, 'container': '%s' % i}}
            for i in range(4)]

        # enough capacity for all deferrals
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[now, now, now, now, now, now]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs[:3]), self.logger, self.stats, 1, 10,
                    max_deferred_elements=2,
                    drain_until=now + 10)
                actual = [x for x in it]
        self.assertEqual([update_ctxs[0],
                          update_ctxs[2],  # deferrals...
                          update_ctxs[1]],
                         actual)
        self.assertEqual(2, mock_sleep.call_count)
        self.assertEqual(0, self.stats.skips)
        self.assertEqual(2, self.stats.drains)
        self.assertEqual(2, self.stats.deferrals)
        self.stats.reset()

        # only space for one deferral
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[now, now, now, now, now]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs[:3]), self.logger, self.stats, 1, 10,
                    max_deferred_elements=1,
                    drain_until=now + 10)
                actual = [x for x in it]
        self.assertEqual([update_ctxs[0],
                          update_ctxs[2]],  # deferrals...
                         actual)
        self.assertEqual(1, mock_sleep.call_count)
        self.assertEqual(1, self.stats.skips)
        self.assertEqual(1, self.stats.drains)
        self.assertEqual(2, self.stats.deferrals)
        self.stats.reset()

        # only time for one deferral
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[now, now, now, now, now + 20, now + 20]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs[:3]), self.logger, self.stats, 1, 10,
                    max_deferred_elements=2,
                    drain_until=now + 10)
                actual = [x for x in it]
        self.assertEqual([update_ctxs[0],
                          update_ctxs[2]],  # deferrals...
                         actual)
        self.assertEqual(1, mock_sleep.call_count)
        self.assertEqual(1, self.stats.skips)
        self.assertEqual(1, self.stats.drains)
        self.assertEqual(2, self.stats.deferrals)
        self.stats.reset()

        # only space for two deferrals, only time for one deferral
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[now, now, now, now, now,
                                     now + 20, now + 20]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs), self.logger, self.stats, 1, 10,
                    max_deferred_elements=2,
                    drain_until=now + 10)
                actual = [x for x in it]
        self.assertEqual([update_ctxs[0],
                          update_ctxs[3]],  # deferrals...
                         actual)
        self.assertEqual(1, mock_sleep.call_count)
        self.assertEqual(2, self.stats.skips)
        self.assertEqual(1, self.stats.drains)
        self.assertEqual(3, self.stats.deferrals)
        self.stats.reset()

    def test_deferral_multiple_buckets(self):
        # verify deferral - multiple buckets
        update_ctxs_1 = [
            {'update': {'account': 'a', 'container': 'c1', 'obj': '%3d' % i}}
            for i in range(3)]
        update_ctxs_2 = [
            {'update': {'account': 'a', 'container': 'c2', 'obj': '%3d' % i}}
            for i in range(3)]

        time_iter = itertools.count(time(), 0.001)

        # deferrals stick in both buckets
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[next(time_iter) for _ in range(12)]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs_1 + update_ctxs_2),
                    self.logger, self.stats, 4, 10,
                    max_deferred_elements=4,
                    drain_until=next(time_iter))
                it.salt = ''  # make container->bucket hashing predictable
                actual = [x for x in it]
        self.assertEqual([update_ctxs_1[0],
                          update_ctxs_2[0],
                          update_ctxs_1[2],  # deferrals...
                          update_ctxs_2[2],
                          update_ctxs_1[1],
                          update_ctxs_2[1],
                          ],
                         actual)
        self.assertEqual(4, mock_sleep.call_count)
        self.assertEqual(0, self.stats.skips)
        self.assertEqual(4, self.stats.drains)
        self.assertEqual(4, self.stats.deferrals)
        self.stats.reset()

        # oldest deferral bumped from one bucket due to max_deferrals == 3
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[next(time_iter) for _ in range(10)]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs_1 + update_ctxs_2),
                    self.logger, self.stats, 4, 10,
                    max_deferred_elements=3,
                    drain_until=next(time_iter))
                it.salt = ''  # make container->bucket hashing predictable
                actual = [x for x in it]
        self.assertEqual([update_ctxs_1[0],
                          update_ctxs_2[0],
                          update_ctxs_1[2],  # deferrals...
                          update_ctxs_2[2],
                          update_ctxs_2[1],
                          ],
                         actual)
        self.assertEqual(3, mock_sleep.call_count)
        self.assertEqual(1, self.stats.skips)
        self.assertEqual(3, self.stats.drains)
        self.assertEqual(4, self.stats.deferrals)
        self.stats.reset()

        # older deferrals bumped from one bucket due to max_deferrals == 2
        with mock.patch('swift.obj.updater.time.time',
                        side_effect=[next(time_iter) for _ in range(10)]):
            with mock.patch('swift.obj.updater.time.sleep') as mock_sleep:
                it = object_updater.BucketizedUpdateSkippingLimiter(
                    iter(update_ctxs_1 + update_ctxs_2),
                    self.logger, self.stats, 4, 10,
                    max_deferred_elements=2,
                    drain_until=next(time_iter))
                it.salt = ''  # make container->bucket hashing predictable
                actual = [x for x in it]
        self.assertEqual([update_ctxs_1[0],
                          update_ctxs_2[0],
                          update_ctxs_2[2],  # deferrals...
                          update_ctxs_2[1],
                          ],
                         actual)
        self.assertEqual(2, mock_sleep.call_count)
        self.assertEqual(2, self.stats.skips)
        self.assertEqual(2, self.stats.drains)
        self.assertEqual(4, self.stats.deferrals)
        self.stats.reset()


class TestRateLimiterBucket(unittest.TestCase):
    def test_wait_until(self):
        b1 = object_updater.RateLimiterBucket(10)
        self.assertEqual(10, b1.wait_until)
        b1.last_time = b1.wait_until
        self.assertEqual(20, b1.wait_until)
        b1.last_time = 12345.678
        self.assertEqual(12355.678, b1.wait_until)

    def test_len(self):
        b1 = object_updater.RateLimiterBucket(10)
        b1.deque.append(1)
        b1.deque.append(2)
        self.assertEqual(2, len(b1))
        b1.deque.pop()
        self.assertEqual(1, len(b1))

    def test_bool(self):
        b1 = object_updater.RateLimiterBucket(10)
        self.assertFalse(b1)
        b1.deque.append(1)
        self.assertTrue(b1)
        b1.deque.pop()
        self.assertFalse(b1)

    def test_bucket_ordering(self):
        time_iter = itertools.count(time(), step=0.001)
        b1 = object_updater.RateLimiterBucket(10)
        b2 = object_updater.RateLimiterBucket(10)

        b2.last_time = next(time_iter)
        buckets = PriorityQueue()
        buckets.put(b1)
        buckets.put(b2)
        self.assertEqual([b1, b2], [buckets.get_nowait() for _ in range(2)])

        b1.last_time = next(time_iter)
        buckets.put(b1)
        buckets.put(b2)
        self.assertEqual([b2, b1], [buckets.get_nowait() for _ in range(2)])


class TestSweepStats(unittest.TestCase):
    def test_copy(self):
        num_props = len(vars(object_updater.SweepStats()))
        stats = object_updater.SweepStats(*range(1, num_props + 1))
        stats2 = stats.copy()
        self.assertEqual(vars(stats), vars(stats2))

    def test_since(self):
        stats = object_updater.SweepStats(1, 2, 3, 4, 5, 6, 7, 8, 9)
        stats2 = object_updater.SweepStats(4, 6, 8, 10, 12, 14, 16, 18, 20)
        expected = object_updater.SweepStats(3, 4, 5, 6, 7, 8, 9, 10, 11)
        self.assertEqual(vars(expected), vars(stats2.since(stats)))

    def test_reset(self):
        num_props = len(vars(object_updater.SweepStats()))
        stats = object_updater.SweepStats(*range(1, num_props + 1))
        stats.reset()
        expected = object_updater.SweepStats()
        self.assertEqual(vars(expected), vars(stats))

    def test_str(self):
        num_props = len(vars(object_updater.SweepStats()))
        stats = object_updater.SweepStats(*range(1, num_props + 1))
        self.assertEqual(
            '4 successes, 2 failures, 3 quarantines, 5 unlinks, 1 errors, '
            '6 redirects, 7 skips, 8 deferrals, 9 drains', str(stats))


if __name__ == '__main__':
    unittest.main()
