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
import random
import itertools
from contextlib import closing
from gzip import GzipFile
from tempfile import mkdtemp
from shutil import rmtree
from test import listen_zero
from test.unit import (
    make_timestamp_iter, debug_logger, patch_policies, mocked_http_conn,
    FakeLogger)
from time import time
from distutils.dir_util import mkpath

from eventlet import spawn, Timeout

from swift.obj import updater as object_updater
from swift.obj.diskfile import (
    ASYNCDIR_BASE, get_async_dir, DiskFileManager, get_tmp_dir)
from swift.common.ring import RingData
from swift.common import utils
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import (
    hash_path, normalize_timestamp, mkdirs, write_pickle)
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
                         [{'id': 0, 'ip': '127.0.0.1', 'port': 1,
                           'device': 'sda1', 'zone': 0},
                          {'id': 1, 'ip': '127.0.0.1', 'port': 1,
                           'device': 'sda1', 'zone': 2},
                          {'id': 2, 'ip': '127.0.0.1', 'port': 1,
                           'device': 'sda1', 'zone': 4},
                          {'id': 3, 'ip': '127.0.0.1', 'port': 1,
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

        # non-defaults
        conf = {
            'devices': '/some/where/else',
            'mount_check': 'huh?',
            'swift_dir': '/not/here',
            'interval': '600',
            'concurrency': '2',
            'updater_workers': '3',
            'objects_per_second': '10.5',
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        self.assertEqual(daemon.devices, '/some/where/else')
        self.assertEqual(daemon.mount_check, False)
        self.assertEqual(daemon.swift_dir, '/not/here')
        self.assertEqual(daemon.interval, 600)
        self.assertEqual(daemon.concurrency, 2)
        self.assertEqual(daemon.updater_workers, 3)
        self.assertEqual(daemon.max_objects_per_second, 10.5)

        # check deprecated option
        daemon = object_updater.ObjectUpdater({'slowdown': '0.04'},
                                              logger=self.logger)
        self.assertEqual(daemon.max_objects_per_second, 20.0)

        def check_bad(conf):
            with self.assertRaises(ValueError):
                object_updater.ObjectUpdater(conf, logger=self.logger)

        check_bad({'interval': 'foo'})
        check_bad({'interval': '300.0'})
        check_bad({'concurrency': 'bar'})
        check_bad({'concurrency': '1.0'})
        check_bad({'slowdown': 'baz'})
        check_bad({'objects_per_second': 'quux'})

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

    def test_object_sweep(self):
        def check_with_idx(index, warn, should_skip):
            if int(index) > 0:
                asyncdir = os.path.join(self.sda1,
                                        ASYNCDIR_BASE + "-" + index)
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
                             ASYNCDIR_BASE + '-' + str(int(index) + 100)))

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
                        expected.add((o_path, int(index)))
                    write_pickle({}, o_path)

            seen = set()

            class MockObjectUpdater(object_updater.ObjectUpdater):
                def process_object_update(self, update_path, device, policy):
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
            write_pickle({}, o_path)

        class MockObjectUpdater(object_updater.ObjectUpdater):
            def process_object_update(self, update_path, device, policy):
                os.unlink(update_path)
                self.stats.successes += 1
                self.stats.unlinks += 1

        logger = FakeLogger()
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
            now[0] += 5
            return rv

        # With 10s between updates, time() advancing 5s every time we look,
        # and 5 async_pendings on disk, we should get at least two progress
        # lines.
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
                write_pickle({}, o_path)

        class MockObjectUpdater(object_updater.ObjectUpdater):
            def process_object_update(self, update_path, device, policy):
                os.unlink(update_path)
                self.stats.successes += 1
                self.stats.unlinks += 1

        logger = FakeLogger()
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

    @mock.patch.object(object_updater, 'check_drive')
    def test_run_once(self, mock_check_drive):
        mock_check_drive.side_effect = lambda r, d, mc: os.path.join(r, d)
        ou = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
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
        self.assertEqual([
            mock.call(self.devices_dir, 'sda1', True),
        ], mock_check_drive.mock_calls)

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
                    line = inc.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0]] = \
                            line.split(b':')[1].strip()
                        line = inc.readline()
                    self.assertIn(b'x-container-timestamp', headers)
                    self.assertIn(b'X-Backend-Storage-Policy-Index',
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

        event = spawn(accept, [201, 500, 500])
        for dev in ou.get_container_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]

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

        event = spawn(accept, [404, 201])
        ou.logger._clear()
        ou.run_once()
        err = event.wait()
        if err:
            raise err
        self.assertTrue(os.path.exists(op_path))
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0, 2],
                         pickle.load(open(op_path, 'rb')).get('successes'))

        event = spawn(accept, [201])
        ou.logger._clear()
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
        self.assertEqual(ou.logger.get_increment_counts(),
                         {'unlinks': 1, 'successes': 1})

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
        ts_iter = make_timestamp_iter()
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
            self._write_async_update(dfmanager, next(ts_iter), policies[0],
                                     headers=headers_out,
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

        ts = next(ts_iter)
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
        }
        # always expect X-Backend-Accept-Redirect to be true
        do_test(headers_out, expected, container_path='.shards_a/shard_c')
        do_test(headers_out, expected)

        # ...unless X-Backend-Accept-Redirect is already set
        expected['X-Backend-Accept-Redirect'] = 'false'
        headers_out_2 = dict(headers_out)
        headers_out_2['X-Backend-Accept-Redirect'] = 'false'
        do_test(headers_out_2, expected)

        # updater should add policy header if missing
        expected['X-Backend-Accept-Redirect'] = 'true'
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
            'X-Backend-Accept-Redirect': 'true'}
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
            (301, {'Location': '/.shards_a/c_shard_new/o',
                   'X-Backend-Redirect-Timestamp': ts_redirect_2.internal}),
            (301, {'Location': '/.shards_a/c_shard_old/o',
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


if __name__ == '__main__':
    unittest.main()
