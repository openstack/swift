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

import cPickle as pickle
import mock
import os
import unittest
import random
import itertools
from contextlib import closing
from gzip import GzipFile
from tempfile import mkdtemp
from shutil import rmtree
from test.unit import FakeLogger
from time import time
from distutils.dir_util import mkpath

from eventlet import spawn, Timeout, listen

from swift.obj import updater as object_updater
from swift.obj.diskfile import (ASYNCDIR_BASE, get_async_dir, DiskFileManager,
                                get_tmp_dir)
from swift.common.ring import RingData
from swift.common import utils
from swift.common.utils import hash_path, normalize_timestamp, mkdirs, \
    write_pickle
from swift.common import swob
from test.unit import debug_logger, patch_policies, mocked_http_conn
from swift.common.storage_policy import StoragePolicy, POLICIES


_mocked_policies = [StoragePolicy(0, 'zero', False),
                    StoragePolicy(1, 'one', True)]


@patch_policies(_mocked_policies)
class TestObjectUpdater(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
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
                           'device': 'sda1', 'zone': 4}], 30),
                f)
        self.devices_dir = os.path.join(self.testdir, 'devices')
        os.mkdir(self.devices_dir)
        self.sda1 = os.path.join(self.devices_dir, 'sda1')
        os.mkdir(self.sda1)
        for policy in POLICIES:
            os.mkdir(os.path.join(self.sda1, get_tmp_dir(policy)))
        self.logger = debug_logger()

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
        daemon = object_updater.ObjectUpdater(conf)
        daemon.logger = FakeLogger()
        paths = daemon._listdir('foo/bar')
        self.assertEqual([], paths)
        log_lines = daemon.logger.get_lines_for_level('error')
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
        daemon = object_updater.ObjectUpdater(conf)
        daemon.logger = FakeLogger()
        path = daemon._listdir('foo/bar/')
        log_lines = daemon.logger.get_lines_for_level('error')
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
            for o, timestamps in objects.iteritems():
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

            cu = MockObjectUpdater({
                'devices': self.devices_dir,
                'mount_check': 'false',
                'swift_dir': self.testdir,
                'interval': '1',
                'concurrency': '1',
                'node_timeout': '5'})
            cu.logger = mock_logger = mock.MagicMock()
            cu.object_sweep(self.sda1)
            self.assertEquals(mock_logger.warn.call_count, warn)
            self.assert_(os.path.exists(os.path.join(self.sda1, 'not_a_dir')))
            if should_skip:
                # if we were supposed to skip over the dir, we didn't process
                # anything at all
                self.assertTrue(os.path.exists(prefix_dir))
                self.assertEqual(set(), seen)
            else:
                self.assert_(not os.path.exists(prefix_dir))
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

    @mock.patch.object(object_updater, 'ismount')
    def test_run_once_with_disk_unmounted(self, mock_ismount):
        mock_ismount.return_value = False
        cu = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'})
        cu.run_once()
        async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(async_dir)
        cu.run_once()
        self.assert_(os.path.exists(async_dir))
        # mount_check == False means no call to ismount
        self.assertEqual([], mock_ismount.mock_calls)

        cu = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'TrUe',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
        odd_dir = os.path.join(async_dir, 'not really supposed '
                               'to be here')
        os.mkdir(odd_dir)
        cu.run_once()
        self.assert_(os.path.exists(async_dir))
        self.assert_(os.path.exists(odd_dir))  # skipped - not mounted!
        # mount_check == True means ismount was checked
        self.assertEqual([
            mock.call(self.sda1),
        ], mock_ismount.mock_calls)
        self.assertEqual(cu.logger.get_increment_counts(), {'errors': 1})

    @mock.patch.object(object_updater, 'ismount')
    def test_run_once(self, mock_ismount):
        mock_ismount.return_value = True
        cu = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
        cu.run_once()
        async_dir = os.path.join(self.sda1, get_async_dir(POLICIES[0]))
        os.mkdir(async_dir)
        cu.run_once()
        self.assert_(os.path.exists(async_dir))
        # mount_check == False means no call to ismount
        self.assertEqual([], mock_ismount.mock_calls)

        cu = object_updater.ObjectUpdater({
            'devices': self.devices_dir,
            'mount_check': 'TrUe',
            'swift_dir': self.testdir,
            'interval': '1',
            'concurrency': '1',
            'node_timeout': '15'}, logger=self.logger)
        odd_dir = os.path.join(async_dir, 'not really supposed '
                               'to be here')
        os.mkdir(odd_dir)
        cu.run_once()
        self.assert_(os.path.exists(async_dir))
        self.assert_(not os.path.exists(odd_dir))
        # mount_check == True means ismount was checked
        self.assertEqual([
            mock.call(self.sda1),
        ], mock_ismount.mock_calls)

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
        cu.run_once()
        self.assert_(not os.path.exists(older_op_path))
        self.assert_(os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
                         {'failures': 1, 'unlinks': 1})
        self.assertEqual(None,
                         pickle.load(open(op_path)).get('successes'))

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
                    headers = swob.HeaderKeyDict()
                    line = inc.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0]] = \
                            line.split(':')[1].strip()
                        line = inc.readline()
                    self.assertTrue('x-container-timestamp' in headers)
                    self.assertTrue('X-Backend-Storage-Policy-Index' in
                                    headers)
            except BaseException as err:
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
            except BaseException as err:
                return err
            return None

        event = spawn(accept, [201, 500, 500])
        for dev in cu.get_container_ring().devs:
            if dev is not None:
                dev['port'] = bindsock.getsockname()[1]

        cu.logger._clear()
        cu.run_once()
        err = event.wait()
        if err:
            raise err
        self.assert_(os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0],
                         pickle.load(open(op_path)).get('successes'))

        event = spawn(accept, [404, 500])
        cu.logger._clear()
        cu.run_once()
        err = event.wait()
        if err:
            raise err
        self.assert_(os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
                         {'failures': 1})
        self.assertEqual([0, 1],
                         pickle.load(open(op_path)).get('successes'))

        event = spawn(accept, [201])
        cu.logger._clear()
        cu.run_once()
        err = event.wait()
        if err:
            raise err
        self.assert_(not os.path.exists(op_path))
        self.assertEqual(cu.logger.get_increment_counts(),
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
            headers_out = swob.HeaderKeyDict({
                'x-size': 0,
                'x-content-type': 'text/plain',
                'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                'x-timestamp': ts.next(),
            })
            data = {'op': op, 'account': account, 'container': container,
                    'obj': obj, 'headers': headers_out}
            dfmanager.pickle_async_update(self.sda1, account, container, obj,
                                          data, ts.next(), policy)

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

    def test_obj_put_async_updates(self):
        ts = (normalize_timestamp(t) for t in
              itertools.count(int(time())))
        policy = random.choice(list(POLICIES))
        # setup updater
        conf = {
            'devices': self.devices_dir,
            'mount_check': 'false',
            'swift_dir': self.testdir,
        }
        daemon = object_updater.ObjectUpdater(conf, logger=self.logger)
        async_dir = os.path.join(self.sda1, get_async_dir(policy))
        os.mkdir(async_dir)

        # write an async
        dfmanager = DiskFileManager(conf, daemon.logger)
        account, container, obj = 'a', 'c', 'o'
        op = 'PUT'
        headers_out = swob.HeaderKeyDict({
            'x-size': 0,
            'x-content-type': 'text/plain',
            'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'x-timestamp': ts.next(),
            'X-Backend-Storage-Policy-Index': int(policy),
        })
        data = {'op': op, 'account': account, 'container': container,
                'obj': obj, 'headers': headers_out}
        dfmanager.pickle_async_update(self.sda1, account, container, obj,
                                      data, ts.next(), policy)

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
            self.assertEqual(headers['X-Backend-Storage-Policy-Index'],
                             str(int(policy)))
        self.assertEqual(daemon.logger.get_increment_counts(),
                         {'successes': 1, 'unlinks': 1, 'async_pendings': 1})


if __name__ == '__main__':
    unittest.main()
