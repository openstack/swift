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

from __future__ import print_function
import unittest
from contextlib import contextmanager

import eventlet
import os
import logging
import errno
import math
import time
from shutil import rmtree, copy
from tempfile import mkdtemp, NamedTemporaryFile
import json

import mock
from mock import patch, call
import six
from six.moves import reload_module

from swift.container.backend import DATADIR
from swift.common import db_replicator
from swift.common.utils import (normalize_timestamp, hash_path,
                                storage_directory, Timestamp)
from swift.common.exceptions import DriveNotMounted
from swift.common.swob import HTTPException

from test import unit
from test.unit import FakeLogger, attach_fake_replication_rpc
from test.unit.common.test_db import ExampleBroker


TEST_ACCOUNT_NAME = 'a c t'
TEST_CONTAINER_NAME = 'c o n'


def teardown_module():
    "clean up my monkey patching"
    reload_module(db_replicator)


@contextmanager
def lock_parent_directory(filename):
    yield True


class FakeRing(object):
    class Ring(object):
        devs = []

        def __init__(self, path, reload_time=15, ring_name=None):
            pass

        def get_part(self, account, container=None, obj=None):
            return 0

        def get_part_nodes(self, part):
            return []

        def get_more_nodes(self, *args):
            return []


class FakeRingWithSingleNode(object):
    class Ring(object):
        devs = [dict(
            id=1, weight=10.0, zone=1, ip='1.1.1.1', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.1', replication_port=6200, region=1
        )]

        def __init__(self, path, reload_time=15, ring_name=None):
            pass

        def get_part(self, account, container=None, obj=None):
            return 0

        def get_part_nodes(self, part):
            return self.devs

        def get_more_nodes(self, *args):
            return (d for d in self.devs)


class FakeRingWithNodes(object):
    class Ring(object):
        devs = [dict(
            id=1, weight=10.0, zone=1, ip='1.1.1.1', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.1', replication_port=6200, region=1
        ), dict(
            id=2, weight=10.0, zone=2, ip='1.1.1.2', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.2', replication_port=6200, region=2
        ), dict(
            id=3, weight=10.0, zone=3, ip='1.1.1.3', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.3', replication_port=6200, region=1
        ), dict(
            id=4, weight=10.0, zone=4, ip='1.1.1.4', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.4', replication_port=6200, region=2
        ), dict(
            id=5, weight=10.0, zone=5, ip='1.1.1.5', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.5', replication_port=6200, region=1
        ), dict(
            id=6, weight=10.0, zone=6, ip='1.1.1.6', port=6200, device='sdb',
            meta='', replication_ip='1.1.1.6', replication_port=6200, region=2
        )]

        def __init__(self, path, reload_time=15, ring_name=None):
            pass

        def get_part(self, account, container=None, obj=None):
            return 0

        def get_part_nodes(self, part):
            return self.devs[:3]

        def get_more_nodes(self, *args):
            return (d for d in self.devs[3:])


class FakeProcess(object):
    def __init__(self, *codes):
        self.codes = iter(codes)
        self.args = None
        self.kwargs = None

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        class Failure(object):
            def communicate(innerself):
                next_item = next(self.codes)
                if isinstance(next_item, int):
                    innerself.returncode = next_item
                    return next_item
                raise next_item
        return Failure()


@contextmanager
def _mock_process(*args):
    orig_process = db_replicator.subprocess.Popen
    db_replicator.subprocess.Popen = FakeProcess(*args)
    yield db_replicator.subprocess.Popen
    db_replicator.subprocess.Popen = orig_process


class ReplHttp(object):
    def __init__(self, response=None, set_status=200):
        if isinstance(response, six.text_type):
            response = response.encode('ascii')
        self.response = response
        self.set_status = set_status
    replicated = False
    host = 'localhost'
    node = {
        'ip': '127.0.0.1',
        'port': '6000',
        'device': 'sdb',
    }

    def replicate(self, *args):
        self.replicated = True

        class Response(object):
            status = self.set_status
            data = self.response

            def read(innerself):
                return self.response
        return Response()


class ChangingMtimesOs(object):
    def __init__(self):
        self.mtime = 0

    def __call__(self, *args, **kwargs):
        self.mtime += 1
        return self.mtime


class FakeBroker(object):
    db_file = __file__
    get_repl_missing_table = False
    stub_replication_info = None
    db_type = 'container'
    db_contains_type = 'object'
    info = {'account': TEST_ACCOUNT_NAME, 'container': TEST_CONTAINER_NAME}

    def __init__(self, *args, **kwargs):
        self.locked = False
        self.metadata = {}
        return None

    @contextmanager
    def lock(self):
        self.locked = True
        yield True
        self.locked = False

    def get_sync(self, *args, **kwargs):
        return 5

    def get_syncs(self):
        return []

    def get_items_since(self, point, *args):
        if point == 0:
            return [{'ROWID': 1}]
        if point == -1:
            return [{'ROWID': 1}, {'ROWID': 2}]
        return []

    def merge_syncs(self, *args, **kwargs):
        self.args = args

    def merge_items(self, *args):
        self.args = args

    def get_replication_info(self):
        if self.get_repl_missing_table:
            raise Exception('no such table')
        info = dict(self.info)
        info.update({
            'hash': 12345,
            'delete_timestamp': 0,
            'put_timestamp': 1,
            'created_at': 1,
            'count': 0,
            'max_row': 99,
            'id': 'ID',
            'metadata': {}
        })
        if self.stub_replication_info:
            info.update(self.stub_replication_info)
        return info

    def get_max_row(self, table=None):
        return self.get_replication_info()['max_row']

    def is_reclaimable(self, now, reclaim_age):
        info = self.get_replication_info()
        return info['count'] == 0 and (
            (now - reclaim_age) >
            info['delete_timestamp'] >
            info['put_timestamp'])

    def get_other_replication_items(self):
        return None

    def reclaim(self, item_timestamp, sync_timestamp):
        pass

    def newid(self, remote_d):
        pass

    def update_metadata(self, metadata):
        self.metadata = metadata

    def merge_timestamps(self, created_at, put_timestamp, delete_timestamp):
        self.created_at = created_at
        self.put_timestamp = put_timestamp
        self.delete_timestamp = delete_timestamp

    def get_brokers(self):
        return [self]


class FakeAccountBroker(FakeBroker):
    db_type = 'account'
    db_contains_type = 'container'
    info = {'account': TEST_ACCOUNT_NAME}


class TestReplicator(db_replicator.Replicator):
    server_type = 'container'
    ring_file = 'container.ring.gz'
    brokerclass = FakeBroker
    datadir = DATADIR
    default_port = 1000


class TestDBReplicator(unittest.TestCase):
    def setUp(self):
        db_replicator.ring = FakeRing()
        self.delete_db_calls = []
        self._patchers = []
        # recon cache path
        self.recon_cache = mkdtemp()
        rmtree(self.recon_cache, ignore_errors=1)
        os.mkdir(self.recon_cache)
        self.logger = unit.debug_logger('test-replicator')

    def tearDown(self):
        for patcher in self._patchers:
            patcher.stop()
        rmtree(self.recon_cache, ignore_errors=1)

    def _patch(self, patching_fn, *args, **kwargs):
        patcher = patching_fn(*args, **kwargs)
        patched_thing = patcher.start()
        self._patchers.append(patcher)
        return patched_thing

    def stub_delete_db(self, broker):
        self.delete_db_calls.append('/path/to/file')
        return True

    def test_creation(self):
        # later config should be extended to assert more config options
        replicator = TestReplicator({'node_timeout': '3.5'})
        self.assertEqual(replicator.node_timeout, 3.5)
        self.assertEqual(replicator.databases_per_second, 50)

    def test_repl_connection(self):
        node = {'replication_ip': '127.0.0.1', 'replication_port': 80,
                'device': 'sdb1'}
        conn = db_replicator.ReplConnection(node, '1234567890', 'abcdefg',
                                            logging.getLogger())

        def req(method, path, body, headers):
            self.assertEqual(method, 'REPLICATE')
            self.assertEqual(headers['Content-Type'], 'application/json')

        class Resp(object):
            def read(self):
                return 'data'
        resp = Resp()
        conn.request = req
        conn.getresponse = lambda *args: resp
        self.assertEqual(conn.replicate(1, 2, 3), resp)

        def other_req(method, path, body, headers):
            raise Exception('blah')
        conn.request = other_req
        self.assertIsNone(conn.replicate(1, 2, 3))

    def test_rsync_file(self):
        replicator = TestReplicator({})
        with _mock_process(-1):
            self.assertEqual(
                False,
                replicator._rsync_file('/some/file', 'remote:/some/file'))
        with _mock_process(0):
            self.assertEqual(
                True,
                replicator._rsync_file('/some/file', 'remote:/some/file'))

    def test_rsync_file_popen_args(self):
        replicator = TestReplicator({})
        with _mock_process(0) as process:
            replicator._rsync_file('/some/file', 'remote:/some_file')
            exp_args = ([
                'rsync', '--quiet', '--no-motd',
                '--timeout=%s' % int(math.ceil(replicator.node_timeout)),
                '--contimeout=%s' % int(math.ceil(replicator.conn_timeout)),
                '--whole-file', '/some/file', 'remote:/some_file'],)
            self.assertEqual(exp_args, process.args)

    def test_rsync_file_popen_args_whole_file_false(self):
        replicator = TestReplicator({})
        with _mock_process(0) as process:
            replicator._rsync_file('/some/file', 'remote:/some_file', False)
            exp_args = ([
                'rsync', '--quiet', '--no-motd',
                '--timeout=%s' % int(math.ceil(replicator.node_timeout)),
                '--contimeout=%s' % int(math.ceil(replicator.conn_timeout)),
                '/some/file', 'remote:/some_file'],)
            self.assertEqual(exp_args, process.args)

    def test_rsync_file_popen_args_different_region_and_rsync_compress(self):
        replicator = TestReplicator({})
        for rsync_compress in (False, True):
            replicator.rsync_compress = rsync_compress
            for different_region in (False, True):
                with _mock_process(0) as process:
                    replicator._rsync_file('/some/file', 'remote:/some_file',
                                           False, different_region)
                    if rsync_compress and different_region:
                        # --compress arg should be passed to rsync binary
                        # only when rsync_compress option is enabled
                        # AND destination node is in a different
                        # region
                        self.assertTrue('--compress' in process.args[0])
                    else:
                        self.assertFalse('--compress' in process.args[0])

    def test_rsync_db(self):
        replicator = TestReplicator({})
        replicator._rsync_file = lambda *args, **kwargs: True
        fake_device = {'replication_ip': '127.0.0.1', 'device': 'sda1'}
        replicator._rsync_db(FakeBroker(), fake_device, ReplHttp(), 'abcd')

    def test_rsync_db_rsync_file_call(self):
        fake_device = {'ip': '127.0.0.1', 'port': '0',
                       'replication_ip': '127.0.0.1', 'replication_port': '0',
                       'device': 'sda1'}

        class MyTestReplicator(TestReplicator):
            def __init__(self, db_file, remote_file):
                super(MyTestReplicator, self).__init__({})
                self.db_file = db_file
                self.remote_file = remote_file

            def _rsync_file(self_, db_file, remote_file, whole_file=True,
                            different_region=False):
                self.assertEqual(self_.db_file, db_file)
                self.assertEqual(self_.remote_file, remote_file)
                self_._rsync_file_called = True
                return False

        broker = FakeBroker()
        remote_file = '127.0.0.1::container/sda1/tmp/abcd'
        replicator = MyTestReplicator(broker.db_file, remote_file)
        replicator._rsync_db(broker, fake_device, ReplHttp(), 'abcd')
        self.assertTrue(replicator._rsync_file_called)

    def test_rsync_db_rsync_file_failure(self):
        class MyTestReplicator(TestReplicator):
            def __init__(self):
                super(MyTestReplicator, self).__init__({})
                self._rsync_file_called = False

            def _rsync_file(self_, *args, **kwargs):
                self.assertEqual(
                    False, self_._rsync_file_called,
                    '_sync_file() should only be called once')
                self_._rsync_file_called = True
                return False

        with patch('os.path.exists', lambda *args: True):
            replicator = MyTestReplicator()
            fake_device = {'ip': '127.0.0.1', 'replication_ip': '127.0.0.1',
                           'device': 'sda1'}
            replicator._rsync_db(FakeBroker(), fake_device, ReplHttp(), 'abcd')
            self.assertEqual(True, replicator._rsync_file_called)

    def test_rsync_db_change_after_sync(self):
        class MyTestReplicator(TestReplicator):
            def __init__(self, broker):
                super(MyTestReplicator, self).__init__({})
                self.broker = broker
                self._rsync_file_call_count = 0

            def _rsync_file(self_, db_file, remote_file, whole_file=True,
                            different_region=False):
                self_._rsync_file_call_count += 1
                if self_._rsync_file_call_count == 1:
                    self.assertEqual(True, whole_file)
                    self.assertEqual(False, self_.broker.locked)
                elif self_._rsync_file_call_count == 2:
                    self.assertEqual(False, whole_file)
                    self.assertEqual(True, self_.broker.locked)
                else:
                    raise RuntimeError('_rsync_file() called too many times')
                return True

        # with journal file
        with patch('os.path.exists', lambda *args: True):
            broker = FakeBroker()
            replicator = MyTestReplicator(broker)
            fake_device = {'ip': '127.0.0.1', 'replication_ip': '127.0.0.1',
                           'device': 'sda1'}
            replicator._rsync_db(broker, fake_device, ReplHttp(), 'abcd')
            self.assertEqual(2, replicator._rsync_file_call_count)

        # with new mtime
        with patch('os.path.exists', lambda *args: False):
            with patch('os.path.getmtime', ChangingMtimesOs()):
                broker = FakeBroker()
                replicator = MyTestReplicator(broker)
                fake_device = {'ip': '127.0.0.1',
                               'replication_ip': '127.0.0.1',
                               'device': 'sda1'}
                replicator._rsync_db(broker, fake_device, ReplHttp(), 'abcd')
                self.assertEqual(2, replicator._rsync_file_call_count)

    def test_in_sync(self):
        replicator = TestReplicator({})
        self.assertEqual(replicator._in_sync(
            {'id': 'a', 'point': 0, 'max_row': 0, 'hash': 'b'},
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'b'},
            FakeBroker(), -1), True)
        self.assertEqual(replicator._in_sync(
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'b'},
            {'id': 'a', 'point': -1, 'max_row': 10, 'hash': 'b'},
            FakeBroker(), -1), True)
        self.assertEqual(bool(replicator._in_sync(
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'c'},
            {'id': 'a', 'point': -1, 'max_row': 10, 'hash': 'd'},
            FakeBroker(), -1)), False)

    def test_run_once_no_local_device_in_ring(self):
        logger = unit.debug_logger('test-replicator')
        replicator = TestReplicator({'recon_cache_path': self.recon_cache},
                                    logger=logger)
        with patch('swift.common.db_replicator.whataremyips',
                   return_value=['127.0.0.1']):
            replicator.run_once()
        expected = [
            "Can't find itself 127.0.0.1 with port 1000 "
            "in ring file, not replicating",
        ]
        self.assertEqual(expected, logger.get_lines_for_level('error'))

    def test_run_once_with_local_device_in_ring(self):
        logger = unit.debug_logger('test-replicator')
        base = 'swift.common.db_replicator.'
        with patch(base + 'whataremyips', return_value=['1.1.1.1']), \
                patch(base + 'ring', FakeRingWithNodes()):
            replicator = TestReplicator({'bind_port': 6200,
                                         'recon_cache_path': self.recon_cache},
                                        logger=logger)
            replicator.run_once()
        self.assertFalse(logger.get_lines_for_level('error'))

    def test_run_once_no_ips(self):
        replicator = TestReplicator({}, logger=unit.FakeLogger())
        self._patch(patch.object, db_replicator, 'whataremyips',
                    lambda *a, **kw: [])

        replicator.run_once()

        self.assertEqual(
            replicator.logger.log_dict['error'],
            [(('ERROR Failed to get my own IPs?',), {})])

    def test_run_once_node_is_not_mounted(self):
        db_replicator.ring = FakeRingWithSingleNode()
        # If a bind_ip is specified, it's plumbed into whataremyips() and
        # returned by itself.
        conf = {'mount_check': 'true', 'bind_ip': '1.1.1.1',
                'bind_port': 6200}
        replicator = TestReplicator(conf, logger=unit.FakeLogger())
        self.assertEqual(replicator.mount_check, True)
        self.assertEqual(replicator.port, 6200)

        err = ValueError('Boom!')

        def mock_check_drive(root, device, mount_check):
            self.assertEqual(root, replicator.root)
            self.assertEqual(device, replicator.ring.devs[0]['device'])
            self.assertEqual(mount_check, True)
            raise err

        self._patch(patch.object, db_replicator, 'check_drive',
                    mock_check_drive)
        replicator.run_once()

        self.assertEqual(
            replicator.logger.log_dict['warning'],
            [(('Skipping: %s', (err, )), {})])

    def test_run_once_node_is_mounted(self):
        db_replicator.ring = FakeRingWithSingleNode()
        conf = {'mount_check': 'true', 'bind_port': 6200}
        replicator = TestReplicator(conf, logger=unit.FakeLogger())
        self.assertEqual(replicator.mount_check, True)
        self.assertEqual(replicator.port, 6200)

        def mock_unlink_older_than(path, mtime):
            self.assertEqual(path,
                             os.path.join(replicator.root,
                                          replicator.ring.devs[0]['device'],
                                          'tmp'))
            self.assertTrue(time.time() - replicator.reclaim_age >= mtime)

        def mock_spawn_n(fn, part, object_file, node_id):
            self.assertEqual('123', part)
            self.assertEqual('/srv/node/sda/c.db', object_file)
            self.assertEqual(1, node_id)

        self._patch(patch.object, db_replicator, 'whataremyips',
                    lambda *a, **kw: ['1.1.1.1'])
        self._patch(patch.object, db_replicator, 'unlink_older_than',
                    mock_unlink_older_than)
        self._patch(patch.object, db_replicator, 'roundrobin_datadirs',
                    lambda *args: [('123', '/srv/node/sda/c.db', 1)])
        self._patch(patch.object, replicator.cpool, 'spawn_n', mock_spawn_n)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(ismount=True) as mocks:
            mock_os.path.isdir.return_value = True
            replicator.run_once()
            mock_os.path.isdir.assert_called_with(
                os.path.join(replicator.root,
                             replicator.ring.devs[0]['device'],
                             replicator.datadir))
            self.assertEqual([
                mock.call(os.path.join(
                    replicator.root,
                    replicator.ring.devs[0]['device'])),
            ], mocks['ismount'].call_args_list)

    def test_usync(self):
        fake_http = ReplHttp()
        replicator = TestReplicator({})
        replicator._usync_db(0, FakeBroker(), fake_http, '12345', '67890')

    def test_usync_http_error_above_300(self):
        fake_http = ReplHttp(set_status=301)
        replicator = TestReplicator({})
        self.assertFalse(
            replicator._usync_db(0, FakeBroker(), fake_http, '12345', '67890'))

    def test_usync_http_error_below_200(self):
        fake_http = ReplHttp(set_status=101)
        replicator = TestReplicator({})
        self.assertFalse(
            replicator._usync_db(0, FakeBroker(), fake_http, '12345', '67890'))

    @mock.patch('swift.common.db_replicator.dump_recon_cache')
    @mock.patch('swift.common.db_replicator.time.time', return_value=1234.5678)
    def test_stats(self, mock_time, mock_recon_cache):
        logger = unit.debug_logger('test-replicator')
        replicator = TestReplicator({}, logger=logger)
        replicator._zero_stats()
        self.assertEqual(replicator.stats['start'], mock_time.return_value)
        replicator._report_stats()
        self.assertEqual(logger.get_lines_for_level('info'), [
            'Attempted to replicate 0 dbs in 0.00000 seconds (0.00000/s)',
            'Removed 0 dbs',
            '0 successes, 0 failures',
            'diff:0 diff_capped:0 empty:0 hashmatch:0 no_change:0 '
            'remote_merge:0 rsync:0 ts_repl:0',
        ])
        self.assertEqual(1, len(mock_recon_cache.mock_calls))
        self.assertEqual(mock_recon_cache.mock_calls[0][1][0], {
            'replication_time': 0.0,
            'replication_last': mock_time.return_value,
            'replication_stats': replicator.stats,
        })

        mock_recon_cache.reset_mock()
        logger.clear()
        replicator.stats.update({
            'attempted': 30,
            'success': 25,
            'remove': 9,
            'failure': 1,

            'diff': 5,
            'diff_capped': 4,
            'empty': 7,
            'hashmatch': 8,
            'no_change': 6,
            'remote_merge': 2,
            'rsync': 3,
            'ts_repl': 10,
        })
        mock_time.return_value += 246.813576
        replicator._report_stats()
        self.maxDiff = None
        self.assertEqual(logger.get_lines_for_level('info'), [
            'Attempted to replicate 30 dbs in 246.81358 seconds (0.12155/s)',
            'Removed 9 dbs',
            '25 successes, 1 failures',
            'diff:5 diff_capped:4 empty:7 hashmatch:8 no_change:6 '
            'remote_merge:2 rsync:3 ts_repl:10',
        ])
        self.assertEqual(1, len(mock_recon_cache.mock_calls))
        self.assertEqual(mock_recon_cache.mock_calls[0][1][0], {
            'replication_time': 246.813576,
            'replication_last': mock_time.return_value,
            'replication_stats': replicator.stats,
        })

    def test_replicate_object(self):
        # verify return values from replicate_object
        db_replicator.ring = FakeRingWithNodes()
        db_path = '/path/to/file'
        replicator = TestReplicator({}, logger=FakeLogger())
        info = FakeBroker().get_replication_info()
        # make remote appear to be in sync
        rinfo = {'point': info['max_row'], 'id': 'remote_id'}

        class FakeResponse(object):
            def __init__(self, status, rinfo):
                self._status = status
                self.data = json.dumps(rinfo).encode('ascii')

            @property
            def status(self):
                if isinstance(self._status, (Exception, eventlet.Timeout)):
                    raise self._status
                return self._status

        # all requests fail
        replicate = 'swift.common.db_replicator.ReplConnection.replicate'
        with mock.patch(replicate) as fake_replicate:
            fake_replicate.side_effect = [
                FakeResponse(500, None),
                FakeResponse(500, None),
                FakeResponse(500, None)]
            with mock.patch.object(replicator, 'delete_db') as mock_delete:
                res = replicator._replicate_object('0', db_path, 'node_id')
        self.assertRaises(StopIteration, next, fake_replicate.side_effect)
        self.assertEqual((False, [False, False, False]), res)
        self.assertEqual(0, mock_delete.call_count)
        self.assertFalse(replicator.logger.get_lines_for_level('error'))
        self.assertFalse(replicator.logger.get_lines_for_level('warning'))
        replicator.logger.clear()

        with mock.patch(replicate) as fake_replicate:
            fake_replicate.side_effect = [
                FakeResponse(Exception('ugh'), None),
                FakeResponse(eventlet.Timeout(), None),
                FakeResponse(200, rinfo)]
            with mock.patch.object(replicator, 'delete_db') as mock_delete:
                res = replicator._replicate_object('0', db_path, 'node_id')
        self.assertRaises(StopIteration, next, fake_replicate.side_effect)
        self.assertEqual((False, [False, False, True]), res)
        self.assertEqual(0, mock_delete.call_count)
        lines = replicator.logger.get_lines_for_level('error')
        self.assertIn('ERROR syncing', lines[0])
        self.assertIn('ERROR syncing', lines[1])
        self.assertFalse(lines[2:])
        self.assertFalse(replicator.logger.get_lines_for_level('warning'))
        replicator.logger.clear()

        # partial success
        with mock.patch(replicate) as fake_replicate:
            fake_replicate.side_effect = [
                FakeResponse(200, rinfo),
                FakeResponse(200, rinfo),
                FakeResponse(500, None)]
            with mock.patch.object(replicator, 'delete_db') as mock_delete:
                res = replicator._replicate_object('0', db_path, 'node_id')
        self.assertRaises(StopIteration, next, fake_replicate.side_effect)
        self.assertEqual((False, [True, True, False]), res)
        self.assertEqual(0, mock_delete.call_count)
        self.assertFalse(replicator.logger.get_lines_for_level('error'))
        self.assertFalse(replicator.logger.get_lines_for_level('warning'))
        replicator.logger.clear()

        # 507 triggers additional requests
        with mock.patch(replicate) as fake_replicate:
            fake_replicate.side_effect = [
                FakeResponse(200, rinfo),
                FakeResponse(200, rinfo),
                FakeResponse(507, None),
                FakeResponse(507, None),
                FakeResponse(200, rinfo)]
            with mock.patch.object(replicator, 'delete_db') as mock_delete:
                res = replicator._replicate_object('0', db_path, 'node_id')
        self.assertRaises(StopIteration, next, fake_replicate.side_effect)
        self.assertEqual((False, [True, True, False, False, True]), res)
        self.assertEqual(0, mock_delete.call_count)
        lines = replicator.logger.get_lines_for_level('error')
        self.assertIn('Remote drive not mounted', lines[0])
        self.assertIn('Remote drive not mounted', lines[1])
        self.assertFalse(lines[2:])
        self.assertFalse(replicator.logger.get_lines_for_level('warning'))
        replicator.logger.clear()

        # all requests succeed; node id == 'node_id' causes node to be
        # considered a handoff so expect the db to be deleted
        with mock.patch(replicate) as fake_replicate:
            fake_replicate.side_effect = [
                FakeResponse(200, rinfo),
                FakeResponse(200, rinfo),
                FakeResponse(200, rinfo)]
            with mock.patch.object(replicator, 'delete_db') as mock_delete:
                res = replicator._replicate_object('0', db_path, 'node_id')
        self.assertRaises(StopIteration, next, fake_replicate.side_effect)
        self.assertEqual((True, [True, True, True]), res)
        self.assertEqual(1, mock_delete.call_count)
        self.assertFalse(replicator.logger.get_lines_for_level('error'))
        self.assertFalse(replicator.logger.get_lines_for_level('warning'))

    def test_replicate_object_quarantine(self):
        replicator = TestReplicator({})
        self._patch(patch.object, replicator.brokerclass, 'db_file',
                    '/a/b/c/d/e/hey')
        self._patch(patch.object, replicator.brokerclass,
                    'get_repl_missing_table', True)

        def mock_renamer(was, new, fsync=False, cause_colision=False):
            if cause_colision and '-' not in new:
                raise OSError(errno.EEXIST, "File already exists")
            self.assertEqual('/a/b/c/d/e', was)
            if '-' in new:
                self.assertTrue(
                    new.startswith('/a/quarantined/containers/e-'))
            else:
                self.assertEqual('/a/quarantined/containers/e', new)

        def mock_renamer_error(was, new, fsync):
            return mock_renamer(was, new, fsync, cause_colision=True)
        with patch.object(db_replicator, 'renamer', mock_renamer):
            replicator._replicate_object('0', 'file', 'node_id')
        # try the double quarantine
        with patch.object(db_replicator, 'renamer', mock_renamer_error):
            replicator._replicate_object('0', 'file', 'node_id')

    def test_replicate_object_delete_because_deleted(self):
        replicator = TestReplicator({})
        try:
            replicator.delete_db = self.stub_delete_db
            replicator.brokerclass.stub_replication_info = {
                'delete_timestamp': 2, 'put_timestamp': 1}
            replicator._replicate_object('0', '/path/to/file', 'node_id')
        finally:
            replicator.brokerclass.stub_replication_info = None
        self.assertEqual(['/path/to/file'], self.delete_db_calls)

    def test_replicate_object_delete_because_not_shouldbehere(self):
        replicator = TestReplicator({})
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator.brokerclass = FakeAccountBroker
        replicator._repl_to_node = lambda *args: True
        replicator.delete_db = self.stub_delete_db
        orig_cleanup = replicator.cleanup_post_replicate
        with mock.patch.object(replicator, 'cleanup_post_replicate',
                               side_effect=orig_cleanup) as mock_cleanup:
            replicator._replicate_object('0', '/path/to/file', 'node_id')
        mock_cleanup.assert_called_once_with(mock.ANY, mock.ANY, [True] * 3)
        self.assertIsInstance(mock_cleanup.call_args[0][0],
                              replicator.brokerclass)
        self.assertEqual(['/path/to/file'], self.delete_db_calls)
        self.assertEqual(0, replicator.stats['failure'])

    def test_replicate_object_delete_delegated_to_cleanup_post_replicate(self):
        replicator = TestReplicator({})
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator.brokerclass = FakeAccountBroker
        replicator._repl_to_node = lambda *args: True
        replicator.delete_db = self.stub_delete_db

        # cleanup succeeds
        with mock.patch.object(replicator, 'cleanup_post_replicate',
                               return_value=True) as mock_cleanup:
            replicator._replicate_object('0', '/path/to/file', 'node_id')
        mock_cleanup.assert_called_once_with(mock.ANY, mock.ANY, [True] * 3)
        self.assertIsInstance(mock_cleanup.call_args[0][0],
                              replicator.brokerclass)
        self.assertFalse(self.delete_db_calls)
        self.assertEqual(0, replicator.stats['failure'])
        self.assertEqual(3, replicator.stats['success'])

        # cleanup fails
        replicator._zero_stats()
        with mock.patch.object(replicator, 'cleanup_post_replicate',
                               return_value=False) as mock_cleanup:
            replicator._replicate_object('0', '/path/to/file', 'node_id')
        mock_cleanup.assert_called_once_with(mock.ANY, mock.ANY, [True] * 3)
        self.assertIsInstance(mock_cleanup.call_args[0][0],
                              replicator.brokerclass)
        self.assertFalse(self.delete_db_calls)
        self.assertEqual(3, replicator.stats['failure'])
        self.assertEqual(0, replicator.stats['success'])

        # shouldbehere True - cleanup not required
        replicator._zero_stats()
        primary_node_id = replicator.ring.get_part_nodes('0')[0]['id']
        with mock.patch.object(replicator, 'cleanup_post_replicate',
                               return_value=True) as mock_cleanup:
            replicator._replicate_object('0', '/path/to/file', primary_node_id)
        mock_cleanup.assert_not_called()
        self.assertFalse(self.delete_db_calls)
        self.assertEqual(0, replicator.stats['failure'])
        self.assertEqual(2, replicator.stats['success'])

    def test_cleanup_post_replicate(self):
        replicator = TestReplicator({}, logger=self.logger)
        replicator.ring = FakeRingWithNodes().Ring('path')
        broker = FakeBroker()
        replicator._repl_to_node = lambda *args: True
        info = broker.get_replication_info()

        with mock.patch.object(replicator, 'delete_db') as mock_delete_db:
            res = replicator.cleanup_post_replicate(
                broker, info, [False] * 3)
        mock_delete_db.assert_not_called()
        self.assertTrue(res)
        self.assertEqual(['Not deleting db %s (0/3 success)' % broker.db_file],
                         replicator.logger.get_lines_for_level('debug'))
        replicator.logger.clear()

        with mock.patch.object(replicator, 'delete_db') as mock_delete_db:
            res = replicator.cleanup_post_replicate(
                broker, info, [True, False, True])
        mock_delete_db.assert_not_called()
        self.assertTrue(res)
        self.assertEqual(['Not deleting db %s (2/3 success)' % broker.db_file],
                         replicator.logger.get_lines_for_level('debug'))
        replicator.logger.clear()

        broker.stub_replication_info = {'max_row': 101}
        with mock.patch.object(replicator, 'delete_db') as mock_delete_db:
            res = replicator.cleanup_post_replicate(
                broker, info, [True] * 3)
        mock_delete_db.assert_not_called()
        self.assertTrue(res)
        self.assertEqual(['Not deleting db %s (2 new rows)' % broker.db_file],
                         replicator.logger.get_lines_for_level('debug'))
        replicator.logger.clear()

        broker.stub_replication_info = {'max_row': 98}
        with mock.patch.object(replicator, 'delete_db') as mock_delete_db:
            res = replicator.cleanup_post_replicate(
                broker, info, [True] * 3)
        mock_delete_db.assert_not_called()
        self.assertTrue(res)
        broker.stub_replication_info = None
        self.assertEqual(['Not deleting db %s (negative max_row_delta: -1)' %
                          broker.db_file],
                         replicator.logger.get_lines_for_level('error'))
        replicator.logger.clear()

        with mock.patch.object(replicator, 'delete_db') as mock_delete_db:
            res = replicator.cleanup_post_replicate(
                broker, info, [True] * 3)
        mock_delete_db.assert_called_once_with(broker)
        self.assertTrue(res)
        self.assertEqual(['Successfully deleted db %s' % broker.db_file],
                         replicator.logger.get_lines_for_level('debug'))
        replicator.logger.clear()

        with mock.patch.object(replicator, 'delete_db',
                               return_value=False) as mock_delete_db:
            res = replicator.cleanup_post_replicate(
                broker, info, [True] * 3)
        mock_delete_db.assert_called_once_with(broker)
        self.assertFalse(res)
        self.assertEqual(['Failed to delete db %s' % broker.db_file],
                         replicator.logger.get_lines_for_level('debug'))
        replicator.logger.clear()

    def test_replicate_object_with_exception(self):
        replicator = TestReplicator({})
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator.brokerclass = FakeAccountBroker
        replicator.delete_db = self.stub_delete_db
        replicator._repl_to_node = mock.Mock(side_effect=Exception())
        replicator._replicate_object('0', '/path/to/file',
                                     replicator.ring.devs[0]['id'])
        self.assertEqual(2, replicator._repl_to_node.call_count)
        # with one DriveNotMounted exception called on +1 more replica
        replicator._repl_to_node = mock.Mock(side_effect=[DriveNotMounted()])
        replicator._replicate_object('0', '/path/to/file',
                                     replicator.ring.devs[0]['id'])
        self.assertEqual(3, replicator._repl_to_node.call_count)
        # called on +1 more replica and self when *first* handoff
        replicator._repl_to_node = mock.Mock(side_effect=[DriveNotMounted()])
        replicator._replicate_object('0', '/path/to/file',
                                     replicator.ring.devs[3]['id'])
        self.assertEqual(4, replicator._repl_to_node.call_count)
        # even if it's the last handoff it works to keep 3 replicas
        # 2 primaries + 1 handoff
        replicator._repl_to_node = mock.Mock(side_effect=[DriveNotMounted()])
        replicator._replicate_object('0', '/path/to/file',
                                     replicator.ring.devs[-1]['id'])
        self.assertEqual(4, replicator._repl_to_node.call_count)
        # with two DriveNotMounted exceptions called on +2 more replica keeping
        # durability
        replicator._repl_to_node = mock.Mock(
            side_effect=[DriveNotMounted()] * 2)
        replicator._replicate_object('0', '/path/to/file',
                                     replicator.ring.devs[0]['id'])
        self.assertEqual(4, replicator._repl_to_node.call_count)

    def test_replicate_object_with_exception_run_out_of_nodes(self):
        replicator = TestReplicator({})
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator.brokerclass = FakeAccountBroker
        replicator.delete_db = self.stub_delete_db
        # all other devices are not mounted
        replicator._repl_to_node = mock.Mock(side_effect=DriveNotMounted())
        replicator._replicate_object('0', '/path/to/file',
                                     replicator.ring.devs[0]['id'])
        self.assertEqual(5, replicator._repl_to_node.call_count)

    def test_replicate_account_out_of_place(self):
        replicator = TestReplicator({}, logger=unit.FakeLogger())
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator.brokerclass = FakeAccountBroker
        replicator._repl_to_node = lambda *args: True
        replicator.delete_db = self.stub_delete_db
        # Correct node_id, wrong part
        part = replicator.ring.get_part(TEST_ACCOUNT_NAME) + 1
        node_id = replicator.ring.get_part_nodes(part)[0]['id']
        replicator._replicate_object(str(part), '/path/to/file', node_id)
        self.assertEqual(['/path/to/file'], self.delete_db_calls)
        error_msgs = replicator.logger.get_lines_for_level('error')
        expected = 'Found /path/to/file for /a%20c%20t when it should be ' \
            'on partition 0; will replicate out and remove.'
        self.assertEqual(error_msgs, [expected])

    def test_replicate_container_out_of_place(self):
        replicator = TestReplicator({}, logger=unit.FakeLogger())
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator._repl_to_node = lambda *args: True
        replicator.delete_db = self.stub_delete_db
        # Correct node_id, wrong part
        part = replicator.ring.get_part(
            TEST_ACCOUNT_NAME, TEST_CONTAINER_NAME) + 1
        node_id = replicator.ring.get_part_nodes(part)[0]['id']
        replicator._replicate_object(str(part), '/path/to/file', node_id)
        self.assertEqual(['/path/to/file'], self.delete_db_calls)
        self.assertEqual(
            replicator.logger.log_dict['error'],
            [(('Found /path/to/file for /a%20c%20t/c%20o%20n when it should '
               'be on partition 0; will replicate out and remove.',), {})])

    def test_replicate_container_out_of_place_no_node(self):
        replicator = TestReplicator({}, logger=unit.FakeLogger())
        replicator.ring = FakeRingWithSingleNode().Ring('path')
        replicator._repl_to_node = lambda *args: True

        replicator.delete_db = self.stub_delete_db
        # Correct node_id, wrong part
        part = replicator.ring.get_part(
            TEST_ACCOUNT_NAME, TEST_CONTAINER_NAME) + 1
        node_id = replicator.ring.get_part_nodes(part)[0]['id']
        replicator._replicate_object(str(part), '/path/to/file', node_id)
        self.assertEqual(['/path/to/file'], self.delete_db_calls)

        self.delete_db_calls = []

        # No nodes this time
        replicator.ring.get_part_nodes = lambda *args: []
        replicator.delete_db = self.stub_delete_db
        # Correct node_id, wrong part
        part = replicator.ring.get_part(
            TEST_ACCOUNT_NAME, TEST_CONTAINER_NAME) + 1
        replicator._replicate_object(str(part), '/path/to/file', node_id)
        self.assertEqual([], self.delete_db_calls)

    def test_replicate_object_different_region(self):
        db_replicator.ring = FakeRingWithNodes()
        replicator = TestReplicator({})
        replicator._repl_to_node = mock.Mock()
        # For node_id = 1, one replica in same region(1) and other is in a
        # different region(2). Refer: FakeRingWithNodes
        replicator._replicate_object('0', '/path/to/file', 1)
        # different_region was set True and passed to _repl_to_node()
        self.assertEqual(replicator._repl_to_node.call_args_list[0][0][-1],
                         True)
        # different_region was set False and passed to _repl_to_node()
        self.assertEqual(replicator._repl_to_node.call_args_list[1][0][-1],
                         False)

    def test_delete_db(self):
        db_replicator.lock_parent_directory = lock_parent_directory
        replicator = TestReplicator({}, logger=unit.FakeLogger())
        replicator._zero_stats()
        replicator.extract_device = lambda _: 'some_device'

        temp_dir = mkdtemp()
        try:
            temp_suf_dir = os.path.join(temp_dir, '16e')
            os.mkdir(temp_suf_dir)
            temp_hash_dir = os.path.join(temp_suf_dir,
                                         '166e33924a08ede4204871468c11e16e')
            os.mkdir(temp_hash_dir)
            temp_file = NamedTemporaryFile(dir=temp_hash_dir, delete=False)
            temp_hash_dir2 = os.path.join(temp_suf_dir,
                                          '266e33924a08ede4204871468c11e16e')
            os.mkdir(temp_hash_dir2)
            temp_file2 = NamedTemporaryFile(dir=temp_hash_dir2, delete=False)

            # sanity-checks
            self.assertTrue(os.path.exists(temp_dir))
            self.assertTrue(os.path.exists(temp_suf_dir))
            self.assertTrue(os.path.exists(temp_hash_dir))
            self.assertTrue(os.path.exists(temp_file.name))
            self.assertTrue(os.path.exists(temp_hash_dir2))
            self.assertTrue(os.path.exists(temp_file2.name))
            self.assertEqual(0, replicator.stats['remove'])

            temp_file.db_file = temp_file.name
            replicator.delete_db(temp_file)

            self.assertTrue(os.path.exists(temp_dir))
            self.assertTrue(os.path.exists(temp_suf_dir))
            self.assertFalse(os.path.exists(temp_hash_dir))
            self.assertFalse(os.path.exists(temp_file.name))
            self.assertTrue(os.path.exists(temp_hash_dir2))
            self.assertTrue(os.path.exists(temp_file2.name))
            self.assertEqual([(('removes.some_device',), {})],
                             replicator.logger.log_dict['increment'])
            self.assertEqual(1, replicator.stats['remove'])

            temp_file2.db_file = temp_file2.name
            replicator.delete_db(temp_file2)

            self.assertTrue(os.path.exists(temp_dir))
            self.assertFalse(os.path.exists(temp_suf_dir))
            self.assertFalse(os.path.exists(temp_hash_dir))
            self.assertFalse(os.path.exists(temp_file.name))
            self.assertFalse(os.path.exists(temp_hash_dir2))
            self.assertFalse(os.path.exists(temp_file2.name))
            self.assertEqual([(('removes.some_device',), {})] * 2,
                             replicator.logger.log_dict['increment'])
            self.assertEqual(2, replicator.stats['remove'])
        finally:
            rmtree(temp_dir)

    def test_extract_device(self):
        replicator = TestReplicator({'devices': '/some/root'})
        self.assertEqual('some_device', replicator.extract_device(
            '/some/root/some_device/deeper/and/deeper'))
        self.assertEqual('UNKNOWN', replicator.extract_device(
            '/some/foo/some_device/deeper/and/deeper'))

    def test_dispatch_no_arg_pop(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        with unit.mock_check_drive(isdir=True):
            response = rpc.dispatch(('a',), 'arg')
        self.assertEqual(b'Invalid object type', response.body)
        self.assertEqual(400, response.status_int)

    def test_dispatch_drive_not_mounted(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=True)

        with unit.mock_check_drive() as mocks:
            response = rpc.dispatch(('drive', 'part', 'hash'), ['method'])
        self.assertEqual([mock.call(os.path.join('/drive'))],
                         mocks['ismount'].call_args_list)

        self.assertEqual('507 drive is not mounted', response.status)
        self.assertEqual(507, response.status_int)

    def test_dispatch_unexpected_operation_db_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        def mock_mkdirs(path):
            self.assertEqual('/drive/tmp', path)

        self._patch(patch.object, db_replicator, 'mkdirs', mock_mkdirs)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = False
            response = rpc.dispatch(('drive', 'part', 'hash'), ['unexpected'])

        self.assertEqual('404 Not Found', response.status)
        self.assertEqual(404, response.status_int)

    def test_dispatch_operation_unexpected(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        self._patch(patch.object, db_replicator, 'mkdirs', lambda *args: True)

        def unexpected_method(broker, args):
            self.assertEqual(FakeBroker, broker.__class__)
            self.assertEqual(['arg1', 'arg2'], args)
            return 'unexpected-called'

        rpc.unexpected = unexpected_method

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = True
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['unexpected', 'arg1', 'arg2'])
            mock_os.path.exists.assert_called_with('/part/ash/hash/hash.db')

        self.assertEqual('unexpected-called', response)

    def test_dispatch_operation_rsync_then_merge(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        self._patch(patch.object, db_replicator, 'renamer', lambda *args: True)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = True
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['rsync_then_merge', 'arg1', 'arg2'])
            expected_calls = [call('/part/ash/hash/hash.db'),
                              call('/drive/tmp/arg1'),
                              call(FakeBroker.db_file),
                              call('/drive/tmp/arg1')]
            self.assertEqual(mock_os.path.exists.call_args_list,
                             expected_calls)
            self.assertEqual('204 No Content', response.status)
            self.assertEqual(204, response.status_int)

    def test_dispatch_operation_complete_rsync(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        self._patch(patch.object, db_replicator, 'renamer', lambda *args: True)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.side_effect = [False, True]
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['complete_rsync', 'arg1'])
            expected_calls = [call('/part/ash/hash/hash.db'),
                              call('/drive/tmp/arg1')]
            self.assertEqual(mock_os.path.exists.call_args_list,
                             expected_calls)
            self.assertEqual('204 No Content', response.status)
            self.assertEqual(204, response.status_int)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.side_effect = [False, True]
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['complete_rsync', 'arg1', 'arg2'])
            expected_calls = [call('/part/ash/hash/arg2'),
                              call('/drive/tmp/arg1')]
            self.assertEqual(mock_os.path.exists.call_args_list,
                             expected_calls)
            self.assertEqual('204 No Content', response.status)
            self.assertEqual(204, response.status_int)

    def test_rsync_then_merge_db_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = False
            response = rpc.rsync_then_merge('drive', '/data/db.db',
                                            ('arg1', 'arg2'))
            mock_os.path.exists.assert_called_with('/data/db.db')
            self.assertEqual('404 Not Found', response.status)
            self.assertEqual(404, response.status_int)

    def test_rsync_then_merge_old_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.side_effect = [True, False]
            response = rpc.rsync_then_merge('drive', '/data/db.db',
                                            ('arg1', 'arg2'))
            expected_calls = [call('/data/db.db'), call('/drive/tmp/arg1')]
            self.assertEqual(mock_os.path.exists.call_args_list,
                             expected_calls)
            self.assertEqual('404 Not Found', response.status)
            self.assertEqual(404, response.status_int)

    def test_rsync_then_merge_with_objects(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        def mock_renamer(old, new):
            self.assertEqual('/drive/tmp/arg1', old)
            # FakeBroker uses module filename as db_file!
            self.assertEqual(__file__, new)

        self._patch(patch.object, db_replicator, 'renamer', mock_renamer)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = True
            response = rpc.rsync_then_merge('drive', '/data/db.db',
                                            ['arg1', 'arg2'])
            self.assertEqual('204 No Content', response.status)
            self.assertEqual(204, response.status_int)

    def test_complete_rsync_db_exists(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = True
            response = rpc.complete_rsync('drive', '/data/db.db', ['arg1'])
            mock_os.path.exists.assert_called_with('/data/db.db')
            self.assertEqual('404 Not Found', response.status)
            self.assertEqual(404, response.status_int)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = True
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1', 'arg2'])
            mock_os.path.exists.assert_called_with('/data/arg2')
            self.assertEqual('404 Not Found', response.status)
            self.assertEqual(404, response.status_int)

    def test_complete_rsync_old_file_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = False
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1'])
            expected_calls = [call('/data/db.db'), call('/drive/tmp/arg1')]
            self.assertEqual(expected_calls,
                             mock_os.path.exists.call_args_list)
            self.assertEqual('404 Not Found', response.status)
            self.assertEqual(404, response.status_int)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.return_value = False
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1', 'arg2'])
            expected_calls = [call('/data/arg2'), call('/drive/tmp/arg1')]
            self.assertEqual(expected_calls,
                             mock_os.path.exists.call_args_list)
            self.assertEqual('404 Not Found', response.status)
            self.assertEqual(404, response.status_int)

    def test_complete_rsync_rename(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)

        def mock_renamer(old, new):
            renamer_calls.append((old, new))

        self._patch(patch.object, db_replicator, 'renamer', mock_renamer)

        renamer_calls = []
        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.side_effect = [False, True]
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1'])
        self.assertEqual('204 No Content', response.status)
        self.assertEqual(204, response.status_int)
        self.assertEqual(('/drive/tmp/arg1', '/data/db.db'), renamer_calls[0])
        self.assertFalse(renamer_calls[1:])

        renamer_calls = []
        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os, \
                unit.mock_check_drive(isdir=True):
            mock_os.path.exists.side_effect = [False, True]
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1', 'arg2'])
        self.assertEqual('204 No Content', response.status)
        self.assertEqual(204, response.status_int)
        self.assertEqual(('/drive/tmp/arg1', '/data/arg2'), renamer_calls[0])
        self.assertFalse(renamer_calls[1:])

    def test_replicator_sync_with_broker_replication_missing_table(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        rpc.logger = unit.debug_logger()
        broker = FakeBroker()
        broker.get_repl_missing_table = True

        called = []

        def mock_quarantine_db(object_file, server_type):
            called.append(True)
            self.assertEqual(broker.db_file, object_file)
            self.assertEqual(broker.db_type, server_type)

        self._patch(patch.object, db_replicator, 'quarantine_db',
                    mock_quarantine_db)

        with unit.mock_check_drive(isdir=True):
            response = rpc.sync(broker, ('remote_sync', 'hash_', 'id_',
                                         'created_at', 'put_timestamp',
                                         'delete_timestamp', 'metadata'))

        self.assertEqual('404 Not Found', response.status)
        self.assertEqual(404, response.status_int)
        self.assertEqual(called, [True])
        errors = rpc.logger.get_lines_for_level('error')
        self.assertEqual(errors,
                         ["Unable to decode remote metadata 'metadata'",
                          "Quarantining DB %s" % broker])

    def test_replicator_sync(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        broker = FakeBroker()

        with unit.mock_check_drive(isdir=True):
            response = rpc.sync(broker, (
                broker.get_sync() + 1, 12345, 'id_',
                'created_at', 'put_timestamp', 'delete_timestamp',
                '{"meta1": "data1", "meta2": "data2"}'))

        self.assertEqual({'meta1': 'data1', 'meta2': 'data2'},
                         broker.metadata)
        self.assertEqual('created_at', broker.created_at)
        self.assertEqual('put_timestamp', broker.put_timestamp)
        self.assertEqual('delete_timestamp', broker.delete_timestamp)

        self.assertEqual('200 OK', response.status)
        self.assertEqual(200, response.status_int)

    def test_rsync_then_merge(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        with unit.mock_check_drive(isdir=True):
            rpc.rsync_then_merge('sda1', '/srv/swift/blah', ('a', 'b'))

    def test_merge_items(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        fake_broker = FakeBroker()
        args = ('a', 'b')
        with unit.mock_check_drive(isdir=True):
            rpc.merge_items(fake_broker, args)
        self.assertEqual(fake_broker.args, args)

    def test_merge_syncs(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        fake_broker = FakeBroker()
        args = ('a', 'b')
        with unit.mock_check_drive(isdir=True):
            rpc.merge_syncs(fake_broker, args)
        self.assertEqual(fake_broker.args, (args[0],))

    def test_complete_rsync_with_bad_input(self):
        drive = '/some/root'
        db_file = __file__
        args = ['old_file']
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        with unit.mock_check_drive(isdir=True):
            resp = rpc.complete_rsync(drive, db_file, args)
        self.assertTrue(isinstance(resp, HTTPException))
        self.assertEqual(404, resp.status_int)
        with unit.mock_check_drive(isdir=True):
            resp = rpc.complete_rsync(drive, 'new_db_file', args)
        self.assertTrue(isinstance(resp, HTTPException))
        self.assertEqual(404, resp.status_int)

    def test_complete_rsync(self):
        drive = mkdtemp()
        args = ['old_file']
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker,
                                          mount_check=False)
        os.mkdir('%s/tmp' % drive)
        old_file = '%s/tmp/old_file' % drive
        new_file = '%s/new_db_file' % drive
        try:
            fp = open(old_file, 'w')
            fp.write('void')
            fp.close
            resp = rpc.complete_rsync(drive, new_file, args)
            self.assertEqual(204, resp.status_int)
        finally:
            rmtree(drive)

    @unit.with_tempdir
    def test_empty_suffix_and_hash_dirs_get_cleanedup(self, tempdir):
        datadir = os.path.join(tempdir, 'containers')
        db_path = ('450/afd/7089ab48d955ab0851fc51cc17a34afd/'
                   '7089ab48d955ab0851fc51cc17a34afd.db')
        random_file = ('1060/xyz/1234ab48d955ab0851fc51cc17a34xyz/'
                       '1234ab48d955ab0851fc51cc17a34xyz.abc')

        # trailing "/" indicates empty dir
        paths = [
            # empty part dir
            '240/',
            # empty suffix dir
            '18/aba/',
            # empty hashdir
            '1054/27e/d41d8cd98f00b204e9800998ecf8427e/',
            # database
            db_path,
            # non database file
            random_file,
        ]
        for path in paths:
            path = os.path.join(datadir, path)
            os.makedirs(os.path.dirname(path))
            if os.path.basename(path):
                # our setup requires "directories" to end in "/" (i.e. basename
                # is ''); otherwise, create an empty file
                open(path, 'w')
        # sanity
        self.assertEqual({'240', '18', '1054', '1060', '450'},
                         set(os.listdir(datadir)))
        for path in paths:
            dirpath = os.path.join(datadir, os.path.dirname(path))
            self.assertTrue(os.path.isdir(dirpath))

        node_id = 1
        results = list(db_replicator.roundrobin_datadirs(
            [(datadir, node_id, lambda p: True)]))
        expected = [
            ('450', os.path.join(datadir, db_path), node_id),
        ]
        self.assertEqual(results, expected)

        # all the empty leaf dirs are cleaned up
        for path in paths:
            if os.path.basename(path):
                check = self.assertTrue
            else:
                check = self.assertFalse
            dirpath = os.path.join(datadir, os.path.dirname(path))
            isdir = os.path.isdir(dirpath)
            check(isdir, '%r is%s a directory!' % (
                dirpath, '' if isdir else ' not'))

        # despite the leaves cleaned up it takes a few loops to finish it off
        self.assertEqual({'18', '1054', '1060', '450'},
                         set(os.listdir(datadir)))

        results = list(db_replicator.roundrobin_datadirs(
            [(datadir, node_id, lambda p: True)]))
        self.assertEqual(results, expected)
        self.assertEqual({'1054', '1060', '450'},
                         set(os.listdir(datadir)))

        results = list(db_replicator.roundrobin_datadirs(
            [(datadir, node_id, lambda p: True)]))
        self.assertEqual(results, expected)
        # non db file in '1060' dir is not deleted and exception is handled
        self.assertEqual({'1060', '450'},
                         set(os.listdir(datadir)))

    def test_roundrobin_datadirs(self):
        listdir_calls = []
        isdir_calls = []
        exists_calls = []
        shuffle_calls = []
        rmdir_calls = []

        def _listdir(path):
            listdir_calls.append(path)
            if not path.startswith('/srv/node/sda/containers') and \
                    not path.startswith('/srv/node/sdb/containers'):
                return []
            path = path[len('/srv/node/sdx/containers'):]
            if path == '':
                return ['123', '456', '789', '9999', "-5", "not-a-partition"]
                # 456 will pretend to be a file
                # 9999 will be an empty partition with no contents
                # -5 and not-a-partition were created by something outside
                #   Swift
            elif path == '/123':
                return ['abc', 'def.db']  # def.db will pretend to be a file
            elif path == '/123/abc':
                # 11111111111111111111111111111abc will pretend to be a file
                return ['00000000000000000000000000000abc',
                        '11111111111111111111111111111abc']
            elif path == '/123/abc/00000000000000000000000000000abc':
                return ['00000000000000000000000000000abc.db',
                        # This other.db isn't in the right place, so should be
                        # ignored later.
                        '000000000000000000000000000other.db',
                        'weird1']  # weird1 will pretend to be a dir, if asked
            elif path == '/789':
                return ['ghi', 'jkl']  # jkl will pretend to be a file
            elif path == '/789/ghi':
                # 33333333333333333333333333333ghi will pretend to be a file
                return ['22222222222222222222222222222ghi',
                        '33333333333333333333333333333ghi']
            elif path == '/789/ghi/22222222222222222222222222222ghi':
                return ['22222222222222222222222222222ghi.db',
                        'weird2']  # weird2 will pretend to be a dir, if asked
            elif path == '9999':
                return []
            elif path == 'not-a-partition':
                raise Exception("shouldn't look in not-a-partition")
            elif path == '-5':
                raise Exception("shouldn't look in -5")
            return []

        def _isdir(path):
            isdir_calls.append(path)
            if not path.startswith('/srv/node/sda/containers') and \
                    not path.startswith('/srv/node/sdb/containers'):
                return False
            path = path[len('/srv/node/sdx/containers'):]
            if path in ('/123', '/123/abc',
                        '/123/abc/00000000000000000000000000000abc',
                        '/123/abc/00000000000000000000000000000abc/weird1',
                        '/789', '/789/ghi',
                        '/789/ghi/22222222222222222222222222222ghi',
                        '/789/ghi/22222222222222222222222222222ghi/weird2',
                        '/9999'):
                return True
            return False

        def _exists(arg):
            exists_calls.append(arg)
            return True

        def _shuffle(arg):
            shuffle_calls.append(arg)

        def _rmdir(arg):
            rmdir_calls.append(arg)

        base = 'swift.common.db_replicator.'
        with mock.patch(base + 'os.listdir', _listdir), \
                mock.patch(base + 'os.path.isdir', _isdir), \
                mock.patch(base + 'os.path.exists', _exists), \
                mock.patch(base + 'random.shuffle', _shuffle), \
                mock.patch(base + 'os.rmdir', _rmdir):

            datadirs = [('/srv/node/sda/containers', 1, lambda p: True),
                        ('/srv/node/sdb/containers', 2, lambda p: True)]
            results = list(db_replicator.roundrobin_datadirs(datadirs))
            # The results show that the .db files are returned, the devices
            # interleaved.
            self.assertEqual(results, [
                ('123', '/srv/node/sda/containers/123/abc/'
                        '00000000000000000000000000000abc/'
                        '00000000000000000000000000000abc.db', 1),
                ('123', '/srv/node/sdb/containers/123/abc/'
                        '00000000000000000000000000000abc/'
                        '00000000000000000000000000000abc.db', 2),
                ('789', '/srv/node/sda/containers/789/ghi/'
                        '22222222222222222222222222222ghi/'
                        '22222222222222222222222222222ghi.db', 1),
                ('789', '/srv/node/sdb/containers/789/ghi/'
                        '22222222222222222222222222222ghi/'
                        '22222222222222222222222222222ghi.db', 2)])
            # The listdir calls show that we only listdir the dirs
            self.assertEqual(listdir_calls, [
                '/srv/node/sda/containers',
                '/srv/node/sda/containers/123',
                '/srv/node/sda/containers/123/abc',
                '/srv/node/sdb/containers',
                '/srv/node/sdb/containers/123',
                '/srv/node/sdb/containers/123/abc',
                '/srv/node/sda/containers/789',
                '/srv/node/sda/containers/789/ghi',
                '/srv/node/sdb/containers/789',
                '/srv/node/sdb/containers/789/ghi',
                '/srv/node/sda/containers/9999',
                '/srv/node/sdb/containers/9999'])
            # The isdir calls show that we did ask about the things pretending
            # to be files at various levels.
            self.assertEqual(isdir_calls, [
                '/srv/node/sda/containers/123',
                '/srv/node/sda/containers/123/abc',
                ('/srv/node/sda/containers/123/abc/'
                 '00000000000000000000000000000abc'),
                '/srv/node/sdb/containers/123',
                '/srv/node/sdb/containers/123/abc',
                ('/srv/node/sdb/containers/123/abc/'
                 '00000000000000000000000000000abc'),
                ('/srv/node/sda/containers/123/abc/'
                 '11111111111111111111111111111abc'),
                '/srv/node/sda/containers/123/def.db',
                '/srv/node/sda/containers/456',
                '/srv/node/sda/containers/789',
                '/srv/node/sda/containers/789/ghi',
                ('/srv/node/sda/containers/789/ghi/'
                 '22222222222222222222222222222ghi'),
                ('/srv/node/sdb/containers/123/abc/'
                 '11111111111111111111111111111abc'),
                '/srv/node/sdb/containers/123/def.db',
                '/srv/node/sdb/containers/456',
                '/srv/node/sdb/containers/789',
                '/srv/node/sdb/containers/789/ghi',
                ('/srv/node/sdb/containers/789/ghi/'
                 '22222222222222222222222222222ghi'),
                ('/srv/node/sda/containers/789/ghi/'
                 '33333333333333333333333333333ghi'),
                '/srv/node/sda/containers/789/jkl',
                '/srv/node/sda/containers/9999',
                ('/srv/node/sdb/containers/789/ghi/'
                 '33333333333333333333333333333ghi'),
                '/srv/node/sdb/containers/789/jkl',
                '/srv/node/sdb/containers/9999'])
            # The exists calls are the .db files we looked for as we walked the
            # structure.
            self.assertEqual(exists_calls, [
                ('/srv/node/sda/containers/123/abc/'
                 '00000000000000000000000000000abc/'
                 '00000000000000000000000000000abc.db'),
                ('/srv/node/sdb/containers/123/abc/'
                 '00000000000000000000000000000abc/'
                 '00000000000000000000000000000abc.db'),
                ('/srv/node/sda/containers/789/ghi/'
                 '22222222222222222222222222222ghi/'
                 '22222222222222222222222222222ghi.db'),
                ('/srv/node/sdb/containers/789/ghi/'
                 '22222222222222222222222222222ghi/'
                 '22222222222222222222222222222ghi.db')])
            # Shows that we called shuffle twice, once for each device.
            self.assertEqual(
                shuffle_calls, [['123', '456', '789', '9999'],
                                ['123', '456', '789', '9999']])

            # Shows that we called removed the two empty partition directories.
            self.assertEqual(
                rmdir_calls, ['/srv/node/sda/containers/9999',
                              '/srv/node/sdb/containers/9999'])

    @mock.patch("swift.common.db_replicator.ReplConnection", mock.Mock())
    def test_http_connect(self):
        node = "node"
        partition = "partition"
        db_file = __file__
        replicator = TestReplicator({})
        replicator._http_connect(node, partition, db_file)
        expected_hsh = os.path.basename(db_file).split('.', 1)[0]
        expected_hsh = expected_hsh.split('_', 1)[0]
        db_replicator.ReplConnection.assert_has_calls([
            mock.call(node, partition, expected_hsh, replicator.logger)])


class TestHandoffsOnly(unittest.TestCase):
    class FakeRing3Nodes(object):
        _replicas = 3

        # Three nodes, two disks each
        devs = [
            dict(id=0, region=1, zone=1,
                 meta='', weight=500.0, ip='10.0.0.1', port=6201,
                 replication_ip='10.0.0.1', replication_port=6201,
                 device='sdp'),
            dict(id=1, region=1, zone=1,
                 meta='', weight=500.0, ip='10.0.0.1', port=6201,
                 replication_ip='10.0.0.1', replication_port=6201,
                 device='sdq'),

            dict(id=2, region=1, zone=1,
                 meta='', weight=500.0, ip='10.0.0.2', port=6201,
                 replication_ip='10.0.0.2', replication_port=6201,
                 device='sdp'),
            dict(id=3, region=1, zone=1,
                 meta='', weight=500.0, ip='10.0.0.2', port=6201,
                 replication_ip='10.0.0.2', replication_port=6201,
                 device='sdq'),

            dict(id=4, region=1, zone=1,
                 meta='', weight=500.0, ip='10.0.0.3', port=6201,
                 replication_ip='10.0.0.3', replication_port=6201,
                 device='sdp'),
            dict(id=5, region=1, zone=1,
                 meta='', weight=500.0, ip='10.0.0.3', port=6201,
                 replication_ip='10.0.0.3', replication_port=6201,
                 device='sdq'),
        ]

        def __init__(self, *a, **kw):
            pass

        def get_part(self, account, container=None, obj=None):
            return 0

        def get_part_nodes(self, part):
            nodes = []
            for offset in range(self._replicas):
                i = (part + offset) % len(self.devs)
                nodes.append(self.devs[i])
            return nodes

        def get_more_nodes(self, part):
            for offset in range(self._replicas, len(self.devs)):
                i = (part + offset) % len(self.devs)
                yield self.devs[i]

    def _make_fake_db(self, disk, partition, db_hash):
        directories = [
            os.path.join(self.root, disk),
            os.path.join(self.root, disk, 'containers'),
            os.path.join(self.root, disk, 'containers', str(partition)),
            os.path.join(self.root, disk, 'containers', str(partition),
                         db_hash[-3:]),
            os.path.join(self.root, disk, 'containers', str(partition),
                         db_hash[-3:], db_hash)]

        for d in directories:
            try:
                os.mkdir(d)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise
        file_path = os.path.join(directories[-1], db_hash + ".db")
        with open(file_path, 'w'):
            pass

    def setUp(self):
        self.root = mkdtemp()

        # object disks; they're just here to make sure they don't trip us up
        os.mkdir(os.path.join(self.root, 'sdc'))
        os.mkdir(os.path.join(self.root, 'sdc', 'objects'))
        os.mkdir(os.path.join(self.root, 'sdd'))
        os.mkdir(os.path.join(self.root, 'sdd', 'objects'))

        # part 0 belongs on sdp
        self._make_fake_db('sdp', 0, '010101013cf2b7979af9eaa71cb67220')

        # part 1 does not belong on sdp
        self._make_fake_db('sdp', 1, 'abababab2b5368158355e799323b498d')

        # part 1 belongs on sdq
        self._make_fake_db('sdq', 1, '02020202e30f696a3cfa63d434a3c94e')

        # part 2 does not belong on sdq
        self._make_fake_db('sdq', 2, 'bcbcbcbc15d3835053d568c57e2c83b5')

    def tearDown(self):
        rmtree(self.root, ignore_errors=True)

    def test_scary_warnings(self):
        logger = unit.FakeLogger()
        replicator = TestReplicator({
            'handoffs_only': 'yes',
            'devices': self.root,
            'bind_port': 6201,
            'mount_check': 'no',
        }, logger=logger)

        with patch.object(db_replicator, 'whataremyips',
                          return_value=['10.0.0.1']), \
                patch.object(replicator, '_replicate_object'), \
                patch.object(replicator, 'ring', self.FakeRing3Nodes()):
            replicator.run_once()

        self.assertEqual(
            logger.get_lines_for_level('warning'),
            [('Starting replication pass with handoffs_only enabled. This '
              'mode is not intended for normal operation; use '
              'handoffs_only with care.'),
             ('Finished replication pass with handoffs_only enabled. '
              'If handoffs_only is no longer required, disable it.')])

    def test_skips_primary_partitions(self):
        replicator = TestReplicator({
            'handoffs_only': 'yes',
            'devices': self.root,
            'bind_port': 6201,
            'mount_check': 'no',
        })

        with patch.object(db_replicator, 'whataremyips',
                          return_value=['10.0.0.1']), \
                patch.object(replicator, '_replicate_object') as mock_repl, \
                patch.object(replicator, 'ring', self.FakeRing3Nodes()):
            replicator.run_once()

        self.assertEqual(sorted(mock_repl.mock_calls), [
            mock.call('1', os.path.join(
                self.root, 'sdp', 'containers', '1', '98d',
                'abababab2b5368158355e799323b498d',
                'abababab2b5368158355e799323b498d.db'), 0),
            mock.call('2', os.path.join(
                self.root, 'sdq', 'containers', '2', '3b5',
                'bcbcbcbc15d3835053d568c57e2c83b5',
                'bcbcbcbc15d3835053d568c57e2c83b5.db'), 1)])

    def test_override_partitions(self):
        replicator = TestReplicator({
            'devices': self.root,
            'bind_port': 6201,
            'mount_check': 'no',
        })

        with patch.object(db_replicator, 'whataremyips',
                          return_value=['10.0.0.1']), \
                patch.object(replicator, '_replicate_object') as mock_repl, \
                patch.object(replicator, 'ring', self.FakeRing3Nodes()):
            replicator.run_once(partitions="0,2")

        self.assertEqual(sorted(mock_repl.mock_calls), [
            mock.call('0', os.path.join(
                self.root, 'sdp', 'containers', '0', '220',
                '010101013cf2b7979af9eaa71cb67220',
                '010101013cf2b7979af9eaa71cb67220.db'), 0),
            mock.call('2', os.path.join(
                self.root, 'sdq', 'containers', '2', '3b5',
                'bcbcbcbc15d3835053d568c57e2c83b5',
                'bcbcbcbc15d3835053d568c57e2c83b5.db'), 1)])

    def test_override_devices(self):
        replicator = TestReplicator({
            'devices': self.root,
            'bind_port': 6201,
            'mount_check': 'no',
        })

        with patch.object(db_replicator, 'whataremyips',
                          return_value=['10.0.0.1']), \
                patch.object(replicator, '_replicate_object') as mock_repl, \
                patch.object(replicator, 'ring', self.FakeRing3Nodes()):
            replicator.run_once(devices="sdp")

        self.assertEqual(sorted(mock_repl.mock_calls), [
            mock.call('0', os.path.join(
                self.root, 'sdp', 'containers', '0', '220',
                '010101013cf2b7979af9eaa71cb67220',
                '010101013cf2b7979af9eaa71cb67220.db'), 0),
            mock.call('1', os.path.join(
                self.root, 'sdp', 'containers', '1', '98d',
                'abababab2b5368158355e799323b498d',
                'abababab2b5368158355e799323b498d.db'), 0)])

    def test_override_devices_and_partitions(self):
        replicator = TestReplicator({
            'devices': self.root,
            'bind_port': 6201,
            'mount_check': 'no',
        })

        with patch.object(db_replicator, 'whataremyips',
                          return_value=['10.0.0.1']), \
                patch.object(replicator, '_replicate_object') as mock_repl, \
                patch.object(replicator, 'ring', self.FakeRing3Nodes()):
            replicator.run_once(partitions="0,2", devices="sdp")

        self.assertEqual(sorted(mock_repl.mock_calls), [
            mock.call('0', os.path.join(
                self.root, 'sdp', 'containers', '0', '220',
                '010101013cf2b7979af9eaa71cb67220',
                '010101013cf2b7979af9eaa71cb67220.db'), 0)])


class TestReplToNode(unittest.TestCase):
    def setUp(self):
        db_replicator.ring = FakeRing()
        self.delete_db_calls = []
        self.broker = FakeBroker()
        self.replicator = TestReplicator({'per_diff': 10})
        self.fake_node = {'ip': '127.0.0.1', 'device': 'sda1', 'port': 1000}
        self.fake_info = {'id': 'a', 'point': -1, 'max_row': 20, 'hash': 'b',
                          'created_at': 100, 'put_timestamp': 0,
                          'delete_timestamp': 0, 'count': 0,
                          'metadata': json.dumps({
                              'Test': ('Value', normalize_timestamp(1))})}
        self.replicator.logger = mock.Mock()
        self.replicator._rsync_db = mock.Mock(return_value=True)
        self.replicator._usync_db = mock.Mock(return_value=True)
        self.http = ReplHttp('{"id": 3, "point": -1}')
        self.replicator._http_connect = lambda *args: self.http

    def test_repl_to_node_usync_success(self):
        rinfo = {"id": 3, "point": -1, "max_row": 10, "hash": "c"}
        self.http = ReplHttp(json.dumps(rinfo))
        local_sync = self.broker.get_sync()
        self.assertEqual(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.replicator._usync_db.assert_has_calls([
            mock.call(max(rinfo['point'], local_sync), self.broker,
                      self.http, rinfo['id'], self.fake_info['id'])
        ])

    def test_repl_to_node_rsync_success(self):
        rinfo = {"id": 3, "point": -1, "max_row": 9, "hash": "c"}
        self.http = ReplHttp(json.dumps(rinfo))
        self.broker.get_sync()
        self.assertEqual(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.replicator.logger.increment.assert_has_calls([
            mock.call.increment('remote_merges')
        ])
        self.replicator._rsync_db.assert_has_calls([
            mock.call(self.broker, self.fake_node, self.http,
                      self.fake_info['id'],
                      replicate_method='rsync_then_merge',
                      replicate_timeout=(self.fake_info['count'] / 2000),
                      different_region=False)
        ])

    def test_repl_to_node_already_in_sync(self):
        rinfo = {"id": 3, "point": -1, "max_row": 20, "hash": "b"}
        self.http = ReplHttp(json.dumps(rinfo))
        self.broker.get_sync()
        self.assertEqual(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.assertEqual(self.replicator._rsync_db.call_count, 0)
        self.assertEqual(self.replicator._usync_db.call_count, 0)

    def test_repl_to_node_metadata_update(self):
        now = Timestamp(time.time()).internal
        rmetadata = {"X-Container-Sysmeta-Test": ("XYZ", now)}
        rinfo = {"id": 3, "point": -1, "max_row": 20, "hash": "b",
                 "metadata": json.dumps(rmetadata)}
        self.http = ReplHttp(json.dumps(rinfo))
        self.broker.get_sync()
        self.assertEqual(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        metadata = self.broker.metadata
        self.assertIn("X-Container-Sysmeta-Test", metadata)
        self.assertEqual("XYZ", metadata["X-Container-Sysmeta-Test"][0])
        self.assertEqual(now, metadata["X-Container-Sysmeta-Test"][1])

    def test_repl_to_node_not_found(self):
        self.http = ReplHttp('{"id": 3, "point": -1}', set_status=404)
        self.assertEqual(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info, False), True)
        self.replicator.logger.increment.assert_has_calls([
            mock.call.increment('rsyncs')
        ])
        self.replicator._rsync_db.assert_has_calls([
            mock.call(self.broker, self.fake_node, self.http,
                      self.fake_info['id'], different_region=False)
        ])

    def test_repl_to_node_drive_not_mounted(self):
        self.http = ReplHttp('{"id": 3, "point": -1}', set_status=507)

        self.assertRaises(DriveNotMounted, self.replicator._repl_to_node,
                          self.fake_node, FakeBroker(), '0', self.fake_info)

    def test_repl_to_node_300_status(self):
        self.http = ReplHttp('{"id": 3, "point": -1}', set_status=300)

        self.assertFalse(self.replicator._repl_to_node(
            self.fake_node, FakeBroker(), '0', self.fake_info))

    def test_repl_to_node_not_response(self):
        self.http = mock.Mock(replicate=mock.Mock(return_value=None))
        self.assertEqual(self.replicator._repl_to_node(
            self.fake_node, FakeBroker(), '0', self.fake_info), False)

    def test_repl_to_node_small_container_always_usync(self):
        # Tests that a small container that is > 50% out of sync will
        # still use usync.
        rinfo = {"id": 3, "point": -1, "hash": "c"}

        # Turn per_diff back to swift's default.
        self.replicator.per_diff = 1000
        for r, l in ((5, 20), (40, 100), (450, 1000), (550, 1500)):
            rinfo['max_row'] = r
            self.fake_info['max_row'] = l
            self.replicator._usync_db = mock.Mock(return_value=True)
            self.http = ReplHttp(json.dumps(rinfo))
            local_sync = self.broker.get_sync()
            self.assertEqual(self.replicator._repl_to_node(
                self.fake_node, self.broker, '0', self.fake_info), True)
            self.replicator._usync_db.assert_has_calls([
                mock.call(max(rinfo['point'], local_sync), self.broker,
                          self.http, rinfo['id'], self.fake_info['id'])
            ])


class ExampleReplicator(db_replicator.Replicator):
    server_type = 'fake'
    brokerclass = ExampleBroker
    datadir = 'fake'
    default_port = 1000


class TestReplicatorSync(unittest.TestCase):

    # override in subclass
    backend = ExampleReplicator.brokerclass
    datadir = ExampleReplicator.datadir
    replicator_daemon = ExampleReplicator
    replicator_rpc = db_replicator.ReplicatorRpc

    def setUp(self):
        self.root = mkdtemp()
        self.rpc = self.replicator_rpc(
            self.root, self.datadir, self.backend, mount_check=False,
            logger=unit.debug_logger())
        FakeReplConnection = attach_fake_replication_rpc(self.rpc)
        self._orig_ReplConnection = db_replicator.ReplConnection
        db_replicator.ReplConnection = FakeReplConnection
        self._orig_Ring = db_replicator.ring.Ring
        self._ring = unit.FakeRing()
        db_replicator.ring.Ring = lambda *args, **kwargs: self._get_ring()
        self.logger = unit.debug_logger()

    def tearDown(self):
        db_replicator.ReplConnection = self._orig_ReplConnection
        db_replicator.ring.Ring = self._orig_Ring
        rmtree(self.root)

    def _get_ring(self):
        return self._ring

    def _get_broker(self, account, container=None, node_index=0):
        hash_ = hash_path(account, container)
        part, nodes = self._ring.get_nodes(account, container)
        drive = nodes[node_index]['device']
        db_path = os.path.join(self.root, drive,
                               storage_directory(self.datadir, part, hash_),
                               hash_ + '.db')
        return self.backend(db_path, account=account, container=container)

    def _get_broker_part_node(self, broker):
        part, nodes = self._ring.get_nodes(broker.account, broker.container)
        storage_dir = broker.db_file[len(self.root):].lstrip(os.path.sep)
        broker_device = storage_dir.split(os.path.sep, 1)[0]
        for node in nodes:
            if node['device'] == broker_device:
                return part, node

    def _get_daemon(self, node, conf_updates):
        conf = {
            'devices': self.root,
            'recon_cache_path': self.root,
            'mount_check': 'false',
            'bind_port': node['replication_port'],
        }
        if conf_updates:
            conf.update(conf_updates)
        return self.replicator_daemon(conf, logger=self.logger)

    def _install_fake_rsync_file(self, daemon, captured_calls=None):
        def _rsync_file(db_file, remote_file, **kwargs):
            if captured_calls is not None:
                captured_calls.append((db_file, remote_file, kwargs))
            remote_server, remote_path = remote_file.split('/', 1)
            dest_path = os.path.join(self.root, remote_path)
            copy(db_file, dest_path)
            return True
        daemon._rsync_file = _rsync_file

    def _run_once(self, node, conf_updates=None, daemon=None):
        daemon = daemon or self._get_daemon(node, conf_updates)
        self._install_fake_rsync_file(daemon)
        with mock.patch('swift.common.db_replicator.whataremyips',
                        new=lambda *a, **kw: [node['replication_ip']]), \
                unit.mock_check_drive(isdir=not daemon.mount_check,
                                      ismount=daemon.mount_check):
            daemon.run_once()
        return daemon

    def test_local_ids(self):
        for drive in ('sda', 'sdb', 'sdd'):
            os.makedirs(os.path.join(self.root, drive, self.datadir))
        for node in self._ring.devs:
            daemon = self._run_once(node)
            if node['device'] == 'sdc':
                self.assertEqual(daemon._local_device_ids, set())
            else:
                self.assertEqual(daemon._local_device_ids,
                                 set([node['id']]))

    def test_clean_up_after_deleted_brokers(self):
        broker = self._get_broker('a', 'c', node_index=0)
        part, node = self._get_broker_part_node(broker)
        part = str(part)
        daemon = self._run_once(node)
        # create a super old broker and delete it!
        forever_ago = time.time() - daemon.reclaim_age
        put_timestamp = normalize_timestamp(forever_ago - 2)
        delete_timestamp = normalize_timestamp(forever_ago - 1)
        broker.initialize(put_timestamp)
        broker.delete_db(delete_timestamp)
        # if we have a container broker make sure it's reported
        if hasattr(broker, 'reported'):
            info = broker.get_info()
            broker.reported(info['put_timestamp'],
                            info['delete_timestamp'],
                            info['object_count'],
                            info['bytes_used'])
        info = broker.get_replication_info()
        self.assertTrue(daemon.report_up_to_date(info))
        # we have a part dir
        part_root = os.path.join(self.root, node['device'], self.datadir)
        parts = os.listdir(part_root)
        self.assertEqual([part], parts)
        # with a single suffix
        suff = os.listdir(os.path.join(part_root, part))
        self.assertEqual(1, len(suff))
        # running replicator will remove the deleted db
        daemon = self._run_once(node, daemon=daemon)
        self.assertEqual(1, daemon.stats['remove'])
        # we still have a part dir (but it's empty)
        suff = os.listdir(os.path.join(part_root, part))
        self.assertEqual(0, len(suff))
        # run it again and there's nothing to do...
        daemon = self._run_once(node, daemon=daemon)
        self.assertEqual(0, daemon.stats['attempted'])
        # but empty part dir is cleaned up!
        parts = os.listdir(part_root)
        self.assertEqual(0, len(parts))

    def test_rsync_then_merge(self):
        # setup current db (and broker)
        broker = self._get_broker('a', 'c', node_index=0)
        part, node = self._get_broker_part_node(broker)
        part = str(part)
        put_timestamp = normalize_timestamp(time.time())
        broker.initialize(put_timestamp)
        put_metadata = {'example-meta': ['bah', put_timestamp]}
        broker.update_metadata(put_metadata)

        # sanity (re-open, and the db keeps the metadata)
        broker = self._get_broker('a', 'c', node_index=0)
        self.assertEqual(put_metadata, broker.metadata)

        # create rsynced db in tmp dir
        obj_hash = hash_path('a', 'c')
        rsynced_db_broker = self.backend(
            os.path.join(self.root, node['device'], 'tmp', obj_hash + '.db'),
            account='a', container='b')
        rsynced_db_broker.initialize(put_timestamp)

        # do rysnc_then_merge
        rpc = db_replicator.ReplicatorRpc(
            self.root, self.datadir, self.backend, False)
        response = rpc.dispatch((node['device'], part, obj_hash),
                                ['rsync_then_merge', obj_hash + '.db', 'arg2'])
        # sanity
        self.assertEqual('204 No Content', response.status)
        self.assertEqual(204, response.status_int)

        # re-open the db
        broker = self._get_broker('a', 'c', node_index=0)
        # keep the metadata in existing db
        self.assertEqual(put_metadata, broker.metadata)

    def test_replicator_sync(self):
        # setup current db (and broker)
        broker = self._get_broker('a', 'c', node_index=0)
        part, node = self._get_broker_part_node(broker)
        part = str(part)
        put_timestamp = normalize_timestamp(time.time())
        broker.initialize(put_timestamp)
        put_metadata = {'example-meta': ['bah', put_timestamp]}
        sync_local_metadata = {
            "meta1": ["data1", put_timestamp],
            "meta2": ["data2", put_timestamp]}
        broker.update_metadata(put_metadata)

        # sanity (re-open, and the db keeps the metadata)
        broker = self._get_broker('a', 'c', node_index=0)
        self.assertEqual(put_metadata, broker.metadata)

        # do rysnc_then_merge
        rpc = db_replicator.ReplicatorRpc(
            self.root, self.datadir, ExampleBroker, False)
        response = rpc.sync(
            broker, (broker.get_sync('id_') + 1, 12345, 'id_',
                     put_timestamp, put_timestamp, '0',
                     json.dumps(sync_local_metadata)))
        # sanity
        self.assertEqual('200 OK', response.status)
        self.assertEqual(200, response.status_int)

        # re-open the db
        broker = self._get_broker('a', 'c', node_index=0)
        # keep the both metadata in existing db and local db
        expected = put_metadata.copy()
        expected.update(sync_local_metadata)
        self.assertEqual(expected, broker.metadata)


if __name__ == '__main__':
    unittest.main()
