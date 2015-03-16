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

import unittest
from contextlib import contextmanager
import os
import logging
import errno
import math
import time
from mock import patch, call
from shutil import rmtree, copy
from tempfile import mkdtemp, NamedTemporaryFile
import mock
import simplejson

from swift.container.backend import DATADIR
from swift.common import db_replicator
from swift.common.utils import (normalize_timestamp, hash_path,
                                storage_directory)
from swift.common.exceptions import DriveNotMounted
from swift.common.swob import HTTPException

from test import unit
from test.unit.common.test_db import ExampleBroker


TEST_ACCOUNT_NAME = 'a c t'
TEST_CONTAINER_NAME = 'c o n'


def teardown_module():
    "clean up my monkey patching"
    reload(db_replicator)


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
            id=1, weight=10.0, zone=1, ip='1.1.1.1', port=6000, device='sdb',
            meta='', replication_ip='1.1.1.1', replication_port=6000
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
            id=1, weight=10.0, zone=1, ip='1.1.1.1', port=6000, device='sdb',
            meta=''
        ), dict(
            id=2, weight=10.0, zone=2, ip='1.1.1.2', port=6000, device='sdb',
            meta=''
        ), dict(
            id=3, weight=10.0, zone=3, ip='1.1.1.3', port=6000, device='sdb',
            meta=''
        ), dict(
            id=4, weight=10.0, zone=4, ip='1.1.1.4', port=6000, device='sdb',
            meta=''
        ), dict(
            id=5, weight=10.0, zone=5, ip='1.1.1.5', port=6000, device='sdb',
            meta=''
        ), dict(
            id=6, weight=10.0, zone=6, ip='1.1.1.6', port=6000, device='sdb',
            meta='')]

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
                next = self.codes.next()
                if isinstance(next, int):
                    innerself.returncode = next
                    return next
                raise next
        return Failure()


@contextmanager
def _mock_process(*args):
    orig_process = db_replicator.subprocess.Popen
    db_replicator.subprocess.Popen = FakeProcess(*args)
    yield db_replicator.subprocess.Popen
    db_replicator.subprocess.Popen = orig_process


class ReplHttp(object):
    def __init__(self, response=None, set_status=200):
        self.response = response
        self.set_status = set_status
    replicated = False
    host = 'localhost'

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
        })
        if self.stub_replication_info:
            info.update(self.stub_replication_info)
        return info

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

    def tearDown(self):
        for patcher in self._patchers:
            patcher.stop()

    def _patch(self, patching_fn, *args, **kwargs):
        patcher = patching_fn(*args, **kwargs)
        patched_thing = patcher.start()
        self._patchers.append(patcher)
        return patched_thing

    def stub_delete_db(self, broker):
        self.delete_db_calls.append('/path/to/file')

    def test_repl_connection(self):
        node = {'replication_ip': '127.0.0.1', 'replication_port': 80,
                'device': 'sdb1'}
        conn = db_replicator.ReplConnection(node, '1234567890', 'abcdefg',
                                            logging.getLogger())

        def req(method, path, body, headers):
            self.assertEquals(method, 'REPLICATE')
            self.assertEquals(headers['Content-Type'], 'application/json')

        class Resp(object):
            def read(self):
                return 'data'
        resp = Resp()
        conn.request = req
        conn.getresponse = lambda *args: resp
        self.assertEquals(conn.replicate(1, 2, 3), resp)

        def other_req(method, path, body, headers):
            raise Exception('blah')
        conn.request = other_req
        self.assertEquals(conn.replicate(1, 2, 3), None)

    def test_rsync_file(self):
        replicator = TestReplicator({})
        with _mock_process(-1):
            self.assertEquals(
                False,
                replicator._rsync_file('/some/file', 'remote:/some/file'))
        with _mock_process(0):
            self.assertEquals(
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

    def test_rsync_db(self):
        replicator = TestReplicator({})
        replicator._rsync_file = lambda *args: True
        fake_device = {'replication_ip': '127.0.0.1', 'device': 'sda1'}
        replicator._rsync_db(FakeBroker(), fake_device, ReplHttp(), 'abcd')

    def test_rsync_db_rsync_file_call(self):
        fake_device = {'ip': '127.0.0.1', 'port': '0',
                       'replication_ip': '127.0.0.1', 'replication_port': '0',
                       'device': 'sda1'}

        def mock_rsync_ip(ip):
            self.assertEquals(fake_device['ip'], ip)
            return 'rsync_ip(%s)' % ip

        class MyTestReplicator(TestReplicator):
            def __init__(self, db_file, remote_file):
                super(MyTestReplicator, self).__init__({})
                self.db_file = db_file
                self.remote_file = remote_file

            def _rsync_file(self_, db_file, remote_file, whole_file=True):
                self.assertEqual(self_.db_file, db_file)
                self.assertEqual(self_.remote_file, remote_file)
                self_._rsync_file_called = True
                return False

        with patch('swift.common.db_replicator.rsync_ip', mock_rsync_ip):
            broker = FakeBroker()
            remote_file = 'rsync_ip(127.0.0.1)::container/sda1/tmp/abcd'
            replicator = MyTestReplicator(broker.db_file, remote_file)
            replicator._rsync_db(broker, fake_device, ReplHttp(), 'abcd')
            self.assert_(replicator._rsync_file_called)

        with patch('swift.common.db_replicator.rsync_ip', mock_rsync_ip):
            broker = FakeBroker()
            remote_file = 'rsync_ip(127.0.0.1)::container0/sda1/tmp/abcd'
            replicator = MyTestReplicator(broker.db_file, remote_file)
            replicator.vm_test_mode = True
            replicator._rsync_db(broker, fake_device, ReplHttp(), 'abcd')
            self.assert_(replicator._rsync_file_called)

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

            def _rsync_file(self_, db_file, remote_file, whole_file=True):
                self_._rsync_file_call_count += 1
                if self_._rsync_file_call_count == 1:
                    self.assertEquals(True, whole_file)
                    self.assertEquals(False, self_.broker.locked)
                elif self_._rsync_file_call_count == 2:
                    self.assertEquals(False, whole_file)
                    self.assertEquals(True, self_.broker.locked)
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
            self.assertEquals(2, replicator._rsync_file_call_count)

        # with new mtime
        with patch('os.path.exists', lambda *args: False):
            with patch('os.path.getmtime', ChangingMtimesOs()):
                broker = FakeBroker()
                replicator = MyTestReplicator(broker)
                fake_device = {'ip': '127.0.0.1',
                               'replication_ip': '127.0.0.1',
                               'device': 'sda1'}
                replicator._rsync_db(broker, fake_device, ReplHttp(), 'abcd')
                self.assertEquals(2, replicator._rsync_file_call_count)

    def test_in_sync(self):
        replicator = TestReplicator({})
        self.assertEquals(replicator._in_sync(
            {'id': 'a', 'point': 0, 'max_row': 0, 'hash': 'b'},
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'b'},
            FakeBroker(), -1), True)
        self.assertEquals(replicator._in_sync(
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'b'},
            {'id': 'a', 'point': -1, 'max_row': 10, 'hash': 'b'},
            FakeBroker(), -1), True)
        self.assertEquals(bool(replicator._in_sync(
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'c'},
            {'id': 'a', 'point': -1, 'max_row': 10, 'hash': 'd'},
            FakeBroker(), -1)), False)

    def test_run_once(self):
        replicator = TestReplicator({})
        replicator.run_once()

    def test_run_once_no_ips(self):
        replicator = TestReplicator({}, logger=unit.FakeLogger())
        self._patch(patch.object, db_replicator, 'whataremyips',
                    lambda *args: [])

        replicator.run_once()

        self.assertEqual(
            replicator.logger.log_dict['error'],
            [(('ERROR Failed to get my own IPs?',), {})])

    def test_run_once_node_is_not_mounted(self):
        db_replicator.ring = FakeRingWithSingleNode()
        conf = {'mount_check': 'true', 'bind_port': 6000}
        replicator = TestReplicator(conf, logger=unit.FakeLogger())
        self.assertEqual(replicator.mount_check, True)
        self.assertEqual(replicator.port, 6000)

        def mock_ismount(path):
            self.assertEquals(path,
                              os.path.join(replicator.root,
                                           replicator.ring.devs[0]['device']))
            return False

        self._patch(patch.object, db_replicator, 'whataremyips',
                    lambda *args: ['1.1.1.1'])
        self._patch(patch.object, db_replicator, 'ismount', mock_ismount)
        replicator.run_once()

        self.assertEqual(
            replicator.logger.log_dict['warning'],
            [(('Skipping %(device)s as it is not mounted' %
               replicator.ring.devs[0],), {})])

    def test_run_once_node_is_mounted(self):
        db_replicator.ring = FakeRingWithSingleNode()
        conf = {'mount_check': 'true', 'bind_port': 6000}
        replicator = TestReplicator(conf, logger=unit.FakeLogger())
        self.assertEqual(replicator.mount_check, True)
        self.assertEqual(replicator.port, 6000)

        def mock_unlink_older_than(path, mtime):
            self.assertEquals(path,
                              os.path.join(replicator.root,
                                           replicator.ring.devs[0]['device'],
                                           'tmp'))
            self.assertTrue(time.time() - replicator.reclaim_age >= mtime)

        def mock_spawn_n(fn, part, object_file, node_id):
            self.assertEquals('123', part)
            self.assertEquals('/srv/node/sda/c.db', object_file)
            self.assertEquals(1, node_id)

        self._patch(patch.object, db_replicator, 'whataremyips',
                    lambda *args: ['1.1.1.1'])
        self._patch(patch.object, db_replicator, 'ismount', lambda *args: True)
        self._patch(patch.object, db_replicator, 'unlink_older_than',
                    mock_unlink_older_than)
        self._patch(patch.object, db_replicator, 'roundrobin_datadirs',
                    lambda *args: [('123', '/srv/node/sda/c.db', 1)])
        self._patch(patch.object, replicator.cpool, 'spawn_n', mock_spawn_n)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.isdir.return_value = True
            replicator.run_once()
            mock_os.path.isdir.assert_called_with(
                os.path.join(replicator.root,
                             replicator.ring.devs[0]['device'],
                             replicator.datadir))

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

    def test_stats(self):
        # I'm not sure how to test that this logs the right thing,
        # but we can at least make sure it gets covered.
        replicator = TestReplicator({})
        replicator._zero_stats()
        replicator._report_stats()

    def test_replicate_object(self):
        db_replicator.ring = FakeRingWithNodes()
        replicator = TestReplicator({})
        replicator.delete_db = self.stub_delete_db
        replicator._replicate_object('0', '/path/to/file', 'node_id')
        self.assertEquals([], self.delete_db_calls)

    def test_replicate_object_quarantine(self):
        replicator = TestReplicator({})
        self._patch(patch.object, replicator.brokerclass, 'db_file',
                    '/a/b/c/d/e/hey')
        self._patch(patch.object, replicator.brokerclass,
                    'get_repl_missing_table', True)

        def mock_renamer(was, new, fsync=False, cause_colision=False):
            if cause_colision and '-' not in new:
                raise OSError(errno.EEXIST, "File already exists")
            self.assertEquals('/a/b/c/d/e', was)
            if '-' in new:
                self.assert_(
                    new.startswith('/a/quarantined/containers/e-'))
            else:
                self.assertEquals('/a/quarantined/containers/e', new)

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
        self.assertEquals(['/path/to/file'], self.delete_db_calls)

    def test_replicate_object_delete_because_not_shouldbehere(self):
        replicator = TestReplicator({})
        replicator.delete_db = self.stub_delete_db
        replicator._replicate_object('0', '/path/to/file', 'node_id')
        self.assertEquals(['/path/to/file'], self.delete_db_calls)

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
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        response = rpc.dispatch(('a',), 'arg')
        self.assertEquals('Invalid object type', response.body)
        self.assertEquals(400, response.status_int)

    def test_dispatch_drive_not_mounted(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, True)

        def mock_ismount(path):
            self.assertEquals('/drive', path)
            return False

        self._patch(patch.object, db_replicator, 'ismount', mock_ismount)

        response = rpc.dispatch(('drive', 'part', 'hash'), ['method'])

        self.assertEquals('507 drive is not mounted', response.status)
        self.assertEquals(507, response.status_int)

    def test_dispatch_unexpected_operation_db_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        def mock_mkdirs(path):
            self.assertEquals('/drive/tmp', path)

        self._patch(patch.object, db_replicator, 'mkdirs', mock_mkdirs)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = False
            response = rpc.dispatch(('drive', 'part', 'hash'), ['unexpected'])

        self.assertEquals('404 Not Found', response.status)
        self.assertEquals(404, response.status_int)

    def test_dispatch_operation_unexpected(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        self._patch(patch.object, db_replicator, 'mkdirs', lambda *args: True)

        def unexpected_method(broker, args):
            self.assertEquals(FakeBroker, broker.__class__)
            self.assertEqual(['arg1', 'arg2'], args)
            return 'unexpected-called'

        rpc.unexpected = unexpected_method

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = True
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['unexpected', 'arg1', 'arg2'])
            mock_os.path.exists.assert_called_with('/part/ash/hash/hash.db')

        self.assertEquals('unexpected-called', response)

    def test_dispatch_operation_rsync_then_merge(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        self._patch(patch.object, db_replicator, 'renamer', lambda *args: True)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = True
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['rsync_then_merge', 'arg1', 'arg2'])
            expected_calls = [call('/part/ash/hash/hash.db'),
                              call('/drive/tmp/arg1')]
            self.assertEquals(mock_os.path.exists.call_args_list,
                              expected_calls)
            self.assertEquals('204 No Content', response.status)
            self.assertEquals(204, response.status_int)

    def test_dispatch_operation_complete_rsync(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        self._patch(patch.object, db_replicator, 'renamer', lambda *args: True)

        with patch('swift.common.db_replicator.os', new=mock.MagicMock(
                wraps=os)) as mock_os:
            mock_os.path.exists.side_effect = [False, True]
            response = rpc.dispatch(('drive', 'part', 'hash'),
                                    ['complete_rsync', 'arg1', 'arg2'])
            expected_calls = [call('/part/ash/hash/hash.db'),
                              call('/drive/tmp/arg1')]
            self.assertEquals(mock_os.path.exists.call_args_list,
                              expected_calls)
            self.assertEquals('204 No Content', response.status)
            self.assertEquals(204, response.status_int)

    def test_rsync_then_merge_db_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = False
            response = rpc.rsync_then_merge('drive', '/data/db.db',
                                            ('arg1', 'arg2'))
            mock_os.path.exists.assert_called_with('/data/db.db')
            self.assertEquals('404 Not Found', response.status)
            self.assertEquals(404, response.status_int)

    def test_rsync_then_merge_old_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.side_effect = [True, False]
            response = rpc.rsync_then_merge('drive', '/data/db.db',
                                            ('arg1', 'arg2'))
            expected_calls = [call('/data/db.db'), call('/drive/tmp/arg1')]
            self.assertEquals(mock_os.path.exists.call_args_list,
                              expected_calls)
            self.assertEquals('404 Not Found', response.status)
            self.assertEquals(404, response.status_int)

    def test_rsync_then_merge_with_objects(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        def mock_renamer(old, new):
            self.assertEquals('/drive/tmp/arg1', old)
            self.assertEquals('/data/db.db', new)

        self._patch(patch.object, db_replicator, 'renamer', mock_renamer)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = True
            response = rpc.rsync_then_merge('drive', '/data/db.db',
                                            ['arg1', 'arg2'])
            self.assertEquals('204 No Content', response.status)
            self.assertEquals(204, response.status_int)

    def test_complete_rsync_db_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = True
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1', 'arg2'])
            mock_os.path.exists.assert_called_with('/data/db.db')
            self.assertEquals('404 Not Found', response.status)
            self.assertEquals(404, response.status_int)

    def test_complete_rsync_old_file_does_not_exist(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.return_value = False
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1', 'arg2'])
            expected_calls = [call('/data/db.db'), call('/drive/tmp/arg1')]
            self.assertEquals(expected_calls,
                              mock_os.path.exists.call_args_list)
            self.assertEquals('404 Not Found', response.status)
            self.assertEquals(404, response.status_int)

    def test_complete_rsync_rename(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)

        def mock_exists(path):
            if path == '/data/db.db':
                return False
            self.assertEquals('/drive/tmp/arg1', path)
            return True

        def mock_renamer(old, new):
            self.assertEquals('/drive/tmp/arg1', old)
            self.assertEquals('/data/db.db', new)

        self._patch(patch.object, db_replicator, 'renamer', mock_renamer)

        with patch('swift.common.db_replicator.os',
                   new=mock.MagicMock(wraps=os)) as mock_os:
            mock_os.path.exists.side_effect = [False, True]
            response = rpc.complete_rsync('drive', '/data/db.db',
                                          ['arg1', 'arg2'])
            self.assertEquals('204 No Content', response.status)
            self.assertEquals(204, response.status_int)

    def test_replicator_sync_with_broker_replication_missing_table(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        rpc.logger = unit.debug_logger()
        broker = FakeBroker()
        broker.get_repl_missing_table = True

        called = []

        def mock_quarantine_db(object_file, server_type):
            called.append(True)
            self.assertEquals(broker.db_file, object_file)
            self.assertEquals(broker.db_type, server_type)

        self._patch(patch.object, db_replicator, 'quarantine_db',
                    mock_quarantine_db)

        response = rpc.sync(broker, ('remote_sync', 'hash_', 'id_',
                                     'created_at', 'put_timestamp',
                                     'delete_timestamp', 'metadata'))

        self.assertEquals('404 Not Found', response.status)
        self.assertEquals(404, response.status_int)
        self.assertEqual(called, [True])
        errors = rpc.logger.get_lines_for_level('error')
        self.assertEqual(errors,
                         ["Unable to decode remote metadata 'metadata'",
                          "Quarantining DB %s" % broker])

    def test_replicator_sync(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        broker = FakeBroker()

        response = rpc.sync(broker, (broker.get_sync() + 1, 12345, 'id_',
                                     'created_at', 'put_timestamp',
                                     'delete_timestamp',
                                     '{"meta1": "data1", "meta2": "data2"}'))

        self.assertEquals({'meta1': 'data1', 'meta2': 'data2'},
                          broker.metadata)
        self.assertEquals('created_at', broker.created_at)
        self.assertEquals('put_timestamp', broker.put_timestamp)
        self.assertEquals('delete_timestamp', broker.delete_timestamp)

        self.assertEquals('200 OK', response.status)
        self.assertEquals(200, response.status_int)

    def test_rsync_then_merge(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        rpc.rsync_then_merge('sda1', '/srv/swift/blah', ('a', 'b'))

    def test_merge_items(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        fake_broker = FakeBroker()
        args = ('a', 'b')
        rpc.merge_items(fake_broker, args)
        self.assertEquals(fake_broker.args, args)

    def test_merge_syncs(self):
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        fake_broker = FakeBroker()
        args = ('a', 'b')
        rpc.merge_syncs(fake_broker, args)
        self.assertEquals(fake_broker.args, (args[0],))

    def test_complete_rsync_with_bad_input(self):
        drive = '/some/root'
        db_file = __file__
        args = ['old_file']
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        resp = rpc.complete_rsync(drive, db_file, args)
        self.assertTrue(isinstance(resp, HTTPException))
        self.assertEquals(404, resp.status_int)
        resp = rpc.complete_rsync(drive, 'new_db_file', args)
        self.assertTrue(isinstance(resp, HTTPException))
        self.assertEquals(404, resp.status_int)

    def test_complete_rsync(self):
        drive = mkdtemp()
        args = ['old_file']
        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
        os.mkdir('%s/tmp' % drive)
        old_file = '%s/tmp/old_file' % drive
        new_file = '%s/new_db_file' % drive
        try:
            fp = open(old_file, 'w')
            fp.write('void')
            fp.close
            resp = rpc.complete_rsync(drive, new_file, args)
            self.assertEquals(204, resp.status_int)
        finally:
            rmtree(drive)

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
                return ['123', '456', '789', '9999']
                # 456 will pretend to be a file
                # 9999 will be an empty partition with no contents
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

        orig_listdir = db_replicator.os.listdir
        orig_isdir = db_replicator.os.path.isdir
        orig_exists = db_replicator.os.path.exists
        orig_shuffle = db_replicator.random.shuffle
        orig_rmdir = db_replicator.os.rmdir

        try:
            db_replicator.os.listdir = _listdir
            db_replicator.os.path.isdir = _isdir
            db_replicator.os.path.exists = _exists
            db_replicator.random.shuffle = _shuffle
            db_replicator.os.rmdir = _rmdir

            datadirs = [('/srv/node/sda/containers', 1),
                        ('/srv/node/sdb/containers', 2)]
            results = list(db_replicator.roundrobin_datadirs(datadirs))
            # The results show that the .db files are returned, the devices
            # interleaved.
            self.assertEquals(results, [
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
            self.assertEquals(listdir_calls, [
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
            self.assertEquals(isdir_calls, [
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
            self.assertEquals(exists_calls, [
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
            self.assertEquals(
                shuffle_calls, [['123', '456', '789', '9999'],
                                ['123', '456', '789', '9999']])

            # Shows that we called removed the two empty partition directories.
            self.assertEquals(
                rmdir_calls, ['/srv/node/sda/containers/9999',
                              '/srv/node/sdb/containers/9999'])
        finally:
            db_replicator.os.listdir = orig_listdir
            db_replicator.os.path.isdir = orig_isdir
            db_replicator.os.path.exists = orig_exists
            db_replicator.random.shuffle = orig_shuffle
            db_replicator.os.rmdir = orig_rmdir

    @mock.patch("swift.common.db_replicator.ReplConnection", mock.Mock())
    def test_http_connect(self):
        node = "node"
        partition = "partition"
        db_file = __file__
        replicator = TestReplicator({})
        replicator._http_connect(node, partition, db_file)
        db_replicator.ReplConnection.assert_has_calls(
            mock.call(node, partition,
                      os.path.basename(db_file).split('.', 1)[0],
                      replicator.logger))


class TestReplToNode(unittest.TestCase):
    def setUp(self):
        db_replicator.ring = FakeRing()
        self.delete_db_calls = []
        self.broker = FakeBroker()
        self.replicator = TestReplicator({})
        self.fake_node = {'ip': '127.0.0.1', 'device': 'sda1', 'port': 1000}
        self.fake_info = {'id': 'a', 'point': -1, 'max_row': 10, 'hash': 'b',
                          'created_at': 100, 'put_timestamp': 0,
                          'delete_timestamp': 0, 'count': 0,
                          'metadata': {
                              'Test': ('Value', normalize_timestamp(1))}}
        self.replicator.logger = mock.Mock()
        self.replicator._rsync_db = mock.Mock(return_value=True)
        self.replicator._usync_db = mock.Mock(return_value=True)
        self.http = ReplHttp('{"id": 3, "point": -1}')
        self.replicator._http_connect = lambda *args: self.http

    def test_repl_to_node_usync_success(self):
        rinfo = {"id": 3, "point": -1, "max_row": 5, "hash": "c"}
        self.http = ReplHttp(simplejson.dumps(rinfo))
        local_sync = self.broker.get_sync()
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.replicator._usync_db.assert_has_calls([
            mock.call(max(rinfo['point'], local_sync), self.broker,
                      self.http, rinfo['id'], self.fake_info['id'])
        ])

    def test_repl_to_node_rsync_success(self):
        rinfo = {"id": 3, "point": -1, "max_row": 4, "hash": "c"}
        self.http = ReplHttp(simplejson.dumps(rinfo))
        self.broker.get_sync()
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.replicator.logger.increment.assert_has_calls([
            mock.call.increment('remote_merges')
        ])
        self.replicator._rsync_db.assert_has_calls([
            mock.call(self.broker, self.fake_node, self.http,
                      self.fake_info['id'],
                      replicate_method='rsync_then_merge',
                      replicate_timeout=(self.fake_info['count'] / 2000))
        ])

    def test_repl_to_node_already_in_sync(self):
        rinfo = {"id": 3, "point": -1, "max_row": 10, "hash": "b"}
        self.http = ReplHttp(simplejson.dumps(rinfo))
        self.broker.get_sync()
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.assertEquals(self.replicator._rsync_db.call_count, 0)
        self.assertEquals(self.replicator._usync_db.call_count, 0)

    def test_repl_to_node_not_found(self):
        self.http = ReplHttp('{"id": 3, "point": -1}', set_status=404)
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, self.broker, '0', self.fake_info), True)
        self.replicator.logger.increment.assert_has_calls([
            mock.call.increment('rsyncs')
        ])
        self.replicator._rsync_db.assert_has_calls([
            mock.call(self.broker, self.fake_node, self.http,
                      self.fake_info['id'])
        ])

    def test_repl_to_node_drive_not_mounted(self):
        self.http = ReplHttp('{"id": 3, "point": -1}', set_status=507)

        self.assertRaises(DriveNotMounted, self.replicator._repl_to_node,
                          self.fake_node, FakeBroker(), '0', self.fake_info)

    def test_repl_to_node_300_status(self):
        self.http = ReplHttp('{"id": 3, "point": -1}', set_status=300)

        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, FakeBroker(), '0', self.fake_info), None)

    def test_repl_to_node_not_response(self):
        self.http = mock.Mock(replicate=mock.Mock(return_value=None))
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, FakeBroker(), '0', self.fake_info), False)


class FakeHTTPResponse(object):

    def __init__(self, resp):
        self.resp = resp

    @property
    def status(self):
        return self.resp.status_int

    @property
    def data(self):
        return self.resp.body


def attach_fake_replication_rpc(rpc, replicate_hook=None):
    class FakeReplConnection(object):

        def __init__(self, node, partition, hash_, logger):
            self.logger = logger
            self.node = node
            self.partition = partition
            self.path = '/%s/%s/%s' % (node['device'], partition, hash_)
            self.host = node['replication_ip']

        def replicate(self, op, *sync_args):
            print 'REPLICATE: %s, %s, %r' % (self.path, op, sync_args)
            replicate_args = self.path.lstrip('/').split('/')
            args = [op] + list(sync_args)
            swob_response = rpc.dispatch(replicate_args, args)
            resp = FakeHTTPResponse(swob_response)
            if replicate_hook:
                replicate_hook(op, *sync_args)
            return resp

    return FakeReplConnection


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
            self.root, self.datadir, self.backend, False,
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

    def _run_once(self, node, conf_updates=None, daemon=None):
        daemon = daemon or self._get_daemon(node, conf_updates)

        def _rsync_file(db_file, remote_file, **kwargs):
            remote_server, remote_path = remote_file.split('/', 1)
            dest_path = os.path.join(self.root, remote_path)
            copy(db_file, dest_path)
            return True
        daemon._rsync_file = _rsync_file
        with mock.patch('swift.common.db_replicator.whataremyips',
                        new=lambda: [node['replication_ip']]):
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


if __name__ == '__main__':
    unittest.main()
