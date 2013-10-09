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
from mock import patch
from shutil import rmtree
from tempfile import mkdtemp, NamedTemporaryFile
import mock
import simplejson

from swift.common import db_replicator
from swift.common.utils import normalize_timestamp
from swift.container import server as container_server
from swift.common.exceptions import DriveNotMounted

from test.unit import FakeLogger


TEST_ACCOUNT_NAME = 'a c t'
TEST_CONTAINER_NAME = 'c o n'


def teardown_module():
    "clean up my monkey patching"
    reload(db_replicator)


@contextmanager
def lock_parent_directory(filename):
    yield True


class FakeRing:
    class Ring:
        devs = []

        def __init__(self, path, reload_time=15, ring_name=None):
            pass

        def get_part(self, account, container=None, obj=None):
            return 0

        def get_part_nodes(self, part):
            return []

        def get_more_nodes(self, *args):
            return []


class FakeRingWithNodes:
    class Ring:
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


class FakeProcess:
    def __init__(self, *codes):
        self.codes = iter(codes)
        self.args = None
        self.kwargs = None

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        class Failure:
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


class ReplHttp:
    def __init__(self, response=None, set_status=200):
        self.response = response
        self.set_status = set_status
    replicated = False
    host = 'localhost'

    def replicate(self, *args):
        self.replicated = True

        class Response:
            status = self.set_status
            data = self.response

            def read(innerself):
                return self.response
        return Response()


class ChangingMtimesOs:
    def __init__(self):
        self.mtime = 0

    def __call__(self, *args, **kwargs):
        self.mtime += 1
        return self.mtime


class FakeBroker:
    db_file = __file__
    get_repl_missing_table = False
    stub_replication_info = None
    db_type = 'container'
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
        return []

    def merge_syncs(self, *args, **kwargs):
        self.args = args

    def merge_items(self, *args):
        self.args = args

    def get_replication_info(self):
        if self.get_repl_missing_table:
            raise Exception('no such table')
        if self.stub_replication_info:
            return self.stub_replication_info
        return {'delete_timestamp': 0, 'put_timestamp': 1, 'count': 0}

    def reclaim(self, item_timestamp, sync_timestamp):
        pass

    def get_info(self):
        return self.info


class FakeAccountBroker(FakeBroker):
    db_type = 'account'
    info = {'account': TEST_ACCOUNT_NAME}


class TestReplicator(db_replicator.Replicator):
    server_type = 'container'
    ring_file = 'container.ring.gz'
    brokerclass = FakeBroker
    datadir = container_server.DATADIR
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

    def stub_delete_db(self, object_file):
        self.delete_db_calls.append(object_file)

    def test_repl_connection(self):
        node = {'replication_ip': '127.0.0.1', 'replication_port': 80,
                'device': 'sdb1'}
        conn = db_replicator.ReplConnection(node, '1234567890', 'abcdefg',
                                            logging.getLogger())

        def req(method, path, body, headers):
            self.assertEquals(method, 'REPLICATE')
            self.assertEquals(headers['Content-Type'], 'application/json')

        class Resp:
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

    def test_usync(self):
        fake_http = ReplHttp()
        replicator = TestReplicator({})
        replicator._usync_db(0, FakeBroker(), fake_http, '12345', '67890')

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

        def mock_renamer(was, new, cause_colision=False):
            if cause_colision and '-' not in new:
                raise OSError(errno.EEXIST, "File already exists")
            self.assertEquals('/a/b/c/d/e', was)
            if '-' in new:
                self.assert_(
                    new.startswith('/a/quarantined/containers/e-'))
            else:
                self.assertEquals('/a/quarantined/containers/e', new)

        def mock_renamer_error(was, new):
            return mock_renamer(was, new, cause_colision=True)
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
                'delete_timestamp': 2, 'put_timestamp': 1, 'count': 0}
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
        replicator = TestReplicator({})
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator.brokerclass = FakeAccountBroker
        replicator._repl_to_node = lambda *args: True
        replicator.delete_db = self.stub_delete_db
        replicator.logger = FakeLogger()
        # Correct node_id, wrong part
        part = replicator.ring.get_part(TEST_ACCOUNT_NAME) + 1
        node_id = replicator.ring.get_part_nodes(part)[0]['id']
        replicator._replicate_object(str(part), '/path/to/file', node_id)
        self.assertEqual(['/path/to/file'], self.delete_db_calls)
        self.assertEqual(
            replicator.logger.log_dict['error'],
            [(('Found /path/to/file for /a%20c%20t when it should be on '
               'partition 0; will replicate out and remove.',), {})])

    def test_replicate_container_out_of_place(self):
        replicator = TestReplicator({})
        replicator.ring = FakeRingWithNodes().Ring('path')
        replicator._repl_to_node = lambda *args: True
        replicator.delete_db = self.stub_delete_db
        replicator.logger = FakeLogger()
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
        replicator = TestReplicator({})
        replicator._zero_stats()
        replicator.extract_device = lambda _: 'some_device'
        replicator.logger = FakeLogger()

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

            replicator.delete_db(temp_file.name)

            self.assertTrue(os.path.exists(temp_dir))
            self.assertTrue(os.path.exists(temp_suf_dir))
            self.assertFalse(os.path.exists(temp_hash_dir))
            self.assertFalse(os.path.exists(temp_file.name))
            self.assertTrue(os.path.exists(temp_hash_dir2))
            self.assertTrue(os.path.exists(temp_file2.name))
            self.assertEqual([(('removes.some_device',), {})],
                             replicator.logger.log_dict['increment'])
            self.assertEqual(1, replicator.stats['remove'])

            replicator.delete_db(temp_file2.name)

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

#    def test_dispatch(self):
#        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
#        no_op = lambda *args, **kwargs: True
#        self.assertEquals(rpc.dispatch(('drv', 'part', 'hash'), ('op',)
#                ).status_int, 400)
#        rpc.mount_check = True
#        self.assertEquals(rpc.dispatch(('drv', 'part', 'hash'), ['op',]
#                ).status_int, 507)
#        rpc.mount_check = False
#        rpc.rsync_then_merge = lambda drive, db_file,
#                                      args: self.assertEquals(args, ['test1'])
#        rpc.complete_rsync = lambda drive, db_file,
#                                      args: self.assertEquals(args, ['test2'])
#        rpc.dispatch(('drv', 'part', 'hash'), ['rsync_then_merge','test1'])
#        rpc.dispatch(('drv', 'part', 'hash'), ['complete_rsync','test2'])
#        rpc.dispatch(('drv', 'part', 'hash'), ['other_op',])

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

    def test_roundrobin_datadirs(self):
        listdir_calls = []
        isdir_calls = []
        exists_calls = []
        shuffle_calls = []

        def _listdir(path):
            listdir_calls.append(path)
            if not path.startswith('/srv/node/sda/containers') and \
                    not path.startswith('/srv/node/sdb/containers'):
                return []
            path = path[len('/srv/node/sdx/containers'):]
            if path == '':
                return ['123', '456', '789']  # 456 will pretend to be a file
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
                        '/789/ghi/22222222222222222222222222222ghi/weird2'):
                return True
            return False

        def _exists(arg):
            exists_calls.append(arg)
            return True

        def _shuffle(arg):
            shuffle_calls.append(arg)

        orig_listdir = db_replicator.os.listdir
        orig_isdir = db_replicator.os.path.isdir
        orig_exists = db_replicator.os.path.exists
        orig_shuffle = db_replicator.random.shuffle
        try:
            db_replicator.os.listdir = _listdir
            db_replicator.os.path.isdir = _isdir
            db_replicator.os.path.exists = _exists
            db_replicator.random.shuffle = _shuffle
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
                '/srv/node/sdb/containers/789/ghi'])
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
                ('/srv/node/sdb/containers/789/ghi/'
                 '33333333333333333333333333333ghi'),
                '/srv/node/sdb/containers/789/jkl'])
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
                shuffle_calls, [['123', '456', '789'], ['123', '456', '789']])
        finally:
            db_replicator.os.listdir = orig_listdir
            db_replicator.os.path.isdir = orig_isdir
            db_replicator.os.path.exists = orig_exists
            db_replicator.random.shuffle = orig_shuffle

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

    def test_repl_to_node_http_connect_fails(self):
        self.replicator._http_connect = lambda *args: None
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, FakeBroker(), '0', self.fake_info), False)

    def test_repl_to_node_not_response(self):
        self.http = mock.Mock(replicate=mock.Mock(return_value=None))
        self.assertEquals(self.replicator._repl_to_node(
            self.fake_node, FakeBroker(), '0', self.fake_info), False)


if __name__ == '__main__':
    unittest.main()
