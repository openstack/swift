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

import unittest
from contextlib import contextmanager
import os
import logging
import errno

from swift.common import db_replicator
from swift.common import utils
from swift.common.utils import normalize_timestamp
from swift.container import server as container_server


def teardown_module():
    "clean up my monkey patching"
    reload(db_replicator)

@contextmanager
def lock_parent_directory(filename):
    yield True

class FakeRing:
    class Ring:
        devs = []
        def __init__(self, path):
            pass
        def get_part_nodes(self, part):
            return []
        def get_more_nodes(self, *args):
            return []

class FakeProcess:
    def __init__(self, *codes):
        self.codes = iter(codes)
    def __call__(self, *args, **kwargs):
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
    yield
    db_replicator.subprocess.Popen = orig_process

class ReplHttp:
    def __init__(self, response=None):
        self.response = response
    replicated = False
    host = 'localhost'
    def replicate(self, *args):
        self.replicated = True
        class Response:
            status = 200
            data = self.response
            def read(innerself):
                return self.response
        return Response()

class ChangingMtimesOs:
    def __init__(self):
        self.mtime = 0
        self.path = self
        self.basename = os.path.basename
    def getmtime(self, file):
        self.mtime += 1
        return self.mtime

class FakeBroker:
    db_file = __file__
    get_repl_missing_table = False
    db_type = 'container'
    def __init__(self, *args, **kwargs):
        return None
    @contextmanager
    def lock(self):
        yield True
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
        return {'delete_timestamp': 0, 'put_timestamp': 1, 'count': 0}
    def reclaim(self, item_timestamp, sync_timestamp):
        pass

db_replicator.ring = FakeRing()


class TestReplicator(db_replicator.Replicator):
    server_type = 'container'
    ring_file = 'container.ring.gz'
    brokerclass = FakeBroker
    datadir = container_server.DATADIR
    default_port = 1000

class TestDBReplicator(unittest.TestCase):

    def test_repl_connection(self):
        node = {'ip': '127.0.0.1', 'port': 80, 'device': 'sdb1'}
        conn = db_replicator.ReplConnection(node, '1234567890', 'abcdefg',
                    logging.getLogger())
        def req(method, path, body, headers):
            self.assertEquals(method, 'REPLICATE')
            self.assertEquals(headers['Content-Type'], 'application/json')
        class Resp:
            def read(self): return 'data'
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
            fake_device = {'ip': '127.0.0.1', 'device': 'sda1'}
            self.assertEquals(False,
                    replicator._rsync_file('/some/file', 'remote:/some/file'))
        with _mock_process(0):
            fake_device = {'ip': '127.0.0.1', 'device': 'sda1'}
            self.assertEquals(True,
                    replicator._rsync_file('/some/file', 'remote:/some/file'))

    def test_rsync_db(self):
        replicator = TestReplicator({})
        replicator._rsync_file = lambda *args: True
        fake_device = {'ip': '127.0.0.1', 'device': 'sda1'}
        replicator._rsync_db(FakeBroker(), fake_device, ReplHttp(), 'abcd')

    def test_in_sync(self):
        replicator = TestReplicator({})
        self.assertEquals(replicator._in_sync(
            {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'b'},
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

    def test_repl_to_node(self):
        replicator = TestReplicator({})
        fake_node = {'ip': '127.0.0.1', 'device': 'sda1', 'port': 1000}
        fake_info = {'id': 'a', 'point': -1, 'max_row': 0, 'hash': 'b',
                    'created_at': 100, 'put_timestamp': 0,
                    'delete_timestamp': 0,
                    'metadata': {'Test': ('Value', normalize_timestamp(1))}}
        replicator._http_connect = lambda *args: ReplHttp('{"id": 3, "point": -1}')
        self.assertEquals(replicator._repl_to_node(
            fake_node, FakeBroker(), '0', fake_info), True)

    def test_stats(self):
        # I'm not sure how to test that this logs the right thing,
        # but we can at least make sure it gets covered.
        replicator = TestReplicator({})
        replicator._zero_stats()
        replicator._report_stats()

    def test_replicate_object(self):
        db_replicator.lock_parent_directory = lock_parent_directory
        replicator = TestReplicator({})
        replicator._replicate_object('0', 'file', 'node_id')

    def test_replicate_object_quarantine(self):
        replicator = TestReplicator({})
        was_db_file = replicator.brokerclass.db_file
        try:

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
            was_renamer = db_replicator.renamer
            db_replicator.renamer = mock_renamer
            db_replicator.lock_parent_directory = lock_parent_directory
            replicator.brokerclass.get_repl_missing_table = True
            replicator.brokerclass.db_file = '/a/b/c/d/e/hey'
            replicator._replicate_object('0', 'file', 'node_id')
            # try the double quarantine
            db_replicator.renamer = mock_renamer_error
            replicator._replicate_object('0', 'file', 'node_id')
        finally:
            replicator.brokerclass.db_file = was_db_file
            db_replicator.renamer = was_renamer

#    def test_dispatch(self):
#        rpc = db_replicator.ReplicatorRpc('/', '/', FakeBroker, False)
#        no_op = lambda *args, **kwargs: True
#        self.assertEquals(rpc.dispatch(('drv', 'part', 'hash'), ('op',)
#                ).status_int, 400)
#        rpc.mount_check = True
#        self.assertEquals(rpc.dispatch(('drv', 'part', 'hash'), ['op',]
#                ).status_int, 507)
#        rpc.mount_check = False
#        rpc.rsync_then_merge = lambda drive, db_file, args: self.assertEquals(args, ['test1'])
#        rpc.complete_rsync = lambda drive, db_file, args: self.assertEquals(args, ['test2'])
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

if __name__ == '__main__':
    unittest.main()

