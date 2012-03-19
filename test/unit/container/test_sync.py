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

from swift.container import sync
from swift.common import utils
from swift.common.client import ClientException


utils.HASH_PATH_SUFFIX = 'endcap'


class FakeRing(object):

    def __init__(self):
        self.replica_count = 3
        self.devs = [{'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}
                     for x in xrange(3)]

    def get_nodes(self, account, container=None, obj=None):
        return 1, list(self.devs)


class FakeContainerBroker(object):

    def __init__(self, path, metadata=None, info=None, deleted=False,
                 items_since=None):
        self.db_file = path
        self.metadata = metadata if metadata else {}
        self.info = info if info else {}
        self.deleted = deleted
        self.items_since = items_since if items_since else []
        self.sync_point1 = -1
        self.sync_point2 = -1

    def get_info(self):
        return self.info

    def is_deleted(self):
        return self.deleted

    def get_items_since(self, sync_point, limit):
        if sync_point < 0:
            sync_point = 0
        return self.items_since[sync_point:sync_point + limit]

    def set_x_container_sync_points(self, sync_point1, sync_point2):
        self.sync_point1 = sync_point1
        self.sync_point2 = sync_point2


class TestContainerSync(unittest.TestCase):

    def test_Iter2FileLikeObject(self):
        flo = sync._Iter2FileLikeObject(iter(['123', '4567', '89', '0']))
        expect = '1234567890'

        got = flo.read(2)
        self.assertTrue(len(got) <= 2)
        self.assertEquals(got, expect[:len(got)])
        expect = expect[len(got):]

        got = flo.read(5)
        self.assertTrue(len(got) <= 5)
        self.assertEquals(got, expect[:len(got)])
        expect = expect[len(got):]

        self.assertEquals(flo.read(), expect)
        self.assertEquals(flo.read(), '')
        self.assertEquals(flo.read(2), '')

        flo = sync._Iter2FileLikeObject(iter(['123', '4567', '89', '0']))
        self.assertEquals(flo.read(), '1234567890')
        self.assertEquals(flo.read(), '')
        self.assertEquals(flo.read(2), '')

    def test_init(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        self.assertTrue(cs.container_ring is cring)
        self.assertTrue(cs.object_ring is oring)

    def test_run_forever(self):
        # This runs runs_forever with fakes to succeed for two loops, the first
        # causing a report but no interval sleep, the second no report but an
        # interval sleep.
        time_calls = [0]
        sleep_calls = []
        audit_location_generator_calls = [0]

        def fake_time():
            time_calls[0] += 1
            returns = [1,     # Initialized reported time
                       1,     # Start time
                       3602,  # Is it report time (yes)
                       3602,  # Report time
                       3602,  # Elapsed time for "under interval" (no)
                       3602,  # Start time
                       3603,  # Is it report time (no)
                       3603,  # Elapsed time for "under interval" (yes)
                      ]
            if time_calls[0] == len(returns) + 1:
                raise Exception('we are now done')
            return returns[time_calls[0] - 1]

        def fake_sleep(amount):
            sleep_calls.append(amount)

        def fake_audit_location_generator(*args, **kwargs):
            audit_location_generator_calls[0] += 1
            # Makes .container_sync() short-circuit because 'path' doesn't end
            # with .db
            return [('path', 'device', 'partition')]

        orig_time = sync.time
        orig_sleep = sync.sleep
        orig_audit_location_generator = sync.audit_location_generator
        try:
            sync.time = fake_time
            sync.sleep = fake_sleep
            sync.audit_location_generator = fake_audit_location_generator
            cs = sync.ContainerSync({}, container_ring=FakeRing(),
                                    object_ring=FakeRing())
            cs.run_forever()
        except Exception, err:
            if str(err) != 'we are now done':
                raise
        finally:
            sync.time = orig_time
            sync.sleep = orig_sleep
            sync.audit_location_generator = orig_audit_location_generator

        self.assertEquals(time_calls, [9])
        self.assertEquals(len(sleep_calls), 2)
        self.assertTrue(sleep_calls[0] <= cs.interval)
        self.assertTrue(sleep_calls[1] == cs.interval - 1)
        self.assertEquals(audit_location_generator_calls, [2])
        self.assertEquals(cs.reported, 3602)

    def test_run_once(self):
        # This runs runs_once with fakes twice, the first causing an interim
        # report, the second with no interm report.
        time_calls = [0]
        audit_location_generator_calls = [0]

        def fake_time():
            time_calls[0] += 1
            returns = [1,     # Initialized reported time
                       1,     # Start time
                       3602,  # Is it report time (yes)
                       3602,  # Report time
                       3602,  # End report time
                       3602,  # For elapsed
                       3602,  # Start time
                       3603,  # Is it report time (no)
                       3604,  # End report time
                       3605,  # For elapsed
                      ]
            if time_calls[0] == len(returns) + 1:
                raise Exception('we are now done')
            return returns[time_calls[0] - 1]

        def fake_audit_location_generator(*args, **kwargs):
            audit_location_generator_calls[0] += 1
            # Makes .container_sync() short-circuit because 'path' doesn't end
            # with .db
            return [('path', 'device', 'partition')]

        orig_time = sync.time
        orig_audit_location_generator = sync.audit_location_generator
        try:
            sync.time = fake_time
            sync.audit_location_generator = fake_audit_location_generator
            cs = sync.ContainerSync({}, container_ring=FakeRing(),
                                    object_ring=FakeRing())
            cs.run_once()
            self.assertEquals(time_calls, [6])
            self.assertEquals(audit_location_generator_calls, [1])
            self.assertEquals(cs.reported, 3602)
            cs.run_once()
        except Exception, err:
            if str(err) != 'we are now done':
                raise
        finally:
            sync.time = orig_time
            sync.audit_location_generator = orig_audit_location_generator

        self.assertEquals(time_calls, [10])
        self.assertEquals(audit_location_generator_calls, [2])
        self.assertEquals(cs.reported, 3604)

    def test_container_sync_not_db(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        self.assertEquals(cs.container_failures, 0)

    def test_container_sync_missing_db(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        cs.container_sync('isa.db')
        self.assertEquals(cs.container_failures, 1)

    def test_container_sync_not_my_db(self):
        # Db could be there due to handoff replication so test that we ignore
        # those.
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        orig_ContainerBroker = sync.ContainerBroker
        try:
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c'})
            cs._myips = ['127.0.0.1']   # No match
            cs._myport = 1              # No match
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 0)

            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1              # No match
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 0)

            cs._myips = ['127.0.0.1']   # No match
            cs._myport = 1000           # Match
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 0)

            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            # This complete match will cause the 1 container failure since the
            # broker's info doesn't contain sync point keys
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)
        finally:
            sync.ContainerBroker = orig_ContainerBroker

    def test_container_sync_deleted(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        orig_ContainerBroker = sync.ContainerBroker
        try:
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c'}, deleted=False)
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            # This complete match will cause the 1 container failure since the
            # broker's info doesn't contain sync point keys
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)

            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c'}, deleted=True)
            # This complete match will not cause any more container failures
            # since the broker indicates deletion
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)
        finally:
            sync.ContainerBroker = orig_ContainerBroker

    def test_container_sync_no_to_or_key(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        orig_ContainerBroker = sync.ContainerBroker
        try:
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1})
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            # This complete match will be skipped since the broker's metadata
            # has no x-container-sync-to or x-container-sync-key
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 0)
            self.assertEquals(cs.container_skips, 1)
 
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1)})
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            # This complete match will be skipped since the broker's metadata
            # has no x-container-sync-key
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 0)
            self.assertEquals(cs.container_skips, 2)
 
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-key': ('key', 1)})
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            # This complete match will be skipped since the broker's metadata
            # has no x-container-sync-to
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 0)
            self.assertEquals(cs.container_skips, 3)
 
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)})
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = []
            # This complete match will cause a container failure since the
            # sync-to won't validate as allowed.
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 3)
 
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)})
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            # This complete match will succeed completely since the broker
            # get_items_since will return no new rows.
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 3)
        finally:
            sync.ContainerBroker = orig_ContainerBroker

    def test_container_stop_at(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        orig_ContainerBroker = sync.ContainerBroker
        orig_time = sync.time
        try:
            sync.ContainerBroker = lambda p: FakeContainerBroker(p,
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=['erroneous data'])
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            # This sync will fail since the items_since data is bad.
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 0)

            # Set up fake times to make the sync short-circuit as having taken
            # too long
            fake_times = [
                1.0,        # Compute the time to move on
                100000.0,   # Compute if it's time to move on from first loop
                100000.0]   # Compute if it's time to move on from second loop

            def fake_time():
                return fake_times.pop(0)
                
            sync.time = fake_time
            # This same sync won't fail since it will look like it took so long
            # as to be time to move on (before it ever actually tries to do
            # anything).
            cs.container_sync('isa.db')
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 0)
        finally:
            sync.ContainerBroker = orig_ContainerBroker
            sync.time = orig_time

    def test_container_first_loop(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        orig_ContainerBroker = sync.ContainerBroker
        orig_hash_path = sync.hash_path
        orig_delete_object = sync.delete_object
        try:

            def fake_hash_path(account, container, obj, raw_digest=False):
                # Ensures that no rows match for full syncing, ordinal is 0 and
                # all hashes are 0
                return '\x00' * 16

            sync.hash_path = fake_hash_path
            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': 2,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o'}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Succeeds because no rows match
            self.assertEquals(cs.container_failures, 0)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, None)
            self.assertEquals(fcb.sync_point2, 1)

            def fake_hash_path(account, container, obj, raw_digest=False):
                # Ensures that all rows match for full syncing, ordinal is 0
                # and all hashes are 1
                return '\x01' * 16

            sync.hash_path = fake_hash_path
            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': 1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o'}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Succeeds because the two sync points haven't deviated enough yet
            self.assertEquals(cs.container_failures, 0)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, -1)
            self.assertEquals(fcb.sync_point2, -1)

            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': 2,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o'}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Fails because container_sync_row will fail since the row has no
            # 'deleted' key
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, -1)
            self.assertEquals(fcb.sync_point2, -1)

            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': 2,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o', 'created_at': '1.2',
                              'deleted': True}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Fails because delete_object fails
            self.assertEquals(cs.container_failures, 2)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, -1)
            self.assertEquals(fcb.sync_point2, -1)

            def fake_delete_object(*args, **kwargs):
                pass

            sync.delete_object = fake_delete_object
            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': 2,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o', 'created_at': '1.2',
                              'deleted': True}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Succeeds because delete_object succeeds
            self.assertEquals(cs.container_failures, 2)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, None)
            self.assertEquals(fcb.sync_point2, 1)
        finally:
            sync.ContainerBroker = orig_ContainerBroker
            sync.hash_path = orig_hash_path
            sync.delete_object = orig_delete_object

    def test_container_second_loop(self):
        cring = FakeRing()
        oring = FakeRing()
        cs = sync.ContainerSync({}, container_ring=cring, object_ring=oring)
        orig_ContainerBroker = sync.ContainerBroker
        orig_hash_path = sync.hash_path
        orig_delete_object = sync.delete_object
        try:
            # We'll ensure the first loop is always skipped by keeping the two
            # sync points equal

            def fake_hash_path(account, container, obj, raw_digest=False):
                # Ensures that no rows match for second loop, ordinal is 0 and
                # all hashes are 1
                return '\x01' * 16

            sync.hash_path = fake_hash_path
            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o'}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Succeeds because no rows match
            self.assertEquals(cs.container_failures, 0)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, 1)
            self.assertEquals(fcb.sync_point2, None)

            def fake_hash_path(account, container, obj, raw_digest=False):
                # Ensures that all rows match for second loop, ordinal is 0 and
                # all hashes are 0
                return '\x00' * 16

            def fake_delete_object(*args, **kwargs):
                pass

            sync.hash_path = fake_hash_path
            sync.delete_object = fake_delete_object
            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o'}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Fails because row is missing 'deleted' key
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, -1)
            self.assertEquals(fcb.sync_point2, -1)

            fcb = FakeContainerBroker('path',
                info={'account': 'a', 'container': 'c',
                      'x_container_sync_point1': -1,
                      'x_container_sync_point2': -1},
                metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                          'x-container-sync-key': ('key', 1)},
                items_since=[{'ROWID': 1, 'name': 'o', 'created_at': '1.2',
                              'deleted': True}])
            sync.ContainerBroker = lambda p: fcb
            cs._myips = ['10.0.0.0']    # Match
            cs._myport = 1000           # Match
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')
            # Succeeds because row now has 'deleted' key and delete_object
            # succeeds
            self.assertEquals(cs.container_failures, 1)
            self.assertEquals(cs.container_skips, 0)
            self.assertEquals(fcb.sync_point1, 1)
            self.assertEquals(fcb.sync_point2, None)
        finally:
            sync.ContainerBroker = orig_ContainerBroker
            sync.hash_path = orig_hash_path
            sync.delete_object = orig_delete_object

    def test_container_sync_row_delete(self):
        orig_delete_object = sync.delete_object
        try:

            def fake_delete_object(path, name=None, headers=None, proxy=None):
                self.assertEquals(path, 'http://sync/to/path')
                self.assertEquals(name, 'object')
                self.assertEquals(headers,
                    {'x-container-sync-key': 'key', 'x-timestamp': '1.2'})
                self.assertEquals(proxy, 'http://proxy')

            sync.delete_object = fake_delete_object
            cs = sync.ContainerSync({}, container_ring=FakeRing(),
                                    object_ring=FakeRing())
            cs.proxy = 'http://proxy'
            # Success
            self.assertTrue(cs.container_sync_row({'deleted': True,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), 'info'))
            self.assertEquals(cs.container_deletes, 1)

            exc = []

            def fake_delete_object(path, name=None, headers=None, proxy=None):
                exc.append(Exception('test exception'))
                raise exc[-1]

            sync.delete_object = fake_delete_object
            # Failure because of delete_object exception
            self.assertFalse(cs.container_sync_row({'deleted': True,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), 'info'))
            self.assertEquals(cs.container_deletes, 1)
            self.assertEquals(len(exc), 1)
            self.assertEquals(str(exc[-1]), 'test exception')

            def fake_delete_object(path, name=None, headers=None, proxy=None):
                exc.append(ClientException('test client exception'))
                raise exc[-1]

            sync.delete_object = fake_delete_object
            # Failure because of delete_object exception
            self.assertFalse(cs.container_sync_row({'deleted': True,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), 'info'))
            self.assertEquals(cs.container_deletes, 1)
            self.assertEquals(len(exc), 2)
            self.assertEquals(str(exc[-1]), 'test client exception')

            def fake_delete_object(path, name=None, headers=None, proxy=None):
                exc.append(ClientException('test client exception',
                                           http_status=404))
                raise exc[-1]

            sync.delete_object = fake_delete_object
            # Success because the object wasn't even found
            self.assertTrue(cs.container_sync_row({'deleted': True,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), 'info'))
            self.assertEquals(cs.container_deletes, 2)
            self.assertEquals(len(exc), 3)
            self.assertEquals(str(exc[-1]), 'test client exception: 404')
        finally:
            sync.delete_object = orig_delete_object

    def test_container_sync_row_put(self):
        orig_shuffle = sync.shuffle
        orig_put_object = sync.put_object
        orig_direct_get_object = sync.direct_get_object
        try:
            sync.shuffle = lambda x: x

            def fake_put_object(sync_to, name=None, headers=None,
                                contents=None, proxy=None):
                self.assertEquals(sync_to, 'http://sync/to/path')
                self.assertEquals(name, 'object')
                self.assertEquals(headers, {'x-container-sync-key': 'key',
                    'x-timestamp': '1.2',
                    'other-header': 'other header value',
                    'etag': 'etagvalue'})
                self.assertEquals(contents.read(), 'contents')
                self.assertEquals(proxy, 'http://proxy')

            sync.put_object = fake_put_object

            cs = sync.ContainerSync({}, container_ring=FakeRing(),
                                    object_ring=FakeRing())
            cs.proxy = 'http://proxy'

            def fake_direct_get_object(node, part, account, container, obj,
                                       resp_chunk_size=1):
                return ({'other-header': 'other header value',
                         'etag': '"etagvalue"', 'x-timestamp': '1.2'},
                        iter('contents'))

            sync.direct_get_object = fake_direct_get_object
            # Success as everything says it worked
            self.assertTrue(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 1)

            def fake_direct_get_object(node, part, account, container, obj,
                                       resp_chunk_size=1):
                return ({'date': 'date value',
                         'last-modified': 'last modified value',
                         'x-timestamp': '1.2',
                         'other-header': 'other header value',
                         'etag': '"etagvalue"'},
                        iter('contents'))

            sync.direct_get_object = fake_direct_get_object
            # Success as everything says it worked, also checks 'date' and
            # 'last-modified' headers are removed and that 'etag' header is
            # stripped of double quotes.
            self.assertTrue(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 2)

            exc = []

            def fake_direct_get_object(node, part, account, container, obj,
                                       resp_chunk_size=1):
                exc.append(Exception('test exception'))
                raise exc[-1]

            sync.direct_get_object = fake_direct_get_object
            # Fail due to completely unexpected exception
            self.assertFalse(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 2)
            self.assertEquals(len(exc), 1)
            self.assertEquals(str(exc[-1]), 'test exception')

            def fake_direct_get_object(node, part, account, container, obj,
                                       resp_chunk_size=1):
                exc.append(ClientException('test client exception'))
                raise exc[-1]

            sync.direct_get_object = fake_direct_get_object
            # Fail due to all direct_get_object calls failing
            self.assertFalse(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 2)
            self.assertEquals(len(exc), 4)
            self.assertEquals(str(exc[-1]), 'test client exception')

            def fake_direct_get_object(node, part, account, container, obj,
                                       resp_chunk_size=1):
                return ({'other-header': 'other header value',
                         'x-timestamp': '1.2', 'etag': '"etagvalue"'},
                        iter('contents'))

            def fake_put_object(sync_to, name=None, headers=None,
                                contents=None, proxy=None):
                raise ClientException('test client exception', http_status=401)

            class FakeLogger(object):

                def __init__(self):
                    self.err = ''
                    self.exc = ''

                def info(self, err, *args, **kwargs):
                    self.err = err

                def exception(self, exc, *args, **kwargs):
                    self.exc = exc

            sync.direct_get_object = fake_direct_get_object
            sync.put_object = fake_put_object
            cs.logger = FakeLogger()
            # Fail due to 401
            self.assertFalse(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 2)
            self.assertTrue(cs.logger.err.startswith('Unauth '))

            def fake_put_object(sync_to, name=None, headers=None,
                                contents=None, proxy=None):
                raise ClientException('test client exception', http_status=404)

            sync.put_object = fake_put_object
            # Fail due to 404
            self.assertFalse(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 2)
            self.assertTrue(cs.logger.err.startswith('Not found '))

            def fake_put_object(sync_to, name=None, headers=None,
                                contents=None, proxy=None):
                raise ClientException('test client exception', http_status=503)

            sync.put_object = fake_put_object
            # Fail due to 503
            self.assertFalse(cs.container_sync_row({'deleted': False,
                'name': 'object', 'created_at': '1.2'}, 'http://sync/to/path',
                'key', FakeContainerBroker('broker'), {'account': 'a',
                'container': 'c'}))
            self.assertEquals(cs.container_puts, 2)
            self.assertTrue(cs.logger.exc.startswith('ERROR Syncing '))
        finally:
            sync.shuffle = orig_shuffle
            sync.put_object = orig_put_object
            sync.direct_get_object = orig_direct_get_object


if __name__ == '__main__':
    unittest.main()
