# Copyright (c) 2013 - 2015 OpenStack Foundation
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
from collections import defaultdict

import mock
import os
import time
import unittest

import eventlet
import itertools
from six.moves import urllib

from swift.common.exceptions import DiskFileNotExist, DiskFileError, \
    DiskFileDeleted, DiskFileExpired
from swift.common import utils
from swift.common.storage_policy import POLICIES, EC_POLICY
from swift.common.utils import Timestamp
from swift.obj import ssync_sender, server
from swift.obj.reconstructor import RebuildingECDiskFileStream, \
    ObjectReconstructor
from swift.obj.replicator import ObjectReplicator

from test import listen_zero
from test.unit.obj.common import BaseTest
from test.unit import patch_policies, debug_logger, \
    encode_frag_archive_bodies, skip_if_no_xattrs, quiet_eventlet_exceptions


class TestBaseSsync(BaseTest):
    """
    Provides a framework to test end to end interactions between sender and
    receiver. The basis for each test is actual diskfile state on either side.
    The connection between sender and receiver is wrapped to capture ssync
    traffic for subsequent verification of the protocol. Assertions are made
    about the final state of the sender and receiver diskfiles.
    """
    def setUp(self):
        skip_if_no_xattrs()
        super(TestBaseSsync, self).setUp()
        # rx side setup
        self.rx_testdir = os.path.join(self.tmpdir, 'tmp_test_ssync_receiver')
        utils.mkdirs(os.path.join(self.rx_testdir, self.device))
        conf = {
            'devices': self.rx_testdir,
            'mount_check': 'false',
            'replication_concurrency_per_device': '0',
            'log_requests': 'false'}
        self.rx_logger = debug_logger()
        self.rx_controller = server.ObjectController(conf, self.rx_logger)
        self.ts_iter = (Timestamp(t)
                        for t in itertools.count(int(time.time())))
        self.rx_ip = '127.0.0.1'
        sock = listen_zero()
        self.rx_server = eventlet.spawn(
            eventlet.wsgi.server, sock, self.rx_controller, self.rx_logger)
        self.rx_port = sock.getsockname()[1]
        self.rx_node = {'replication_ip': self.rx_ip,
                        'replication_port': self.rx_port,
                        'device': self.device}
        self.obj_data = {}  # maps obj path -> obj data

    def tearDown(self):
        self.rx_server.kill()
        super(TestBaseSsync, self).tearDown()

    def make_connect_wrapper(self, sender):
        """
        Make a wrapper function for the ssync_sender.Sender.connect() method
        that will in turn wrap the HTTConnection.send() and the
        Sender.readline() so that ssync protocol messages can be captured.
        """
        orig_connect = sender.connect
        trace = dict(messages=[])

        def add_trace(type, msg):
            # record a protocol event for later analysis
            if msg.strip():
                trace['messages'].append((type, msg.strip()))

        def make_send_wrapper(send):
            def wrapped_send(msg):
                _msg = msg.split('\r\n', 1)[1]
                _msg = _msg.rsplit('\r\n', 1)[0]
                add_trace('tx', _msg)
                send(msg)
            return wrapped_send

        def make_readline_wrapper(readline):
            def wrapped_readline(size=1024):
                data = readline(size=size)
                add_trace('rx', data)
                bytes_read = trace.setdefault('readline_bytes', 0)
                trace['readline_bytes'] = bytes_read + len(data)
                return data
            return wrapped_readline

        def wrapped_connect():
            connection, response = orig_connect()
            connection.send = make_send_wrapper(
                connection.send)
            response.readline = make_readline_wrapper(response.readline)
            return connection, response
        return wrapped_connect, trace

    def _get_object_data(self, path, **kwargs):
        # return data for given path
        if path not in self.obj_data:
            self.obj_data[path] = '%s___data' % path
        return self.obj_data[path]

    def _create_ondisk_files(self, df_mgr, obj_name, policy, timestamp,
                             frag_indexes=None, commit=True):
        frag_indexes = frag_indexes or [None]
        metadata = {'Content-Type': 'plain/text'}
        diskfiles = []
        for frag_index in frag_indexes:
            object_data = self._get_object_data('/a/c/%s' % obj_name,
                                                frag_index=frag_index)
            if policy.policy_type == EC_POLICY:
                metadata['X-Object-Sysmeta-Ec-Frag-Index'] = str(frag_index)
            df = self._make_diskfile(
                device=self.device, partition=self.partition, account='a',
                container='c', obj=obj_name, body=object_data,
                extra_metadata=metadata, timestamp=timestamp, policy=policy,
                frag_index=frag_index, df_mgr=df_mgr, commit=commit)
            diskfiles.append(df)
        return diskfiles

    def _open_tx_diskfile(self, obj_name, policy, frag_index=None):
        df_mgr = self.daemon._df_router[policy]
        df = df_mgr.get_diskfile(
            self.device, self.partition, account='a', container='c',
            obj=obj_name, policy=policy, frag_index=frag_index)
        df.open()
        return df

    def _open_rx_diskfile(self, obj_name, policy, frag_index=None):
        df = self.rx_controller.get_diskfile(
            self.device, self.partition, 'a', 'c', obj_name, policy=policy,
            frag_index=frag_index, open_expired=True)
        df.open()
        return df

    def _verify_diskfile_sync(self, tx_df, rx_df, frag_index, same_etag=False):
        # verify that diskfiles' metadata match
        # sanity check, they are not the same ondisk files!
        self.assertNotEqual(tx_df._datadir, rx_df._datadir)
        rx_metadata = dict(rx_df.get_metadata())
        for k, v in tx_df.get_metadata().items():
            if k == 'X-Object-Sysmeta-Ec-Frag-Index':
                # if tx_df had a frag_index then rx_df should also have one
                self.assertTrue(k in rx_metadata)
                self.assertEqual(frag_index, int(rx_metadata.pop(k)))
            elif k == 'ETag' and not same_etag:
                self.assertNotEqual(v, rx_metadata.pop(k, None))
                continue
            else:
                actual = rx_metadata.pop(k)
                self.assertEqual(v, actual, 'Expected %r but got %r for %s' %
                                 (v, actual, k))
        self.assertFalse(rx_metadata)
        expected_body = self._get_object_data(tx_df._name,
                                              frag_index=frag_index)
        actual_body = ''.join([chunk for chunk in rx_df.reader()])
        self.assertEqual(expected_body, actual_body)

    def _analyze_trace(self, trace):
        """
        Parse protocol trace captured by fake connection, making some
        assertions along the way, and return results  as a dict of form:
        results = {'tx_missing': <list of messages>,
                   'rx_missing': <list of messages>,
                   'tx_updates': <list of subreqs>,
                   'rx_updates': <list of messages>}

        Each subreq is a dict with keys: 'method', 'path', 'headers', 'body'
        """
        def tx_missing(results, line):
            self.assertEqual('tx', line[0])
            results['tx_missing'].append(line[1])

        def rx_missing(results, line):
            self.assertEqual('rx', line[0])
            parts = line[1].split('\r\n')
            for part in parts:
                results['rx_missing'].append(part)

        def tx_updates(results, line):
            self.assertEqual('tx', line[0])
            subrequests = results['tx_updates']
            if line[1].startswith(('PUT', 'DELETE', 'POST')):
                parts = line[1].split('\r\n')
                method, path = parts[0].split()
                subreq = {'method': method, 'path': path, 'req': line[1],
                          'headers': parts[1:]}
                subrequests.append(subreq)
            else:
                self.assertTrue(subrequests)
                body = (subrequests[-1]).setdefault('body', '')
                body += line[1]
                subrequests[-1]['body'] = body

        def rx_updates(results, line):
            self.assertEqual('rx', line[0])
            results.setdefault['rx_updates'].append(line[1])

        def unexpected(results, line):
            results.setdefault('unexpected', []).append(line)

        # each trace line is a tuple of ([tx|rx], msg)
        handshakes = iter([(('tx', ':MISSING_CHECK: START'), tx_missing),
                           (('tx', ':MISSING_CHECK: END'), unexpected),
                           (('rx', ':MISSING_CHECK: START'), rx_missing),
                           (('rx', ':MISSING_CHECK: END'), unexpected),
                           (('tx', ':UPDATES: START'), tx_updates),
                           (('tx', ':UPDATES: END'), unexpected),
                           (('rx', ':UPDATES: START'), rx_updates),
                           (('rx', ':UPDATES: END'), unexpected)])
        expect_handshake = next(handshakes)
        phases = ('tx_missing', 'rx_missing', 'tx_updates', 'rx_updates')
        results = dict((k, []) for k in phases)
        handler = unexpected
        lines = list(trace.get('messages', []))
        lines.reverse()
        while lines:
            line = lines.pop()
            if line == expect_handshake[0]:
                handler = expect_handshake[1]
                try:
                    expect_handshake = next(handshakes)
                except StopIteration:
                    # should be the last line
                    self.assertFalse(
                        lines, 'Unexpected trailing lines %s' % lines)
                continue
            handler(results, line)

        try:
            # check all handshakes occurred
            missed = next(handshakes)
            self.fail('Handshake %s not found' % str(missed[0]))
        except StopIteration:
            pass
        # check no message outside of a phase
        self.assertFalse(results.get('unexpected'),
                         'Message outside of a phase: %s' % results.get(None))
        return results

    def _verify_ondisk_files(self, tx_objs, policy, tx_frag_index=None,
                             rx_frag_index=None):
        """
        Verify tx and rx files that should be in sync.
        :param tx_objs: sender diskfiles
        :param policy: storage policy instance
        :param tx_frag_index: the fragment index of tx diskfiles that should
                              have been used as a source for sync'ing
        :param rx_frag_index: the fragment index of expected rx diskfiles
        """
        for o_name, diskfiles in tx_objs.items():
            for tx_df in diskfiles:
                # check tx file still intact - ssync does not do any cleanup!
                tx_df.open()
                if tx_frag_index is None or tx_df._frag_index == tx_frag_index:
                    # this diskfile should have been sync'd,
                    # check rx file is ok
                    rx_df = self._open_rx_diskfile(
                        o_name, policy, rx_frag_index)
                    # for EC revert job or replication etags should match
                    match_etag = (tx_frag_index == rx_frag_index)
                    self._verify_diskfile_sync(
                        tx_df, rx_df, rx_frag_index, match_etag)
                else:
                    # this diskfile should not have been sync'd,
                    # check no rx file,
                    self.assertRaises(DiskFileNotExist, self._open_rx_diskfile,
                                      o_name, policy,
                                      frag_index=tx_df._frag_index)

    def _verify_tombstones(self, tx_objs, policy):
        # verify tx and rx tombstones that should be in sync
        for o_name, diskfiles in tx_objs.items():
            try:
                self._open_tx_diskfile(o_name, policy)
                self.fail('DiskFileDeleted expected')
            except DiskFileDeleted as exc:
                tx_delete_time = exc.timestamp
            try:
                self._open_rx_diskfile(o_name, policy)
                self.fail('DiskFileDeleted expected')
            except DiskFileDeleted as exc:
                rx_delete_time = exc.timestamp
            self.assertEqual(tx_delete_time, rx_delete_time)


@patch_policies(with_ec_default=True)
class TestBaseSsyncEC(TestBaseSsync):
    def setUp(self):
        super(TestBaseSsyncEC, self).setUp()
        self.policy = POLICIES.default
        self.logger = debug_logger('test-ssync-sender')
        self.daemon = ObjectReconstructor(self.daemon_conf, self.logger)
        self.rx_node['backend_index'] = 0

    def _get_object_data(self, path, frag_index=None, **kwargs):
        # return a frag archive for given object name and frag index.
        # for EC policies obj_data maps obj path -> list of frag archives
        if path not in self.obj_data:
            # make unique frag archives for each object name
            data = path * 2 * (self.policy.ec_ndata + self.policy.ec_nparity)
            self.obj_data[path] = encode_frag_archive_bodies(
                self.policy, data)
        return self.obj_data[path][frag_index]


class TestSsyncEC(TestBaseSsyncEC):
    def test_handoff_fragment_revert(self):
        # test that a sync_revert type job does send the correct frag archives
        # to the receiver
        policy = POLICIES.default
        rx_node_index = 0
        tx_node_index = 1
        # for a revert job we iterate over frag index that belongs on
        # remote node
        frag_index = rx_node_index

        # create sender side diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_tombstones = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        # o1 has primary and handoff fragment archives
        t1 = next(self.ts_iter)
        tx_objs['o1'] = self._create_ondisk_files(
            tx_df_mgr, 'o1', policy, t1, (rx_node_index, tx_node_index))
        # o2 only has primary
        t2 = next(self.ts_iter)
        tx_objs['o2'] = self._create_ondisk_files(
            tx_df_mgr, 'o2', policy, t2, (tx_node_index,))
        # o3 only has handoff, rx has other frag index
        t3 = next(self.ts_iter)
        tx_objs['o3'] = self._create_ondisk_files(
            tx_df_mgr, 'o3', policy, t3, (rx_node_index,))
        rx_objs['o3'] = self._create_ondisk_files(
            rx_df_mgr, 'o3', policy, t3, (13,))
        # o4 primary and handoff fragment archives on tx, handoff in sync on rx
        t4 = next(self.ts_iter)
        tx_objs['o4'] = self._create_ondisk_files(
            tx_df_mgr, 'o4', policy, t4, (tx_node_index, rx_node_index,))
        rx_objs['o4'] = self._create_ondisk_files(
            rx_df_mgr, 'o4', policy, t4, (rx_node_index,))
        # o5 is a tombstone, missing on receiver
        t5 = next(self.ts_iter)
        tx_tombstones['o5'] = self._create_ondisk_files(
            tx_df_mgr, 'o5', policy, t5, (tx_node_index,))
        tx_tombstones['o5'][0].delete(t5)

        suffixes = set()
        for diskfiles in list(tx_objs.values()) + list(tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy,
               'frag_index': frag_index}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        sender()

        # verify protocol
        results = self._analyze_trace(trace)
        # sender has handoff frags for o1, o3 and o4 and ts for o5
        self.assertEqual(4, len(results['tx_missing']))
        # receiver is missing frags for o1, o3 and ts for o5
        self.assertEqual(3, len(results['rx_missing']))
        self.assertEqual(3, len(results['tx_updates']))
        self.assertFalse(results['rx_updates'])
        sync_paths = []
        for subreq in results.get('tx_updates'):
            if subreq.get('method') == 'PUT':
                self.assertTrue(
                    'X-Object-Sysmeta-Ec-Frag-Index: %s' % rx_node_index
                    in subreq.get('headers'))
                expected_body = self._get_object_data(subreq['path'],
                                                      rx_node_index)
                self.assertEqual(expected_body, subreq['body'])
            elif subreq.get('method') == 'DELETE':
                self.assertEqual('/a/c/o5', subreq['path'])
            sync_paths.append(subreq.get('path'))
        self.assertEqual(['/a/c/o1', '/a/c/o3', '/a/c/o5'], sorted(sync_paths))

        # verify on disk files...
        self._verify_ondisk_files(
            tx_objs, policy, frag_index, rx_node_index)
        self._verify_tombstones(tx_tombstones, policy)

    def test_handoff_fragment_only_missing_durable_state(self):
        # test that a sync_revert type job does not PUT when the rx is only
        # missing durable state
        policy = POLICIES.default
        rx_node_index = frag_index = 0
        tx_node_index = 1

        # create sender side diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]

        expected_subreqs = defaultdict(list)

        # o1 in sync on rx but rx missing durable state - no PUT required
        t1a = next(self.ts_iter)  # older durable rx .data
        t1b = next(self.ts_iter)  # rx .meta
        t1c = next(self.ts_iter)  # durable tx .data, non-durable rx .data
        obj_name = 'o1'
        tx_objs[obj_name] = self._create_ondisk_files(
            tx_df_mgr, obj_name, policy, t1c, (tx_node_index, rx_node_index,))
        rx_objs[obj_name] = self._create_ondisk_files(
            rx_df_mgr, obj_name, policy, t1a, (rx_node_index,))
        metadata = {'X-Timestamp': t1b.internal}
        rx_objs[obj_name][0].write_metadata(metadata)
        rx_objs[obj_name] = self._create_ondisk_files(
            rx_df_mgr, obj_name, policy, t1c, (rx_node_index, 9), commit=False)

        # o2 on rx has wrong frag_indexes and is non-durable - PUT required
        t2 = next(self.ts_iter)
        obj_name = 'o2'
        tx_objs[obj_name] = self._create_ondisk_files(
            tx_df_mgr, obj_name, policy, t2, (tx_node_index, rx_node_index,))
        rx_objs[obj_name] = self._create_ondisk_files(
            rx_df_mgr, obj_name, policy, t2, (12, 13), commit=False)
        expected_subreqs['PUT'].append(obj_name)

        # o3 on rx has frag at other time and non-durable - PUT required
        t3 = next(self.ts_iter)
        obj_name = 'o3'
        tx_objs[obj_name] = self._create_ondisk_files(
            tx_df_mgr, obj_name, policy, t3, (tx_node_index, rx_node_index,))
        t3b = next(self.ts_iter)
        rx_objs[obj_name] = self._create_ondisk_files(
            rx_df_mgr, obj_name, policy, t3b, (rx_node_index,), commit=False)
        expected_subreqs['PUT'].append(obj_name)

        # o4 on rx has a newer tombstone and even newer frags - no PUT required
        t4 = next(self.ts_iter)
        obj_name = 'o4'
        tx_objs[obj_name] = self._create_ondisk_files(
            tx_df_mgr, obj_name, policy, t4, (tx_node_index, rx_node_index,))
        rx_objs[obj_name] = self._create_ondisk_files(
            rx_df_mgr, obj_name, policy, t4, (rx_node_index,))
        t4b = next(self.ts_iter)
        rx_objs[obj_name][0].delete(t4b)
        t4c = next(self.ts_iter)
        rx_objs[obj_name] = self._create_ondisk_files(
            rx_df_mgr, obj_name, policy, t4c, (rx_node_index,), commit=False)

        suffixes = set()
        for diskfiles in tx_objs.values():
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy,
               'frag_index': frag_index}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        sender()

        # verify protocol
        results = self._analyze_trace(trace)
        self.assertEqual(4, len(results['tx_missing']))
        self.assertEqual(2, len(results['rx_missing']))
        self.assertEqual(2, len(results['tx_updates']))
        self.assertFalse(results['rx_updates'])
        for subreq in results.get('tx_updates'):
            obj = subreq['path'].split('/')[3]
            method = subreq['method']
            self.assertTrue(obj in expected_subreqs[method],
                            'Unexpected %s subreq for object %s, expected %s'
                            % (method, obj, expected_subreqs[method]))
            expected_subreqs[method].remove(obj)
            if method == 'PUT':
                expected_body = self._get_object_data(
                    subreq['path'], frag_index=rx_node_index)
                self.assertEqual(expected_body, subreq['body'])
        # verify all expected subreqs consumed
        for _method, expected in expected_subreqs.items():
            self.assertFalse(expected)

        # verify on disk files...
        tx_objs.pop('o4')  # o4 should not have been sync'd
        self._verify_ondisk_files(
            tx_objs, policy, frag_index, rx_node_index)

    def test_fragment_sync(self):
        # check that a sync_only type job does call reconstructor to build a
        # diskfile to send, and continues making progress despite an error
        # when building one diskfile
        policy = POLICIES.default
        rx_node_index = 0
        tx_node_index = 1
        # for a sync job we iterate over frag index that belongs on local node
        frag_index = tx_node_index

        # create sender side diskfiles...
        tx_objs = {}
        tx_tombstones = {}
        rx_objs = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        # o1 only has primary
        t1 = next(self.ts_iter)
        tx_objs['o1'] = self._create_ondisk_files(
            tx_df_mgr, 'o1', policy, t1, (tx_node_index,))
        # o2 only has primary
        t2 = next(self.ts_iter)
        tx_objs['o2'] = self._create_ondisk_files(
            tx_df_mgr, 'o2', policy, t2, (tx_node_index,))
        # o3 only has primary
        t3 = next(self.ts_iter)
        tx_objs['o3'] = self._create_ondisk_files(
            tx_df_mgr, 'o3', policy, t3, (tx_node_index,))
        # o4 primary fragment archives on tx, handoff in sync on rx
        t4 = next(self.ts_iter)
        tx_objs['o4'] = self._create_ondisk_files(
            tx_df_mgr, 'o4', policy, t4, (tx_node_index,))
        rx_objs['o4'] = self._create_ondisk_files(
            rx_df_mgr, 'o4', policy, t4, (rx_node_index,))
        # o5 is a tombstone, missing on receiver
        t5 = next(self.ts_iter)
        tx_tombstones['o5'] = self._create_ondisk_files(
            tx_df_mgr, 'o5', policy, t5, (tx_node_index,))
        tx_tombstones['o5'][0].delete(t5)

        suffixes = set()
        for diskfiles in list(tx_objs.values()) + list(tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        reconstruct_fa_calls = []

        def fake_reconstruct_fa(job, node, metadata):
            reconstruct_fa_calls.append((job, node, policy, metadata))
            if len(reconstruct_fa_calls) == 2:
                # simulate second reconstruct failing
                raise DiskFileError
            content = self._get_object_data(metadata['name'],
                                            frag_index=rx_node_index)
            return RebuildingECDiskFileStream(
                metadata, rx_node_index, iter([content]))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy,
               'frag_index': frag_index,
               'sync_diskfile_builder': fake_reconstruct_fa}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        sender()

        # verify protocol
        results = self._analyze_trace(trace)
        # sender has primary for o1, o2 and o3, o4 and ts for o5
        self.assertEqual(5, len(results['tx_missing']))
        # receiver is missing o1, o2 and o3 and ts for o5
        self.assertEqual(4, len(results['rx_missing']))
        # sender can only construct 2 out of 3 missing frags
        self.assertEqual(3, len(results['tx_updates']))
        self.assertEqual(3, len(reconstruct_fa_calls))
        self.assertFalse(results['rx_updates'])
        actual_sync_paths = []
        for subreq in results.get('tx_updates'):
            if subreq.get('method') == 'PUT':
                self.assertTrue(
                    'X-Object-Sysmeta-Ec-Frag-Index: %s' % rx_node_index
                    in subreq.get('headers'))
                expected_body = self._get_object_data(
                    subreq['path'], frag_index=rx_node_index)
                self.assertEqual(expected_body, subreq['body'])
            elif subreq.get('method') == 'DELETE':
                self.assertEqual('/a/c/o5', subreq['path'])
            actual_sync_paths.append(subreq.get('path'))

        # remove the failed df from expected synced df's
        expect_sync_paths = ['/a/c/o1', '/a/c/o2', '/a/c/o3', '/a/c/o5']
        failed_path = reconstruct_fa_calls[1][3]['name']
        expect_sync_paths.remove(failed_path)
        failed_obj = None
        for obj, diskfiles in tx_objs.items():
            if diskfiles[0]._name == failed_path:
                failed_obj = obj
        # sanity check
        self.assertTrue(tx_objs.pop(failed_obj))

        # verify on disk files...
        self.assertEqual(sorted(expect_sync_paths), sorted(actual_sync_paths))
        self._verify_ondisk_files(
            tx_objs, policy, frag_index, rx_node_index)
        self._verify_tombstones(tx_tombstones, policy)

    def test_send_with_frag_index_none(self):
        policy = POLICIES.default
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        # create an ec fragment on the remote node
        ts1 = next(self.ts_iter)
        remote_df = self._create_ondisk_files(
            rx_df_mgr, 'o', policy, ts1, (3,))[0]

        # create a tombstone on the local node
        df = self._create_ondisk_files(
            tx_df_mgr, 'o', policy, ts1, (3,))[0]
        suffix = os.path.basename(os.path.dirname(df._datadir))
        ts2 = next(self.ts_iter)
        df.delete(ts2)
        # a reconstructor revert job with only tombstones will have frag_index
        # explicitly set to None
        job = {
            'frag_index': None,
            'partition': self.partition,
            'policy': policy,
            'device': self.device,
        }
        sender = ssync_sender.Sender(
            self.daemon, self.rx_node, job, [suffix])
        success, _ = sender()
        self.assertTrue(success)
        try:
            remote_df.read_metadata()
        except DiskFileDeleted as e:
            self.assertEqual(e.timestamp, ts2)
        else:
            self.fail('Successfully opened remote DiskFile')

    def test_send_invalid_frag_index(self):
        policy = POLICIES.default
        job = {'frag_index': 'No one cares',
               'device': self.device,
               'partition': self.partition,
               'policy': policy}
        self.rx_node['backend_index'] = 'Not a number'
        sender = ssync_sender.Sender(
            self.daemon, self.rx_node, job, ['abc'])
        success, _ = sender()
        self.assertFalse(success)
        error_log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_log_lines))
        error_msg = error_log_lines[0]
        self.assertIn("Expected status 200; got 400", error_msg)
        self.assertIn("Invalid X-Backend-Ssync-Frag-Index 'Not a number'",
                      error_msg)

    def test_revert_job_with_legacy_durable(self):
        # test a sync_revert type job using a sender object with a legacy
        # durable file, that will create a receiver object with durable data
        policy = POLICIES.default
        rx_node_index = 0
        # for a revert job we iterate over frag index that belongs on
        # remote node
        frag_index = rx_node_index

        # create non durable tx obj by not committing, then create a legacy
        # .durable file
        tx_objs = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        t1 = next(self.ts_iter)
        tx_objs['o1'] = self._create_ondisk_files(
            tx_df_mgr, 'o1', policy, t1, (rx_node_index,), commit=False)
        tx_datadir = tx_objs['o1'][0]._datadir
        durable_file = os.path.join(tx_datadir, t1.internal + '.durable')
        with open(durable_file, 'wb'):
            pass
        self.assertEqual(2, len(os.listdir(tx_datadir)))  # sanity check

        suffixes = [os.path.basename(os.path.dirname(tx_datadir))]

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy,
               'frag_index': frag_index}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        sender()

        # verify protocol
        results = self._analyze_trace(trace)
        self.assertEqual(1, len(results['tx_missing']))
        self.assertEqual(1, len(results['rx_missing']))
        self.assertEqual(1, len(results['tx_updates']))
        self.assertFalse(results['rx_updates'])

        # sanity check - rx diskfile is durable
        expected_rx_file = '%s#%s#d.data' % (t1.internal, rx_node_index)
        rx_df = self._open_rx_diskfile('o1', policy, rx_node_index)
        self.assertEqual([expected_rx_file], os.listdir(rx_df._datadir))

        # verify on disk files...
        self._verify_ondisk_files(
            tx_objs, policy, frag_index, rx_node_index)

        # verify that tx and rx both generate the same suffix hashes...
        tx_hashes = tx_df_mgr.get_hashes(
            self.device, self.partition, suffixes, policy)
        rx_hashes = rx_df_mgr.get_hashes(
            self.device, self.partition, suffixes, policy)
        self.assertEqual(suffixes, tx_hashes.keys())  # sanity
        self.assertEqual(tx_hashes, rx_hashes)

        # sanity check - run ssync again and expect no sync activity
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        sender.connect, trace = self.make_connect_wrapper(sender)
        sender()
        results = self._analyze_trace(trace)
        self.assertEqual(1, len(results['tx_missing']))
        self.assertFalse(results['rx_missing'])
        self.assertFalse(results['tx_updates'])
        self.assertFalse(results['rx_updates'])


class FakeResponse(object):
    def __init__(self, frag_index, obj_data, length=None):
        self.headers = {
            'X-Object-Sysmeta-Ec-Frag-Index': str(frag_index),
            'X-Object-Sysmeta-Ec-Etag': 'the etag',
            'X-Backend-Timestamp': '1234567890.12345'
        }
        self.frag_index = frag_index
        self.obj_data = obj_data
        self.data = ''
        self.length = length

    def init(self, path):
        if isinstance(self.obj_data, Exception):
            self.data = self.obj_data
        else:
            self.data = self.obj_data[path][self.frag_index]

    def getheaders(self):
        return self.headers

    def read(self, length):
        if isinstance(self.data, Exception):
            raise self.data
        val = self.data
        self.data = ''
        return val if self.length is None else val[:self.length]


class TestSsyncECReconstructorSyncJob(TestBaseSsyncEC):
    def setUp(self):
        super(TestSsyncECReconstructorSyncJob, self).setUp()
        self.rx_node_index = 0
        self.tx_node_index = 1

        # create sender side diskfiles...
        self.tx_objs = {}
        tx_df_mgr = self.daemon._df_router[self.policy]
        t1 = next(self.ts_iter)
        self.tx_objs['o1'] = self._create_ondisk_files(
            tx_df_mgr, 'o1', self.policy, t1, (self.tx_node_index,))
        t2 = next(self.ts_iter)
        self.tx_objs['o2'] = self._create_ondisk_files(
            tx_df_mgr, 'o2', self.policy, t2, (self.tx_node_index,))

        self.suffixes = set()
        for diskfiles in list(self.tx_objs.values()):
            for df in diskfiles:
                self.suffixes.add(
                    os.path.basename(os.path.dirname(df._datadir)))

        self.job_node = dict(self.rx_node)
        self.job_node['id'] = 0

        self.frag_length = int(
            self.tx_objs['o1'][0].get_metadata()['Content-Length'])

    def _test_reconstructor_sync_job(self, frag_responses):
        # Helper method to mock reconstructor to consume given lists of fake
        # responses while reconstructing a fragment for a sync type job. The
        # tests verify that when the reconstructed fragment iter fails in some
        # way then ssync does not mistakenly create fragments on the receiving
        # node which have incorrect data.
        # See https://bugs.launchpad.net/swift/+bug/1631144

        # frag_responses is a list of two lists of responses to each
        # reconstructor GET request for a fragment archive. The two items in
        # the outer list are lists of responses for each of the two fragments
        # to be reconstructed. Items in the inner lists are responses for each
        # of the other fragments fetched during the reconstructor rebuild.
        path_to_responses = {}
        fake_get_response_calls = []

        def fake_get_response(recon, node, part, path, headers, policy):
            # select a list of fake responses for this path and return the next
            # from the list
            if path not in path_to_responses:
                path_to_responses[path] = frag_responses.pop(0)
            response = path_to_responses[path].pop()
            # the frag_responses list is in ssync task order, we only know the
            # path when consuming the responses so initialise the path in the
            # response now
            if response:
                response.init(path)
            fake_get_response_calls.append(path)
            return response

        def fake_get_part_nodes(part):
            # the reconstructor will try to remove the receiver node from the
            # object ring part nodes, but the fake node we created for our
            # receiver is not actually in the ring part nodes, so append it
            # here simply so that the reconstructor does not fail to remove it.
            return (self.policy.object_ring._get_part_nodes(part) +
                    [self.job_node])

        with mock.patch(
                'swift.obj.reconstructor.ObjectReconstructor._get_response',
                fake_get_response), \
                mock.patch.object(
                    self.policy.object_ring, 'get_part_nodes',
                    fake_get_part_nodes):
            self.reconstructor = ObjectReconstructor(
                {}, logger=self.logger)
            job = {
                'device': self.device,
                'partition': self.partition,
                'policy': self.policy,
                'sync_diskfile_builder':
                    self.reconstructor.reconstruct_fa
            }
            sender = ssync_sender.Sender(
                self.daemon, self.job_node, job, self.suffixes)
            sender.connect, trace = self.make_connect_wrapper(sender)
            sender()
        return trace

    def test_sync_reconstructor_partial_rebuild(self):
        # First fragment to sync gets partial content from reconstructor.
        # Expect ssync job to exit early with no file written on receiver.
        frag_responses = [
            [FakeResponse(i, self.obj_data, length=-1)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)],
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)]]

        self._test_reconstructor_sync_job(frag_responses)
        msgs = []
        for obj_name in ('o1', 'o2'):
            try:
                df = self._open_rx_diskfile(
                    obj_name, self.policy, self.rx_node_index)
                msgs.append('Unexpected rx diskfile for %r with content %r' %
                            (obj_name, ''.join([d for d in df.reader()])))
            except DiskFileNotExist:
                pass  # expected outcome
        if msgs:
            self.fail('Failed with:\n%s' % '\n'.join(msgs))
        log_lines = self.logger.get_lines_for_level('error')
        self.assertIn('Sent data length does not match content-length',
                      log_lines[0])
        self.assertFalse(log_lines[1:])
        # trampoline for the receiver to write a log
        eventlet.sleep(0)
        log_lines = self.rx_logger.get_lines_for_level('warning')
        self.assertIn('ssync subrequest failed with 499',
                      log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertFalse(self.rx_logger.get_lines_for_level('error'))

    def test_sync_reconstructor_no_rebuilt_content(self):
        # First fragment to sync gets no content in any response to
        # reconstructor. Expect ssync job to exit early with no file written on
        # receiver.
        frag_responses = [
            [FakeResponse(i, self.obj_data, length=0)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)],
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)]]

        self._test_reconstructor_sync_job(frag_responses)
        msgs = []
        for obj_name in ('o1', 'o2'):
            try:
                df = self._open_rx_diskfile(
                    obj_name, self.policy, self.rx_node_index)
                msgs.append('Unexpected rx diskfile for %r with content %r' %
                            (obj_name, ''.join([d for d in df.reader()])))
            except DiskFileNotExist:
                pass  # expected outcome
        if msgs:
            self.fail('Failed with:\n%s' % '\n'.join(msgs))
        log_lines = self.logger.get_lines_for_level('error')
        self.assertIn('Sent data length does not match content-length',
                      log_lines[0])
        self.assertFalse(log_lines[1:])
        # trampoline for the receiver to write a log
        eventlet.sleep(0)
        log_lines = self.rx_logger.get_lines_for_level('warning')
        self.assertIn('ssync subrequest failed with 499',
                      log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertFalse(self.rx_logger.get_lines_for_level('error'))

    def test_sync_reconstructor_exception_during_rebuild(self):
        # First fragment to sync has some reconstructor get responses raise
        # exception while rebuilding. Expect ssync job to exit early with no
        # files written on receiver.
        frag_responses = [
            # ec_ndata responses are ok, but one of these will be ignored as
            # it is for the frag index being rebuilt
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata)] +
            # ec_nparity responses will raise an Exception - at least one of
            # these will be used during rebuild
            [FakeResponse(i, Exception('raised in response read method'))
             for i in range(self.policy.ec_ndata,
                            self.policy.ec_ndata + self.policy.ec_nparity)],
            # second set of response are all good
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)]]

        with quiet_eventlet_exceptions():
            self._test_reconstructor_sync_job(frag_responses)

        msgs = []
        for obj_name in ('o1', 'o2'):
            try:
                df = self._open_rx_diskfile(
                    obj_name, self.policy, self.rx_node_index)
                msgs.append('Unexpected rx diskfile for %r with content %r' %
                            (obj_name, ''.join([d for d in df.reader()])))
            except DiskFileNotExist:
                pass  # expected outcome
        if msgs:
            self.fail('Failed with:\n%s' % '\n'.join(msgs))

        log_lines = self.logger.get_lines_for_level('error')
        self.assertIn('Error trying to rebuild', log_lines[0])
        self.assertIn('Sent data length does not match content-length',
                      log_lines[1])
        self.assertFalse(log_lines[2:])
        # trampoline for the receiver to write a log
        eventlet.sleep(0)
        log_lines = self.rx_logger.get_lines_for_level('warning')
        self.assertIn('ssync subrequest failed with 499',
                      log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertFalse(self.rx_logger.get_lines_for_level('error'))

    def test_sync_reconstructor_no_responses(self):
        # First fragment to sync gets no responses for reconstructor to rebuild
        # with, nothing is sent to receiver so expect to skip that fragment and
        # continue with second.
        frag_responses = [
            [None
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)],
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)]]

        trace = self._test_reconstructor_sync_job(frag_responses)
        results = self._analyze_trace(trace)
        self.assertEqual(2, len(results['tx_missing']))
        self.assertEqual(2, len(results['rx_missing']))
        self.assertEqual(1, len(results['tx_updates']))
        self.assertFalse(results['rx_updates'])
        self.assertEqual('PUT', results['tx_updates'][0].get('method'))
        synced_obj_path = results['tx_updates'][0].get('path')
        synced_obj_name = synced_obj_path[-2:]

        msgs = []
        obj_name = synced_obj_name
        try:
            df = self._open_rx_diskfile(
                obj_name, self.policy, self.rx_node_index)
            self.assertEqual(
                self._get_object_data(synced_obj_path,
                                      frag_index=self.rx_node_index),
                ''.join([d for d in df.reader()]))
        except DiskFileNotExist:
            msgs.append('Missing rx diskfile for %r' % obj_name)

        obj_names = list(self.tx_objs)
        obj_names.remove(synced_obj_name)
        obj_name = obj_names[0]
        try:
            df = self._open_rx_diskfile(
                obj_name, self.policy, self.rx_node_index)
            msgs.append('Unexpected rx diskfile for %r with content %r' %
                        (obj_name, ''.join([d for d in df.reader()])))
        except DiskFileNotExist:
            pass  # expected outcome
        if msgs:
            self.fail('Failed with:\n%s' % '\n'.join(msgs))
        log_lines = self.logger.get_lines_for_level('error')
        self.assertIn('Unable to get enough responses', log_lines[0])
        # trampoline for the receiver to write a log
        eventlet.sleep(0)
        self.assertFalse(self.rx_logger.get_lines_for_level('warning'))
        self.assertFalse(self.rx_logger.get_lines_for_level('error'))

    def test_sync_reconstructor_rebuild_ok(self):
        # Sanity test for this class of tests. Both fragments get a full
        # complement of responses and rebuild correctly.
        frag_responses = [
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)],
            [FakeResponse(i, self.obj_data)
             for i in range(self.policy.ec_ndata + self.policy.ec_nparity)]]

        trace = self._test_reconstructor_sync_job(frag_responses)
        results = self._analyze_trace(trace)
        self.assertEqual(2, len(results['tx_missing']))
        self.assertEqual(2, len(results['rx_missing']))
        self.assertEqual(2, len(results['tx_updates']))
        self.assertFalse(results['rx_updates'])
        msgs = []
        for obj_name in self.tx_objs:
            try:
                df = self._open_rx_diskfile(
                    obj_name, self.policy, self.rx_node_index)
                self.assertEqual(
                    self._get_object_data(df._name,
                                          frag_index=self.rx_node_index),
                    ''.join([d for d in df.reader()]))
            except DiskFileNotExist:
                msgs.append('Missing rx diskfile for %r' % obj_name)
        if msgs:
            self.fail('Failed with:\n%s' % '\n'.join(msgs))
        self.assertFalse(self.logger.get_lines_for_level('error'))
        self.assertFalse(
            self.logger.get_lines_for_level('error'))
        # trampoline for the receiver to write a log
        eventlet.sleep(0)
        self.assertFalse(self.rx_logger.get_lines_for_level('warning'))
        self.assertFalse(self.rx_logger.get_lines_for_level('error'))


@patch_policies
class TestSsyncReplication(TestBaseSsync):
    def setUp(self):
        super(TestSsyncReplication, self).setUp()
        self.logger = debug_logger('test-ssync-sender')
        self.daemon = ObjectReplicator(self.daemon_conf, self.logger)

    def test_sync(self):
        policy = POLICIES.default

        # create sender side diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_tombstones = {}
        rx_tombstones = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        # o1 and o2 are on tx only
        t1 = next(self.ts_iter)
        tx_objs['o1'] = self._create_ondisk_files(tx_df_mgr, 'o1', policy, t1)
        t2 = next(self.ts_iter)
        tx_objs['o2'] = self._create_ondisk_files(tx_df_mgr, 'o2', policy, t2)
        # o3 is on tx and older copy on rx
        t3a = next(self.ts_iter)
        rx_objs['o3'] = self._create_ondisk_files(rx_df_mgr, 'o3', policy, t3a)
        t3b = next(self.ts_iter)
        tx_objs['o3'] = self._create_ondisk_files(tx_df_mgr, 'o3', policy, t3b)
        # o4 in sync on rx and tx
        t4 = next(self.ts_iter)
        tx_objs['o4'] = self._create_ondisk_files(tx_df_mgr, 'o4', policy, t4)
        rx_objs['o4'] = self._create_ondisk_files(rx_df_mgr, 'o4', policy, t4)
        # o5 is a tombstone, missing on receiver
        t5 = next(self.ts_iter)
        tx_tombstones['o5'] = self._create_ondisk_files(
            tx_df_mgr, 'o5', policy, t5)
        tx_tombstones['o5'][0].delete(t5)
        # o6 is a tombstone, in sync on tx and rx
        t6 = next(self.ts_iter)
        tx_tombstones['o6'] = self._create_ondisk_files(
            tx_df_mgr, 'o6', policy, t6)
        tx_tombstones['o6'][0].delete(t6)
        rx_tombstones['o6'] = self._create_ondisk_files(
            rx_df_mgr, 'o6', policy, t6)
        rx_tombstones['o6'][0].delete(t6)
        # o7 is a tombstone on tx, older data on rx
        t7a = next(self.ts_iter)
        rx_objs['o7'] = self._create_ondisk_files(rx_df_mgr, 'o7', policy, t7a)
        t7b = next(self.ts_iter)
        tx_tombstones['o7'] = self._create_ondisk_files(
            tx_df_mgr, 'o7', policy, t7b)
        tx_tombstones['o7'][0].delete(t7b)

        suffixes = set()
        for diskfiles in list(tx_objs.values()) + list(tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        success, in_sync_objs = sender()

        self.assertEqual(7, len(in_sync_objs))
        self.assertTrue(success)

        # verify protocol
        results = self._analyze_trace(trace)
        self.assertEqual(7, len(results['tx_missing']))
        self.assertEqual(5, len(results['rx_missing']))
        self.assertEqual(5, len(results['tx_updates']))
        self.assertFalse(results['rx_updates'])
        sync_paths = []
        for subreq in results.get('tx_updates'):
            if subreq.get('method') == 'PUT':
                self.assertTrue(
                    subreq['path'] in ('/a/c/o1', '/a/c/o2', '/a/c/o3'))
                expected_body = self._get_object_data(subreq['path'])
                self.assertEqual(expected_body, subreq['body'])
            elif subreq.get('method') == 'DELETE':
                self.assertTrue(subreq['path'] in ('/a/c/o5', '/a/c/o7'))
            sync_paths.append(subreq.get('path'))
        self.assertEqual(
            ['/a/c/o1', '/a/c/o2', '/a/c/o3', '/a/c/o5', '/a/c/o7'],
            sorted(sync_paths))

        # verify on disk files...
        self._verify_ondisk_files(tx_objs, policy)
        self._verify_tombstones(tx_tombstones, policy)

    def test_nothing_to_sync(self):
        job = {'device': self.device,
               'partition': self.partition,
               'policy': POLICIES.default}
        node = {'replication_ip': self.rx_ip,
                'replication_port': self.rx_port,
                'device': self.device,
                'index': 0}
        sender = ssync_sender.Sender(self.daemon, node, job, ['abc'])
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        result, in_sync_objs = sender()

        self.assertTrue(result)
        self.assertFalse(in_sync_objs)
        results = self._analyze_trace(trace)
        self.assertFalse(results['tx_missing'])
        self.assertFalse(results['rx_missing'])
        self.assertFalse(results['tx_updates'])
        self.assertFalse(results['rx_updates'])
        # Minimal receiver response as read by sender:
        #               2  <-- initial \r\n to start ssync exchange
        # +            23  <-- :MISSING CHECK START\r\n
        # +             2  <-- \r\n (minimal missing check response)
        # +            21  <-- :MISSING CHECK END\r\n
        # +            17  <-- :UPDATES START\r\n
        # +            15  <-- :UPDATES END\r\n
        #    TOTAL =   80
        self.assertEqual(80, trace.get('readline_bytes'))

    def test_meta_file_sync(self):
        policy = POLICIES.default

        # create diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_tombstones = {}
        rx_tombstones = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]

        expected_subreqs = defaultdict(list)

        # o1 on tx only with meta file
        t1 = next(self.ts_iter)
        tx_objs['o1'] = self._create_ondisk_files(tx_df_mgr, 'o1', policy, t1)
        t1_meta = next(self.ts_iter)
        metadata = {'X-Timestamp': t1_meta.internal,
                    'X-Object-Meta-Test': 'o1',
                    'X-Object-Sysmeta-Test': 'sys_o1'}
        tx_objs['o1'][0].write_metadata(metadata)
        expected_subreqs['PUT'].append('o1')
        expected_subreqs['POST'].append('o1')

        # o2 on tx with meta, on rx without meta
        t2 = next(self.ts_iter)
        tx_objs['o2'] = self._create_ondisk_files(tx_df_mgr, 'o2', policy, t2)
        t2_meta = next(self.ts_iter)
        metadata = {'X-Timestamp': t2_meta.internal,
                    'X-Object-Meta-Test': 'o2',
                    'X-Object-Sysmeta-Test': 'sys_o2'}
        tx_objs['o2'][0].write_metadata(metadata)
        rx_objs['o2'] = self._create_ondisk_files(rx_df_mgr, 'o2', policy, t2)
        expected_subreqs['POST'].append('o2')

        # o3 is on tx with meta, rx has newer data but no meta
        t3a = next(self.ts_iter)
        tx_objs['o3'] = self._create_ondisk_files(tx_df_mgr, 'o3', policy, t3a)
        t3b = next(self.ts_iter)
        rx_objs['o3'] = self._create_ondisk_files(rx_df_mgr, 'o3', policy, t3b)
        t3_meta = next(self.ts_iter)
        metadata = {'X-Timestamp': t3_meta.internal,
                    'X-Object-Meta-Test': 'o3',
                    'X-Object-Sysmeta-Test': 'sys_o3'}
        tx_objs['o3'][0].write_metadata(metadata)
        expected_subreqs['POST'].append('o3')

        # o4 is on tx with meta, rx has older data and up to date meta
        t4a = next(self.ts_iter)
        rx_objs['o4'] = self._create_ondisk_files(rx_df_mgr, 'o4', policy, t4a)
        t4b = next(self.ts_iter)
        tx_objs['o4'] = self._create_ondisk_files(tx_df_mgr, 'o4', policy, t4b)
        t4_meta = next(self.ts_iter)
        metadata = {'X-Timestamp': t4_meta.internal,
                    'X-Object-Meta-Test': 'o4',
                    'X-Object-Sysmeta-Test': 'sys_o4'}
        tx_objs['o4'][0].write_metadata(metadata)
        rx_objs['o4'][0].write_metadata(metadata)
        expected_subreqs['PUT'].append('o4')

        # o5 is on tx with meta, rx is in sync with data and meta
        t5 = next(self.ts_iter)
        rx_objs['o5'] = self._create_ondisk_files(rx_df_mgr, 'o5', policy, t5)
        tx_objs['o5'] = self._create_ondisk_files(tx_df_mgr, 'o5', policy, t5)
        t5_meta = next(self.ts_iter)
        metadata = {'X-Timestamp': t5_meta.internal,
                    'X-Object-Meta-Test': 'o5',
                    'X-Object-Sysmeta-Test': 'sys_o5'}
        tx_objs['o5'][0].write_metadata(metadata)
        rx_objs['o5'][0].write_metadata(metadata)

        # o6 is tombstone on tx, rx has older data and meta
        t6 = next(self.ts_iter)
        tx_tombstones['o6'] = self._create_ondisk_files(
            tx_df_mgr, 'o6', policy, t6)
        rx_tombstones['o6'] = self._create_ondisk_files(
            rx_df_mgr, 'o6', policy, t6)
        metadata = {'X-Timestamp': next(self.ts_iter).internal,
                    'X-Object-Meta-Test': 'o6',
                    'X-Object-Sysmeta-Test': 'sys_o6'}
        rx_tombstones['o6'][0].write_metadata(metadata)
        tx_tombstones['o6'][0].delete(next(self.ts_iter))
        expected_subreqs['DELETE'].append('o6')

        # o7 is tombstone on rx, tx has older data and meta,
        # no subreqs expected...
        t7 = next(self.ts_iter)
        tx_objs['o7'] = self._create_ondisk_files(tx_df_mgr, 'o7', policy, t7)
        rx_tombstones['o7'] = self._create_ondisk_files(
            rx_df_mgr, 'o7', policy, t7)
        metadata = {'X-Timestamp': next(self.ts_iter).internal,
                    'X-Object-Meta-Test': 'o7',
                    'X-Object-Sysmeta-Test': 'sys_o7'}
        tx_objs['o7'][0].write_metadata(metadata)
        rx_tombstones['o7'][0].delete(next(self.ts_iter))

        suffixes = set()
        for diskfiles in list(tx_objs.values()) + list(tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        success, in_sync_objs = sender()

        self.assertEqual(7, len(in_sync_objs))
        self.assertTrue(success)

        # verify protocol
        results = self._analyze_trace(trace)
        self.assertEqual(7, len(results['tx_missing']))
        self.assertEqual(5, len(results['rx_missing']))
        for subreq in results.get('tx_updates'):
            obj = subreq['path'].split('/')[3]
            method = subreq['method']
            self.assertTrue(obj in expected_subreqs[method],
                            'Unexpected %s subreq for object %s, expected %s'
                            % (method, obj, expected_subreqs[method]))
            expected_subreqs[method].remove(obj)
            if method == 'PUT':
                expected_body = self._get_object_data(subreq['path'])
                self.assertEqual(expected_body, subreq['body'])
        # verify all expected subreqs consumed
        for _method, expected in expected_subreqs.items():
            self.assertFalse(expected)
        self.assertFalse(results['rx_updates'])

        # verify on disk files...
        del tx_objs['o7']  # o7 not expected to be sync'd
        self._verify_ondisk_files(tx_objs, policy)
        self._verify_tombstones(tx_tombstones, policy)
        for oname, rx_obj in rx_objs.items():
            df = rx_obj[0].open()
            metadata = df.get_metadata()
            self.assertEqual(metadata['X-Object-Meta-Test'], oname)
            self.assertEqual(metadata['X-Object-Sysmeta-Test'], 'sys_' + oname)

    def test_expired_object(self):
        # verify that expired objects sync
        policy = POLICIES.default
        tx_df_mgr = self.daemon._df_router[policy]
        t1 = next(self.ts_iter)
        obj_name = 'o1'
        metadata = {'X-Delete-At': '0', 'Content-Type': 'plain/text'}
        df = self._make_diskfile(
            obj=obj_name, body=self._get_object_data('/a/c/%s' % obj_name),
            extra_metadata=metadata, timestamp=t1, policy=policy,
            df_mgr=tx_df_mgr, verify=False)
        with self.assertRaises(DiskFileExpired):
            df.open()  # sanity check - expired

        # create ssync sender instance...
        suffixes = [os.path.basename(os.path.dirname(df._datadir))]
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        success, in_sync_objs = sender()

        self.assertEqual(1, len(in_sync_objs))
        self.assertTrue(success)
        # allow the expired sender diskfile to be opened for verification
        df._open_expired = True
        self._verify_ondisk_files({obj_name: [df]}, policy)

    def _check_no_longer_expired_object(self, obj_name, df, policy):
        # verify that objects with x-delete-at metadata that are not expired
        # can be sync'd

        def do_ssync():
            # create ssync sender instance...
            suffixes = [os.path.basename(os.path.dirname(df._datadir))]
            job = {'device': self.device,
                   'partition': self.partition,
                   'policy': policy}
            node = dict(self.rx_node)
            sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
            # wrap connection from tx to rx to capture ssync messages...
            sender.connect, trace = self.make_connect_wrapper(sender)

            # run the sync protocol...
            return sender()

        with self.assertRaises(DiskFileExpired):
            df.open()  # sanity check - expired
        t1_meta = next(self.ts_iter)
        df.write_metadata({'X-Timestamp': t1_meta.internal})  # no x-delete-at
        df.open()  # sanity check - no longer expired

        success, in_sync_objs = do_ssync()
        self.assertEqual(1, len(in_sync_objs))
        self.assertTrue(success)
        self._verify_ondisk_files({obj_name: [df]}, policy)

        # update object metadata with x-delete-at in distant future
        t2_meta = next(self.ts_iter)
        df.write_metadata({'X-Timestamp': t2_meta.internal,
                           'X-Delete-At': str(int(t2_meta) + 10000)})
        df.open()  # sanity check - not expired

        success, in_sync_objs = do_ssync()
        self.assertEqual(1, len(in_sync_objs))
        self.assertTrue(success)
        self._verify_ondisk_files({obj_name: [df]}, policy)

        # update object metadata with x-delete-at in not so distant future to
        # check that we can update rx with older x-delete-at than it's current
        t3_meta = next(self.ts_iter)
        df.write_metadata({'X-Timestamp': t3_meta.internal,
                           'X-Delete-At': str(int(t2_meta) + 5000)})
        df.open()  # sanity check - not expired

        success, in_sync_objs = do_ssync()
        self.assertEqual(1, len(in_sync_objs))
        self.assertTrue(success)
        self._verify_ondisk_files({obj_name: [df]}, policy)

    def test_no_longer_expired_object_syncs(self):
        policy = POLICIES.default
        # simulate o1 that was PUT with x-delete-at that is now expired but
        # later had a POST that had no x-delete-at: object should not expire.
        tx_df_mgr = self.daemon._df_router[policy]
        t1 = next(self.ts_iter)
        obj_name = 'o1'
        metadata = {'X-Delete-At': '0', 'Content-Type': 'plain/text'}
        df = self._make_diskfile(
            obj=obj_name, body=self._get_object_data('/a/c/%s' % obj_name),
            extra_metadata=metadata, timestamp=t1, policy=policy,
            df_mgr=tx_df_mgr, verify=False)

        self._check_no_longer_expired_object(obj_name, df, policy)

    def test_no_longer_expired_object_syncs_meta(self):
        policy = POLICIES.default
        # simulate o1 that was PUT with x-delete-at that is now expired but
        # later had a POST that had no x-delete-at: object should not expire.
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        t1 = next(self.ts_iter)
        obj_name = 'o1'
        metadata = {'X-Delete-At': '0', 'Content-Type': 'plain/text'}
        df = self._make_diskfile(
            obj=obj_name, body=self._get_object_data('/a/c/%s' % obj_name),
            extra_metadata=metadata, timestamp=t1, policy=policy,
            df_mgr=tx_df_mgr, verify=False)
        # rx got the .data file but is missing the .meta
        rx_df = self._make_diskfile(
            obj=obj_name, body=self._get_object_data('/a/c/%s' % obj_name),
            extra_metadata=metadata, timestamp=t1, policy=policy,
            df_mgr=rx_df_mgr, verify=False)
        with self.assertRaises(DiskFileExpired):
            rx_df.open()  # sanity check - expired

        self._check_no_longer_expired_object(obj_name, df, policy)

    def test_meta_file_not_synced_to_legacy_receiver(self):
        # verify that the sender does sync a data file to a legacy receiver,
        # but does not PUT meta file content to a legacy receiver
        policy = POLICIES.default

        # create diskfiles...
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]

        # rx has data at t1 but no meta
        # object is on tx with data at t2, meta at t3,
        t1 = next(self.ts_iter)
        self._create_ondisk_files(rx_df_mgr, 'o1', policy, t1)
        t2 = next(self.ts_iter)
        tx_obj = self._create_ondisk_files(tx_df_mgr, 'o1', policy, t2)[0]
        t3 = next(self.ts_iter)
        metadata = {'X-Timestamp': t3.internal,
                    'X-Object-Meta-Test': 'o3',
                    'X-Object-Sysmeta-Test': 'sys_o3'}
        tx_obj.write_metadata(metadata)

        suffixes = [os.path.basename(os.path.dirname(tx_obj._datadir))]
        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        def _legacy_check_missing(self, line):
            # reproduces behavior of 'legacy' ssync receiver missing_checks()
            parts = line.split()
            object_hash = urllib.parse.unquote(parts[0])
            timestamp = urllib.parse.unquote(parts[1])
            want = False
            try:
                df = self.diskfile_mgr.get_diskfile_from_hash(
                    self.device, self.partition, object_hash, self.policy,
                    frag_index=self.frag_index)
            except DiskFileNotExist:
                want = True
            else:
                try:
                    df.open()
                except DiskFileDeleted as err:
                    want = err.timestamp < timestamp
                except DiskFileError:
                    want = True
                else:
                    want = df.timestamp < timestamp
            if want:
                return urllib.parse.quote(object_hash)
            return None

        # run the sync protocol...
        func = 'swift.obj.ssync_receiver.Receiver._check_missing'
        with mock.patch(func, _legacy_check_missing):
            success, in_sync_objs = sender()

        self.assertEqual(1, len(in_sync_objs))
        self.assertTrue(success)

        # verify protocol, expecting only a PUT to legacy receiver
        results = self._analyze_trace(trace)
        self.assertEqual(1, len(results['tx_missing']))
        self.assertEqual(1, len(results['rx_missing']))
        self.assertEqual(1, len(results['tx_updates']))
        self.assertEqual('PUT', results['tx_updates'][0]['method'])
        self.assertFalse(results['rx_updates'])

        # verify on disk files...
        rx_obj = self._open_rx_diskfile('o1', policy)
        tx_obj = self._open_tx_diskfile('o1', policy)
        # with legacy behavior rx_obj data and meta timestamps are equal
        self.assertEqual(t2, rx_obj.data_timestamp)
        self.assertEqual(t2, rx_obj.timestamp)
        # with legacy behavior rx_obj data timestamp should equal tx_obj
        self.assertEqual(rx_obj.data_timestamp, tx_obj.data_timestamp)
        # tx meta file should not have been sync'd to rx data file
        self.assertNotIn('X-Object-Meta-Test', rx_obj.get_metadata())

    def test_content_type_sync(self):
        policy = POLICIES.default

        # create diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_df_mgr = self.daemon._df_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]

        expected_subreqs = defaultdict(list)

        # o1 on tx only with two meta files
        name = 'o1'
        t1 = self.ts_iter.next()
        tx_objs[name] = self._create_ondisk_files(tx_df_mgr, name, policy, t1)
        t1_type = self.ts_iter.next()
        metadata_1 = {'X-Timestamp': t1_type.internal,
                      'Content-Type': 'text/test',
                      'Content-Type-Timestamp': t1_type.internal}
        tx_objs[name][0].write_metadata(metadata_1)
        t1_meta = self.ts_iter.next()
        metadata_2 = {'X-Timestamp': t1_meta.internal,
                      'X-Object-Meta-Test': name}
        tx_objs[name][0].write_metadata(metadata_2)
        expected_subreqs['PUT'].append(name)
        expected_subreqs['POST'].append(name)

        # o2 on tx with two meta files, rx has .data and newest .meta but is
        # missing latest content-type
        name = 'o2'
        t2 = self.ts_iter.next()
        tx_objs[name] = self._create_ondisk_files(tx_df_mgr, name, policy, t2)
        t2_type = self.ts_iter.next()
        metadata_1 = {'X-Timestamp': t2_type.internal,
                      'Content-Type': 'text/test',
                      'Content-Type-Timestamp': t2_type.internal}
        tx_objs[name][0].write_metadata(metadata_1)
        t2_meta = self.ts_iter.next()
        metadata_2 = {'X-Timestamp': t2_meta.internal,
                      'X-Object-Meta-Test': name}
        tx_objs[name][0].write_metadata(metadata_2)
        rx_objs[name] = self._create_ondisk_files(rx_df_mgr, name, policy, t2)
        rx_objs[name][0].write_metadata(metadata_2)
        expected_subreqs['POST'].append(name)

        # o3 on tx with two meta files, rx has .data and one .meta but does
        # have latest content-type so nothing to sync
        name = 'o3'
        t3 = self.ts_iter.next()
        tx_objs[name] = self._create_ondisk_files(tx_df_mgr, name, policy, t3)
        t3_type = self.ts_iter.next()
        metadata_1 = {'X-Timestamp': t3_type.internal,
                      'Content-Type': 'text/test',
                      'Content-Type-Timestamp': t3_type.internal}
        tx_objs[name][0].write_metadata(metadata_1)
        t3_meta = self.ts_iter.next()
        metadata_2 = {'X-Timestamp': t3_meta.internal,
                      'X-Object-Meta-Test': name}
        tx_objs[name][0].write_metadata(metadata_2)
        rx_objs[name] = self._create_ondisk_files(rx_df_mgr, name, policy, t3)
        metadata_2b = {'X-Timestamp': t3_meta.internal,
                       'X-Object-Meta-Test': name,
                       'Content-Type': 'text/test',
                       'Content-Type-Timestamp': t3_type.internal}
        rx_objs[name][0].write_metadata(metadata_2b)

        # o4 on tx with one meta file having latest content-type, rx has
        # .data and two .meta having latest content-type so nothing to sync
        # i.e. o4 is the reverse of o3 scenario
        name = 'o4'
        t4 = self.ts_iter.next()
        tx_objs[name] = self._create_ondisk_files(tx_df_mgr, name, policy, t4)
        t4_type = self.ts_iter.next()
        t4_meta = self.ts_iter.next()
        metadata_2b = {'X-Timestamp': t4_meta.internal,
                       'X-Object-Meta-Test': name,
                       'Content-Type': 'text/test',
                       'Content-Type-Timestamp': t4_type.internal}
        tx_objs[name][0].write_metadata(metadata_2b)
        rx_objs[name] = self._create_ondisk_files(rx_df_mgr, name, policy, t4)
        metadata_1 = {'X-Timestamp': t4_type.internal,
                      'Content-Type': 'text/test',
                      'Content-Type-Timestamp': t4_type.internal}
        rx_objs[name][0].write_metadata(metadata_1)
        metadata_2 = {'X-Timestamp': t4_meta.internal,
                      'X-Object-Meta-Test': name}
        rx_objs[name][0].write_metadata(metadata_2)

        # o5 on tx with one meta file having latest content-type, rx has
        # .data and no .meta
        name = 'o5'
        t5 = self.ts_iter.next()
        tx_objs[name] = self._create_ondisk_files(tx_df_mgr, name, policy, t5)
        t5_type = self.ts_iter.next()
        t5_meta = self.ts_iter.next()
        metadata = {'X-Timestamp': t5_meta.internal,
                    'X-Object-Meta-Test': name,
                    'Content-Type': 'text/test',
                    'Content-Type-Timestamp': t5_type.internal}
        tx_objs[name][0].write_metadata(metadata)
        rx_objs[name] = self._create_ondisk_files(rx_df_mgr, name, policy, t5)
        expected_subreqs['POST'].append(name)

        suffixes = set()
        for diskfiles in tx_objs.values():
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        success, in_sync_objs = sender()

        self.assertEqual(5, len(in_sync_objs), trace['messages'])
        self.assertTrue(success)

        # verify protocol
        results = self._analyze_trace(trace)
        self.assertEqual(5, len(results['tx_missing']))
        self.assertEqual(3, len(results['rx_missing']))
        for subreq in results.get('tx_updates'):
            obj = subreq['path'].split('/')[3]
            method = subreq['method']
            self.assertTrue(obj in expected_subreqs[method],
                            'Unexpected %s subreq for object %s, expected %s'
                            % (method, obj, expected_subreqs[method]))
            expected_subreqs[method].remove(obj)
            if method == 'PUT':
                expected_body = self._get_object_data(subreq['path'])
                self.assertEqual(expected_body, subreq['body'])
        # verify all expected subreqs consumed
        for _method, expected in expected_subreqs.items():
            self.assertFalse(expected,
                             'Expected subreqs not seen for %s for objects %s'
                             % (_method, expected))
        self.assertFalse(results['rx_updates'])

        # verify on disk files...
        self._verify_ondisk_files(tx_objs, policy)
        for oname, rx_obj in rx_objs.items():
            df = rx_obj[0].open()
            metadata = df.get_metadata()
            self.assertEqual(metadata['X-Object-Meta-Test'], oname)
            self.assertEqual(metadata['Content-Type'], 'text/test')
        # verify that tx and rx both generate the same suffix hashes...
        tx_hashes = tx_df_mgr.get_hashes(
            self.device, self.partition, suffixes, policy)
        rx_hashes = rx_df_mgr.get_hashes(
            self.device, self.partition, suffixes, policy)
        self.assertEqual(tx_hashes, rx_hashes)


if __name__ == '__main__':
    unittest.main()
