# Copyright (c) 2013 OpenStack Foundation
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

import hashlib
import os
import shutil
import tempfile
import time
import unittest

import eventlet
import itertools
import mock
import six
from six.moves import urllib

from swift.common import exceptions, utils
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import DiskFileNotExist, DiskFileError, \
    DiskFileDeleted
from swift.common.utils import Timestamp
from swift.obj import ssync_sender, diskfile, server, ssync_receiver
from swift.obj.reconstructor import RebuildingECDiskFileStream

from test.unit import debug_logger, patch_policies, make_timestamp_iter


class FakeReplicator(object):
    def __init__(self, testdir, policy=None):
        self.logger = debug_logger('test-ssync-sender')
        self.conn_timeout = 1
        self.node_timeout = 2
        self.http_timeout = 3
        self.network_chunk_size = 65536
        self.disk_chunk_size = 4096
        conf = {
            'devices': testdir,
            'mount_check': 'false',
        }
        policy = POLICIES.default if policy is None else policy
        self._diskfile_router = diskfile.DiskFileRouter(conf, self.logger)
        self._diskfile_mgr = self._diskfile_router[policy]


class NullBufferedHTTPConnection(object):

    def __init__(*args, **kwargs):
        pass

    def putrequest(*args, **kwargs):
        pass

    def putheader(*args, **kwargs):
        pass

    def endheaders(*args, **kwargs):
        pass

    def getresponse(*args, **kwargs):
        pass

    def close(*args, **kwargs):
        pass


class FakeResponse(object):

    def __init__(self, chunk_body=''):
        self.status = 200
        self.close_called = False
        if chunk_body:
            self.fp = six.StringIO(
                '%x\r\n%s\r\n0\r\n\r\n' % (len(chunk_body), chunk_body))

    def read(self, *args, **kwargs):
        return ''

    def close(self):
        self.close_called = True


class FakeConnection(object):

    def __init__(self):
        self.sent = []
        self.closed = False

    def send(self, data):
        self.sent.append(data)

    def close(self):
        self.closed = True


class BaseTestSender(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.testdir = os.path.join(self.tmpdir, 'tmp_test_ssync_sender')
        utils.mkdirs(os.path.join(self.testdir, 'dev'))
        self.daemon = FakeReplicator(self.testdir)
        self.sender = ssync_sender.Sender(self.daemon, None, None, None)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_open_diskfile(self, device='dev', partition='9',
                            account='a', container='c', obj='o', body='test',
                            extra_metadata=None, policy=None,
                            frag_index=None, timestamp=None, df_mgr=None):
        policy = policy or POLICIES.legacy
        object_parts = account, container, obj
        timestamp = Timestamp(time.time()) if timestamp is None else timestamp
        if df_mgr is None:
            df_mgr = self.daemon._diskfile_router[policy]
        df = df_mgr.get_diskfile(
            device, partition, *object_parts, policy=policy,
            frag_index=frag_index)
        content_length = len(body)
        etag = hashlib.md5(body).hexdigest()
        with df.create() as writer:
            writer.write(body)
            metadata = {
                'X-Timestamp': timestamp.internal,
                'Content-Length': str(content_length),
                'ETag': etag,
            }
            if extra_metadata:
                metadata.update(extra_metadata)
            writer.put(metadata)
            writer.commit(timestamp)
        df.open()
        return df


@patch_policies()
class TestSender(BaseTestSender):

    def test_call_catches_MessageTimeout(self):

        def connect(self):
            exc = exceptions.MessageTimeout(1, 'test connect')
            # Cancels Eventlet's raising of this since we're about to do it.
            exc.cancel()
            raise exc

        with mock.patch.object(ssync_sender.Sender, 'connect', connect):
            node = dict(replication_ip='1.2.3.4', replication_port=5678,
                        device='sda1')
            job = dict(partition='9', policy=POLICIES.legacy)
            self.sender = ssync_sender.Sender(self.daemon, node, job, None)
            self.sender.suffixes = ['abc']
            success, candidates = self.sender()
            self.assertFalse(success)
            self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines))
        self.assertEqual('1.2.3.4:5678/sda1/9 1 second: test connect',
                         error_lines[0])

    def test_call_catches_ReplicationException(self):

        def connect(self):
            raise exceptions.ReplicationException('test connect')

        with mock.patch.object(ssync_sender.Sender, 'connect', connect):
            node = dict(replication_ip='1.2.3.4', replication_port=5678,
                        device='sda1')
            job = dict(partition='9', policy=POLICIES.legacy)
            self.sender = ssync_sender.Sender(self.daemon, node, job, None)
            self.sender.suffixes = ['abc']
            success, candidates = self.sender()
            self.assertFalse(success)
            self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines))
        self.assertEqual('1.2.3.4:5678/sda1/9 test connect',
                         error_lines[0])

    def test_call_catches_other_exceptions(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        job = dict(partition='9', policy=POLICIES.legacy)
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        self.sender.connect = 'cause exception'
        success, candidates = self.sender()
        self.assertFalse(success)
        self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                '1.2.3.4:5678/sda1/9 EXCEPTION in replication.Sender:'))

    def test_call_catches_exception_handling_exception(self):
        job = node = None  # Will cause inside exception handler to fail
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        self.sender.connect = 'cause exception'
        success, candidates = self.sender()
        self.assertFalse(success)
        self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                'EXCEPTION in replication.Sender'))

    def test_call_calls_others(self):
        self.sender.suffixes = ['abc']
        self.sender.connect = mock.MagicMock()
        self.sender.missing_check = mock.MagicMock()
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates, {})
        self.sender.connect.assert_called_once_with()
        self.sender.missing_check.assert_called_once_with()
        self.sender.updates.assert_called_once_with()
        self.sender.disconnect.assert_called_once_with()

    def test_call_calls_others_returns_failure(self):
        self.sender.suffixes = ['abc']
        self.sender.connect = mock.MagicMock()
        self.sender.missing_check = mock.MagicMock()
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        self.sender.failures = 1
        success, candidates = self.sender()
        self.assertFalse(success)
        self.assertEqual(candidates, {})
        self.sender.connect.assert_called_once_with()
        self.sender.missing_check.assert_called_once_with()
        self.sender.updates.assert_called_once_with()
        self.sender.disconnect.assert_called_once_with()

    def test_connect(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1', index=0)
        job = dict(partition='9', policy=POLICIES[1])
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.bufferedhttp.BufferedHTTPConnection'
        ) as mock_conn_class:
            mock_conn = mock_conn_class.return_value
            mock_resp = mock.MagicMock()
            mock_resp.status = 200
            mock_conn.getresponse.return_value = mock_resp
            self.sender.connect()
        mock_conn_class.assert_called_once_with('1.2.3.4:5678')
        expectations = {
            'putrequest': [
                mock.call('SSYNC', '/sda1/9'),
            ],
            'putheader': [
                mock.call('Transfer-Encoding', 'chunked'),
                mock.call('X-Backend-Storage-Policy-Index', 1),
                mock.call('X-Backend-Ssync-Frag-Index', 0),
                mock.call('X-Backend-Ssync-Node-Index', 0),
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEqual(expected_calls, mock_method.mock_calls,
                             'connection method "%s" got %r not %r' % (
                                 method_name, mock_method.mock_calls,
                                 expected_calls))

    def test_connect_handoff(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        job = dict(partition='9', policy=POLICIES[1], frag_index=9)
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.bufferedhttp.BufferedHTTPConnection'
        ) as mock_conn_class:
            mock_conn = mock_conn_class.return_value
            mock_resp = mock.MagicMock()
            mock_resp.status = 200
            mock_conn.getresponse.return_value = mock_resp
            self.sender.connect()
        mock_conn_class.assert_called_once_with('1.2.3.4:5678')
        expectations = {
            'putrequest': [
                mock.call('SSYNC', '/sda1/9'),
            ],
            'putheader': [
                mock.call('Transfer-Encoding', 'chunked'),
                mock.call('X-Backend-Storage-Policy-Index', 1),
                mock.call('X-Backend-Ssync-Frag-Index', 9),
                mock.call('X-Backend-Ssync-Node-Index', ''),
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEqual(expected_calls, mock_method.mock_calls,
                             'connection method "%s" got %r not %r' % (
                                 method_name, mock_method.mock_calls,
                                 expected_calls))

    def test_connect_handoff_no_frag(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        job = dict(partition='9', policy=POLICIES[0])
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.bufferedhttp.BufferedHTTPConnection'
        ) as mock_conn_class:
            mock_conn = mock_conn_class.return_value
            mock_resp = mock.MagicMock()
            mock_resp.status = 200
            mock_conn.getresponse.return_value = mock_resp
            self.sender.connect()
        mock_conn_class.assert_called_once_with('1.2.3.4:5678')
        expectations = {
            'putrequest': [
                mock.call('SSYNC', '/sda1/9'),
            ],
            'putheader': [
                mock.call('Transfer-Encoding', 'chunked'),
                mock.call('X-Backend-Storage-Policy-Index', 0),
                mock.call('X-Backend-Ssync-Frag-Index', ''),
                mock.call('X-Backend-Ssync-Node-Index', ''),
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEqual(expected_calls, mock_method.mock_calls,
                             'connection method "%s" got %r not %r' % (
                                 method_name, mock_method.mock_calls,
                                 expected_calls))

    def test_connect_handoff_none_frag(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        job = dict(partition='9', policy=POLICIES[1], frag_index=None)
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.bufferedhttp.BufferedHTTPConnection'
        ) as mock_conn_class:
            mock_conn = mock_conn_class.return_value
            mock_resp = mock.MagicMock()
            mock_resp.status = 200
            mock_conn.getresponse.return_value = mock_resp
            self.sender.connect()
        mock_conn_class.assert_called_once_with('1.2.3.4:5678')
        expectations = {
            'putrequest': [
                mock.call('SSYNC', '/sda1/9'),
            ],
            'putheader': [
                mock.call('Transfer-Encoding', 'chunked'),
                mock.call('X-Backend-Storage-Policy-Index', 1),
                mock.call('X-Backend-Ssync-Frag-Index', ''),
                mock.call('X-Backend-Ssync-Node-Index', ''),
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEqual(expected_calls, mock_method.mock_calls,
                             'connection method "%s" got %r not %r' % (
                                 method_name, mock_method.mock_calls,
                                 expected_calls))

    def test_connect_handoff_replicated(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        # no frag_index in rsync job
        job = dict(partition='9', policy=POLICIES[1])
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.bufferedhttp.BufferedHTTPConnection'
        ) as mock_conn_class:
            mock_conn = mock_conn_class.return_value
            mock_resp = mock.MagicMock()
            mock_resp.status = 200
            mock_conn.getresponse.return_value = mock_resp
            self.sender.connect()
        mock_conn_class.assert_called_once_with('1.2.3.4:5678')
        expectations = {
            'putrequest': [
                mock.call('SSYNC', '/sda1/9'),
            ],
            'putheader': [
                mock.call('Transfer-Encoding', 'chunked'),
                mock.call('X-Backend-Storage-Policy-Index', 1),
                mock.call('X-Backend-Ssync-Frag-Index', ''),
                mock.call('X-Backend-Ssync-Node-Index', ''),
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEqual(expected_calls, mock_method.mock_calls,
                             'connection method "%s" got %r not %r' % (
                                 method_name, mock_method.mock_calls,
                                 expected_calls))

    def test_call(self):
        def patch_sender(sender):
            sender.connect = mock.MagicMock()
            sender.missing_check = mock.MagicMock()
            sender.updates = mock.MagicMock()
            sender.disconnect = mock.MagicMock()

        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        available_map = dict([('9d41d8cd98f00b204e9800998ecf0abc',
                               '1380144470.00000'),
                              ('9d41d8cd98f00b204e9800998ecf0def',
                               '1380144472.22222'),
                              ('9d41d8cd98f00b204e9800998ecf1def',
                               '1380144474.44444')])

        # no suffixes -> no work done
        sender = ssync_sender.Sender(
            self.daemon, node, job, [], remote_check_objs=None)
        patch_sender(sender)
        sender.available_map = available_map
        success, candidates = sender()
        self.assertTrue(success)
        self.assertEqual({}, candidates)

        # all objs in sync
        sender = ssync_sender.Sender(
            self.daemon, node, job, ['ignored'], remote_check_objs=None)
        patch_sender(sender)
        sender.available_map = available_map
        success, candidates = sender()
        self.assertTrue(success)
        self.assertEqual(available_map, candidates)

        # one obj not in sync, sync'ing faked, all objs should be in return set
        wanted = '9d41d8cd98f00b204e9800998ecf0def'
        sender = ssync_sender.Sender(
            self.daemon, node, job, ['ignored'],
            remote_check_objs=None)
        patch_sender(sender)
        sender.send_map = {wanted: []}
        sender.available_map = available_map
        success, candidates = sender()
        self.assertTrue(success)
        self.assertEqual(available_map, candidates)

        # one obj not in sync, remote check only so that obj is not sync'd
        # and should not be in the return set
        wanted = '9d41d8cd98f00b204e9800998ecf0def'
        remote_check_objs = set(available_map.keys())
        sender = ssync_sender.Sender(
            self.daemon, node, job, ['ignored'],
            remote_check_objs=remote_check_objs)
        patch_sender(sender)
        sender.send_map = {wanted: []}
        sender.available_map = available_map
        success, candidates = sender()
        self.assertTrue(success)
        expected_map = dict([('9d41d8cd98f00b204e9800998ecf0abc',
                              '1380144470.00000'),
                             ('9d41d8cd98f00b204e9800998ecf1def',
                              '1380144474.44444')])
        self.assertEqual(expected_map, candidates)

    def test_call_and_missing_check_metadata_legacy_response(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000),
                     'ts_meta': Timestamp(1380155570.00005)})
            else:
                raise Exception(
                    'No match for %r %r %r' % (device, partition, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.node = {}
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.suffixes = ['abc']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '9d41d8cd98f00b204e9800998ecf0abc\r\n'
                ':MISSING_CHECK: END\r\n'
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'
            ))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock()
        self.sender.df_mgr.get_diskfile_from_hash = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        found_post = found_put = False
        for chunk in self.sender.connection.sent:
            if 'POST' in chunk:
                found_post = True
            if 'PUT' in chunk:
                found_put = True
        self.assertFalse(found_post)
        self.assertTrue(found_put)
        self.assertEqual(self.sender.failures, 0)

    def test_call_and_missing_check(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r' % (device, partition, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.node = {}
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.suffixes = ['abc']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '9d41d8cd98f00b204e9800998ecf0abc d\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock()
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))
        self.assertEqual(self.sender.failures, 0)

    def test_call_and_missing_check_with_obj_list(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r' % (device, partition, suffixes))
        job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender = ssync_sender.Sender(self.daemon, None, job, ['abc'],
                                          ['9d41d8cd98f00b204e9800998ecf0abc'])
        self.sender.connection = FakeConnection()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock()
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))
        self.assertEqual(self.sender.failures, 0)

    def test_call_and_missing_check_with_obj_list_but_required(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r' % (device, partition, suffixes))
        job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender = ssync_sender.Sender(self.daemon, {}, job, ['abc'],
                                          ['9d41d8cd98f00b204e9800998ecf0abc'])
        self.sender.connection = FakeConnection()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '9d41d8cd98f00b204e9800998ecf0abc d\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock()
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates, {})

    def test_connect_send_timeout(self):
        self.daemon.node_timeout = 0.01  # make disconnect fail fast
        self.daemon.conn_timeout = 0.01
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1')
        job = dict(partition='9', policy=POLICIES.legacy)
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']

        def putrequest(*args, **kwargs):
            eventlet.sleep(0.1)

        with mock.patch.object(
                ssync_sender.bufferedhttp.BufferedHTTPConnection,
                'putrequest', putrequest):
            success, candidates = self.sender()
            self.assertFalse(success)
            self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                '1.2.3.4:5678/sda1/9 0.01 seconds: connect send'))

    def test_connect_receive_timeout(self):
        self.daemon.node_timeout = 0.02
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1', index=0)
        job = dict(partition='9', policy=POLICIES.legacy)
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']

        class FakeBufferedHTTPConnection(NullBufferedHTTPConnection):

            def getresponse(*args, **kwargs):
                eventlet.sleep(0.1)

        with mock.patch.object(
                ssync_sender.bufferedhttp, 'BufferedHTTPConnection',
                FakeBufferedHTTPConnection):
            success, candidates = self.sender()
            self.assertFalse(success)
            self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                '1.2.3.4:5678/sda1/9 0.02 seconds: connect receive'))

    def test_connect_bad_status(self):
        self.daemon.node_timeout = 0.02
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1', index=0)
        job = dict(partition='9', policy=POLICIES.legacy)

        class FakeBufferedHTTPConnection(NullBufferedHTTPConnection):
            def getresponse(*args, **kwargs):
                response = FakeResponse()
                response.status = 503
                response.read = lambda: 'an error message'
                return response

        missing_check_fn = 'swift.obj.ssync_sender.Sender.missing_check'
        with mock.patch(missing_check_fn) as mock_missing_check:
            with mock.patch.object(
                ssync_sender.bufferedhttp, 'BufferedHTTPConnection',
                    FakeBufferedHTTPConnection):
                self.sender = ssync_sender.Sender(
                    self.daemon, node, job, ['abc'])
                success, candidates = self.sender()
                self.assertFalse(success)
                self.assertEqual(candidates, {})
        error_lines = self.daemon.logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                '1.2.3.4:5678/sda1/9 Expected status 200; got 503'))
            self.assertIn('an error message', line)
        # sanity check that Sender did not proceed to missing_check exchange
        self.assertFalse(mock_missing_check.called)

    def test_readline_newline_in_buffer(self):
        self.sender.response_buffer = 'Has a newline already.\r\nOkay.'
        self.assertEqual(self.sender.readline(), 'Has a newline already.\r\n')
        self.assertEqual(self.sender.response_buffer, 'Okay.')

    def test_readline_buffer_exceeds_network_chunk_size_somehow(self):
        self.daemon.network_chunk_size = 2
        self.sender.response_buffer = '1234567890'
        self.assertEqual(self.sender.readline(), '1234567890')
        self.assertEqual(self.sender.response_buffer, '')

    def test_readline_at_start_of_chunk(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = six.StringIO('2\r\nx\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')

    def test_readline_chunk_with_extension(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = six.StringIO(
            '2 ; chunk=extension\r\nx\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')

    def test_readline_broken_chunk(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = six.StringIO('q\r\nx\n\r\n')
        self.assertRaises(
            exceptions.ReplicationException, self.sender.readline)
        self.assertTrue(self.sender.response.close_called)

    def test_readline_terminated_chunk(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = six.StringIO('b\r\nnot enough')
        self.assertRaises(
            exceptions.ReplicationException, self.sender.readline)
        self.assertTrue(self.sender.response.close_called)

    def test_readline_all(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = six.StringIO('2\r\nx\n\r\n0\r\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')
        self.assertEqual(self.sender.readline(), '')
        self.assertEqual(self.sender.readline(), '')

    def test_readline_all_trailing_not_newline_termed(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = six.StringIO(
            '2\r\nx\n\r\n3\r\n123\r\n0\r\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')
        self.assertEqual(self.sender.readline(), '123')
        self.assertEqual(self.sender.readline(), '')
        self.assertEqual(self.sender.readline(), '')

    def test_missing_check_timeout(self):
        self.sender.connection = FakeConnection()
        self.sender.connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.missing_check)

    def test_missing_check_has_empty_suffixes(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device != 'dev' or partition != '9' or
                    policy != POLICIES.legacy or
                    suffixes != ['abc', 'def']):
                yield  # Just here to make this a generator
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc', 'def']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.missing_check()
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.send_map, {})
        self.assertEqual(self.sender.available_map, {})

    def test_missing_check_has_suffixes(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc', 'def']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
                yield (
                    '/srv/node/dev/objects/9/def/'
                    '9d41d8cd98f00b204e9800998ecf0def',
                    '9d41d8cd98f00b204e9800998ecf0def',
                    {'ts_data': Timestamp(1380144472.22222)})
                yield (
                    '/srv/node/dev/objects/9/def/'
                    '9d41d8cd98f00b204e9800998ecf1def',
                    '9d41d8cd98f00b204e9800998ecf1def',
                    {'ts_data': Timestamp(1380144474.44444),
                     'ts_meta': Timestamp(1380144475.44444)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc', 'def']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.missing_check()
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0def 1380144472.22222\r\n\r\n'
            '3b\r\n9d41d8cd98f00b204e9800998ecf1def 1380144474.44444 '
            'm:186a0\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.send_map, {})
        candidates = [('9d41d8cd98f00b204e9800998ecf0abc',
                       dict(ts_data=Timestamp(1380144470.00000))),
                      ('9d41d8cd98f00b204e9800998ecf0def',
                       dict(ts_data=Timestamp(1380144472.22222))),
                      ('9d41d8cd98f00b204e9800998ecf1def',
                       dict(ts_data=Timestamp(1380144474.44444),
                            ts_meta=Timestamp(1380144475.44444)))]
        self.assertEqual(self.sender.available_map, dict(candidates))

    def test_missing_check_far_end_disconnect(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.response = FakeResponse(chunk_body='\r\n')
        exc = None
        try:
            self.sender.missing_check()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.available_map,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                dict(ts_data=Timestamp(1380144470.00000)))]))

    def test_missing_check_far_end_disconnect2(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.response = FakeResponse(
            chunk_body=':MISSING_CHECK: START\r\n')
        exc = None
        try:
            self.sender.missing_check()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.available_map,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_missing_check_far_end_unexpected(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.response = FakeResponse(chunk_body='OH HAI\r\n')
        exc = None
        try:
            self.sender.missing_check()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'OH HAI'")
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.available_map,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_missing_check_send_map(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '0123abc dm\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.missing_check()
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(
            self.sender.send_map, {'0123abc': {'data': True, 'meta': True}})
        self.assertEqual(self.sender.available_map,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_missing_check_extra_line_parts(self):
        # check that sender tolerates extra parts in missing check
        # line responses to allow for protocol upgrades
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '0123abc d extra response parts\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.missing_check()
        self.assertEqual(self.sender.send_map,
                         {'0123abc': {'data': True}})
        self.assertEqual(self.sender.available_map,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_updates_timeout(self):
        self.sender.connection = FakeConnection()
        self.sender.connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates)

    def test_updates_empty_send_map(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_unexpected_response_lines1(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                'abc\r\n'
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'abc'")
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_unexpected_response_lines2(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                'abc\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'abc'")
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_is_deleted(self):
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        df = self._make_open_diskfile(device, part, *object_parts)
        object_hash = utils.hash_path(*object_parts)
        delete_timestamp = utils.normalize_timestamp(time.time())
        df.delete(delete_timestamp)
        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        self.sender.send_map = {object_hash: {'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.sender.send_delete.assert_called_once_with(
            '/a/c/o', delete_timestamp)
        self.assertEqual(self.sender.send_put.mock_calls, [])
        # note that the delete line isn't actually sent since we mock
        # send_delete; send_delete is tested separately.
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_update_send_delete(self):
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        df = self._make_open_diskfile(device, part, *object_parts)
        object_hash = utils.hash_path(*object_parts)
        delete_timestamp = utils.normalize_timestamp(time.time())
        df.delete(delete_timestamp)
        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        self.sender.send_map = {object_hash: {'data': True}}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            '30\r\n'
            'DELETE /a/c/o\r\n'
            'X-Timestamp: %s\r\n\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n'
            % delete_timestamp
        )

    def test_updates_put(self):
        # sender has data file and meta file
        ts_iter = make_timestamp_iter()
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        t1 = next(ts_iter)
        df = self._make_open_diskfile(
            device, part, *object_parts, timestamp=t1)
        t2 = next(ts_iter)
        metadata = {'X-Timestamp': t2.internal, 'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        object_hash = utils.hash_path(*object_parts)
        df.open()
        expected = df.get_metadata()
        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        # receiver requested data only
        self.sender.send_map = {object_hash: {'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.send_post = mock.MagicMock()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(self.sender.send_post.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_put.mock_calls))
        args, _kwargs = self.sender.send_put.call_args
        path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertTrue(isinstance(df, diskfile.DiskFile))
        self.assertEqual(expected, df.get_metadata())
        # note that the put line isn't actually sent since we mock send_put;
        # send_put is tested separately.
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_post(self):
        ts_iter = make_timestamp_iter()
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        t1 = next(ts_iter)
        df = self._make_open_diskfile(
            device, part, *object_parts, timestamp=t1)
        t2 = next(ts_iter)
        metadata = {'X-Timestamp': t2.internal, 'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        object_hash = utils.hash_path(*object_parts)
        df.open()
        expected = df.get_metadata()
        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        # receiver requested only meta
        self.sender.send_map = {object_hash: {'meta': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.send_post = mock.MagicMock()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(self.sender.send_put.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_post.mock_calls))
        args, _kwargs = self.sender.send_post.call_args
        path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertIsInstance(df, diskfile.DiskFile)
        self.assertEqual(expected, df.get_metadata())
        # note that the post line isn't actually sent since we mock send_post;
        # send_post is tested separately.
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_put_and_post(self):
        ts_iter = make_timestamp_iter()
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        t1 = next(ts_iter)
        df = self._make_open_diskfile(
            device, part, *object_parts, timestamp=t1)
        t2 = next(ts_iter)
        metadata = {'X-Timestamp': t2.internal, 'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        object_hash = utils.hash_path(*object_parts)
        df.open()
        expected = df.get_metadata()
        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        # receiver requested data and meta
        self.sender.send_map = {object_hash: {'meta': True, 'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.send_post = mock.MagicMock()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_put.mock_calls))
        self.assertEqual(1, len(self.sender.send_post.mock_calls))

        args, _kwargs = self.sender.send_put.call_args
        path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertIsInstance(df, diskfile.DiskFile)
        self.assertEqual(expected, df.get_metadata())

        args, _kwargs = self.sender.send_post.call_args
        path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertIsInstance(df, diskfile.DiskFile)
        self.assertEqual(expected, df.get_metadata())
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_storage_policy_index(self):
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        df = self._make_open_diskfile(device, part, *object_parts,
                                      policy=POLICIES[0])
        object_hash = utils.hash_path(*object_parts)
        expected = df.get_metadata()
        self.sender.connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES[0],
            'frag_index': 0}
        self.sender.node = {}
        self.sender.send_map = {object_hash: {'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        args, _kwargs = self.sender.send_put.call_args
        path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertTrue(isinstance(df, diskfile.DiskFile))
        self.assertEqual(expected, df.get_metadata())
        self.assertEqual(os.path.join(self.testdir, 'dev/objects/9/',
                                      object_hash[-3:], object_hash),
                         df._datadir)

    def test_updates_read_response_timeout_start(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        orig_readline = self.sender.readline

        def delayed_readline():
            eventlet.sleep(1)
            return orig_readline()

        self.sender.readline = delayed_readline
        self.sender.daemon.http_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates)

    def test_updates_read_response_disconnect_start(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(chunk_body='\r\n')
        exc = None
        try:
            self.sender.updates()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_read_response_unexp_start(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                'anything else\r\n'
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'anything else'")
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_read_response_timeout_end(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        orig_readline = self.sender.readline

        def delayed_readline():
            rv = orig_readline()
            if rv == ':UPDATES: END\r\n':
                eventlet.sleep(1)
            return rv

        self.sender.readline = delayed_readline
        self.sender.daemon.http_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates)

    def test_updates_read_response_disconnect_end(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                '\r\n'))
        exc = None
        try:
            self.sender.updates()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_read_response_unexp_end(self):
        self.sender.connection = FakeConnection()
        self.sender.send_map = {}
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                'anything else\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates()
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'anything else'")
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_send_delete_timeout(self):
        self.sender.connection = FakeConnection()
        self.sender.connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        exc = None
        try:
            self.sender.send_delete('/a/c/o',
                                    utils.Timestamp('1381679759.90941'))
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_delete')

    def test_send_delete(self):
        self.sender.connection = FakeConnection()
        self.sender.send_delete('/a/c/o',
                                utils.Timestamp('1381679759.90941'))
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '30\r\n'
            'DELETE /a/c/o\r\n'
            'X-Timestamp: 1381679759.90941\r\n'
            '\r\n\r\n')

    def test_send_put_initial_timeout(self):
        df = self._make_open_diskfile()
        df._disk_chunk_size = 2
        self.sender.connection = FakeConnection()
        self.sender.connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        exc = None
        try:
            self.sender.send_put('/a/c/o', df)
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_put')

    def test_send_put_chunk_timeout(self):
        df = self._make_open_diskfile()
        self.sender.connection = FakeConnection()
        self.sender.daemon.node_timeout = 0.01

        one_shot = [None]

        def mock_send(data):
            try:
                one_shot.pop()
            except IndexError:
                eventlet.sleep(1)

        self.sender.connection.send = mock_send

        exc = None
        try:
            self.sender.send_put('/a/c/o', df)
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_put chunk')

    def test_send_put(self):
        ts_iter = make_timestamp_iter()
        t1 = next(ts_iter)
        body = 'test'
        extra_metadata = {'Some-Other-Header': 'value'}
        df = self._make_open_diskfile(body=body, timestamp=t1,
                                      extra_metadata=extra_metadata)
        expected = dict(df.get_metadata())
        expected['body'] = body
        expected['chunk_size'] = len(body)
        # .meta file metadata is not included in expected for data only PUT
        t2 = next(ts_iter)
        metadata = {'X-Timestamp': t2.internal, 'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        df.open()
        self.sender.connection = FakeConnection()
        self.sender.send_put('/a/c/o', df)
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '82\r\n'
            'PUT /a/c/o\r\n'
            'Content-Length: %(Content-Length)s\r\n'
            'ETag: %(ETag)s\r\n'
            'Some-Other-Header: value\r\n'
            'X-Timestamp: %(X-Timestamp)s\r\n'
            '\r\n'
            '\r\n'
            '%(chunk_size)s\r\n'
            '%(body)s\r\n' % expected)

    def test_send_post(self):
        # create .data file
        extra_metadata = {'X-Object-Meta-Foo': 'old_value',
                          'X-Object-Sysmeta-Test': 'test_sysmeta',
                          'Content-Type': 'test_content_type'}
        ts_0 = next(make_timestamp_iter())
        df = self._make_open_diskfile(extra_metadata=extra_metadata,
                                      timestamp=ts_0)
        # create .meta file
        ts_1 = next(make_timestamp_iter())
        newer_metadata = {'X-Object-Meta-Foo': 'new_value',
                          'X-Timestamp': ts_1.internal}
        df.write_metadata(newer_metadata)

        self.sender.connection = FakeConnection()
        with df.open():
            self.sender.send_post('/a/c/o', df)
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '4c\r\n'
            'POST /a/c/o\r\n'
            'X-Object-Meta-Foo: new_value\r\n'
            'X-Timestamp: %s\r\n'
            '\r\n'
            '\r\n' % ts_1.internal)

    def test_disconnect_timeout(self):
        self.sender.connection = FakeConnection()
        self.sender.connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        self.sender.disconnect()
        self.assertEqual(''.join(self.sender.connection.sent), '')
        self.assertTrue(self.sender.connection.closed)

    def test_disconnect(self):
        self.sender.connection = FakeConnection()
        self.sender.disconnect()
        self.assertEqual(''.join(self.sender.connection.sent), '0\r\n\r\n')
        self.assertTrue(self.sender.connection.closed)


class TestBaseSsync(BaseTestSender):
    """
    Provides a framework to test end to end interactions between sender and
    receiver. The basis for each test is actual diskfile state on either side.
    The connection between sender and receiver is wrapped to capture ssync
    traffic for subsequent verification of the protocol. Assertions are made
    about the final state of the sender and receiver diskfiles.
    """

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
            def wrapped_readline():
                data = readline()
                add_trace('rx', data)
                bytes_read = trace.setdefault('readline_bytes', 0)
                trace['readline_bytes'] = bytes_read + len(data)
                return data
            return wrapped_readline

        def wrapped_connect():
            orig_connect()
            sender.connection.send = make_send_wrapper(
                sender.connection.send)
            sender.readline = make_readline_wrapper(sender.readline)
        return wrapped_connect, trace

    def setUp(self):
        self.device = 'dev'
        self.partition = '9'
        self.tmpdir = tempfile.mkdtemp()
        # sender side setup
        self.tx_testdir = os.path.join(self.tmpdir, 'tmp_test_ssync_sender')
        utils.mkdirs(os.path.join(self.tx_testdir, self.device))
        self.daemon = FakeReplicator(self.tx_testdir)

        # rx side setup
        self.rx_testdir = os.path.join(self.tmpdir, 'tmp_test_ssync_receiver')
        utils.mkdirs(os.path.join(self.rx_testdir, self.device))
        conf = {
            'devices': self.rx_testdir,
            'mount_check': 'false',
            'replication_one_per_device': 'false',
            'log_requests': 'false'}
        self.rx_controller = server.ObjectController(conf)
        self.ts_iter = (Timestamp(t)
                        for t in itertools.count(int(time.time())))
        self.rx_ip = '127.0.0.1'
        sock = eventlet.listen((self.rx_ip, 0))
        self.rx_server = eventlet.spawn(
            eventlet.wsgi.server, sock, self.rx_controller, utils.NullLogger())
        self.rx_port = sock.getsockname()[1]
        self.rx_node = {'replication_ip': self.rx_ip,
                        'replication_port': self.rx_port,
                        'device': self.device}

    def tearDown(self):
        self.rx_server.kill()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _create_ondisk_files(self, df_mgr, obj_name, policy, timestamp,
                             frag_indexes=None):
        frag_indexes = [None] if frag_indexes is None else frag_indexes
        metadata = {'Content-Type': 'plain/text'}
        diskfiles = []
        for frag_index in frag_indexes:
            object_data = '/a/c/%s___%s' % (obj_name, frag_index)
            if frag_index is not None:
                metadata['X-Object-Sysmeta-Ec-Frag-Index'] = str(frag_index)
            df = self._make_open_diskfile(
                device=self.device, partition=self.partition, account='a',
                container='c', obj=obj_name, body=object_data,
                extra_metadata=metadata, timestamp=timestamp, policy=policy,
                frag_index=frag_index, df_mgr=df_mgr)
            # sanity checks
            listing = os.listdir(df._datadir)
            self.assertTrue(listing)
            for filename in listing:
                self.assertTrue(filename.startswith(timestamp.internal))
            diskfiles.append(df)
        return diskfiles

    def _open_tx_diskfile(self, obj_name, policy, frag_index=None):
        df_mgr = self.daemon._diskfile_router[policy]
        df = df_mgr.get_diskfile(
            self.device, self.partition, account='a', container='c',
            obj=obj_name, policy=policy, frag_index=frag_index)
        df.open()
        return df

    def _open_rx_diskfile(self, obj_name, policy, frag_index=None):
        df = self.rx_controller.get_diskfile(
            self.device, self.partition, 'a', 'c', obj_name, policy=policy,
            frag_index=frag_index)
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
                self.assertEqual(v, rx_metadata.pop(k), k)
        self.assertFalse(rx_metadata)
        expected_body = '%s___%s' % (tx_df._name, frag_index)
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
class TestSsyncEC(TestBaseSsync):
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
        tx_df_mgr = self.daemon._diskfile_router[policy]
        rx_df_mgr = self.rx_controller._diskfile_router[policy]
        # o1 has primary and handoff fragment archives
        t1 = next(self.ts_iter)
        tx_objs['o1'] = self._create_ondisk_files(
            tx_df_mgr, 'o1', policy, t1, (rx_node_index, tx_node_index))
        # o2 only has primary
        t2 = next(self.ts_iter)
        tx_objs['o2'] = self._create_ondisk_files(
            tx_df_mgr, 'o2', policy, t2, (tx_node_index,))
        # o3 only has handoff
        t3 = next(self.ts_iter)
        tx_objs['o3'] = self._create_ondisk_files(
            tx_df_mgr, 'o3', policy, t3, (rx_node_index,))
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
        for diskfiles in (tx_objs.values() + tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy,
               'frag_index': frag_index}
        node = dict(self.rx_node)
        node.update({'index': rx_node_index})
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
                expected_body = '%s___%s' % (subreq['path'], rx_node_index)
                self.assertEqual(expected_body, subreq['body'])
            elif subreq.get('method') == 'DELETE':
                self.assertEqual('/a/c/o5', subreq['path'])
            sync_paths.append(subreq.get('path'))
        self.assertEqual(['/a/c/o1', '/a/c/o3', '/a/c/o5'], sorted(sync_paths))

        # verify on disk files...
        self._verify_ondisk_files(
            tx_objs, policy, frag_index, rx_node_index)
        self._verify_tombstones(tx_tombstones, policy)

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
        tx_df_mgr = self.daemon._diskfile_router[policy]
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
        for diskfiles in (tx_objs.values() + tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        reconstruct_fa_calls = []

        def fake_reconstruct_fa(job, node, metadata):
            reconstruct_fa_calls.append((job, node, policy, metadata))
            if len(reconstruct_fa_calls) == 2:
                # simulate second reconstruct failing
                raise DiskFileError
            content = '%s___%s' % (metadata['name'], rx_node_index)
            return RebuildingECDiskFileStream(
                metadata, rx_node_index, iter([content]))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy,
               'frag_index': frag_index,
               'sync_diskfile_builder': fake_reconstruct_fa}
        node = dict(self.rx_node)
        node.update({'index': rx_node_index})
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
                expected_body = '%s___%s' % (subreq['path'], rx_node_index)
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


@patch_policies
class TestSsyncReplication(TestBaseSsync):
    def test_sync(self):
        policy = POLICIES.default
        rx_node_index = 0

        # create sender side diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_tombstones = {}
        rx_tombstones = {}
        tx_df_mgr = self.daemon._diskfile_router[policy]
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
        for diskfiles in (tx_objs.values() + tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        node.update({'index': rx_node_index})
        sender = ssync_sender.Sender(self.daemon, node, job, suffixes)
        # wrap connection from tx to rx to capture ssync messages...
        sender.connect, trace = self.make_connect_wrapper(sender)

        # run the sync protocol...
        success, in_sync_objs = sender()

        self.assertEqual(7, len(in_sync_objs), trace['messages'])
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
                expected_body = '%s___None' % subreq['path']
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
        rx_node_index = 0

        # create diskfiles...
        tx_objs = {}
        rx_objs = {}
        tx_tombstones = {}
        rx_tombstones = {}
        tx_df_mgr = self.daemon._diskfile_router[policy]
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
        for diskfiles in (tx_objs.values() + tx_tombstones.values()):
            for df in diskfiles:
                suffixes.add(os.path.basename(os.path.dirname(df._datadir)))

        # create ssync sender instance...
        job = {'device': self.device,
               'partition': self.partition,
               'policy': policy}
        node = dict(self.rx_node)
        node.update({'index': rx_node_index})
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
                expected_body = '%s___None' % subreq['path']
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

    def test_meta_file_not_synced_to_legacy_receiver(self):
        # verify that the sender does sync a data file to a legacy receiver,
        # but does not PUT meta file content to a legacy receiver
        policy = POLICIES.default
        rx_node_index = 0

        # create diskfiles...
        tx_df_mgr = self.daemon._diskfile_router[policy]
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
        node.update({'index': rx_node_index})
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
            except exceptions.DiskFileNotExist:
                want = True
            else:
                try:
                    df.open()
                except exceptions.DiskFileDeleted as err:
                    want = err.timestamp < timestamp
                except exceptions.DiskFileError as err:
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


class TestModuleMethods(unittest.TestCase):
    def test_encode_missing(self):
        object_hash = '9d41d8cd98f00b204e9800998ecf0abc'
        ts_iter = make_timestamp_iter()
        t_data = next(ts_iter)
        t_meta = next(ts_iter)
        d_meta_data = t_meta.raw - t_data.raw

        # equal data and meta timestamps -> legacy single timestamp string
        expected = '%s %s' % (object_hash, t_data.internal)
        self.assertEqual(
            expected,
            ssync_sender.encode_missing(object_hash, t_data, ts_meta=t_data))

        # newer meta timestamp -> hex data delta encoded as extra message part
        expected = '%s %s m:%x' % (object_hash, t_data.internal, d_meta_data)
        self.assertEqual(
            expected,
            ssync_sender.encode_missing(object_hash, t_data, ts_meta=t_meta))

        # test encode and decode functions invert
        expected = {'object_hash': object_hash, 'ts_meta': t_meta,
                    'ts_data': t_data}
        msg = ssync_sender.encode_missing(**expected)
        actual = ssync_receiver.decode_missing(msg)
        self.assertEqual(expected, actual)

    def test_decode_wanted(self):
        parts = ['d']
        expected = {'data': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)

        parts = ['m']
        expected = {'meta': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)

        parts = ['dm']
        expected = {'data': True, 'meta': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)

        # you don't really these next few...
        parts = ['md']
        expected = {'data': True, 'meta': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)

        parts = ['xcy', 'funny', {'business': True}]
        expected = {'data': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)


if __name__ == '__main__':
    unittest.main()
