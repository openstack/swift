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
import os
import time
import unittest

import eventlet
import mock
import six

from swift.common import exceptions, utils
from swift.common.storage_policy import POLICIES
from swift.common.utils import Timestamp
from swift.obj import ssync_sender, diskfile, ssync_receiver
from swift.obj.replicator import ObjectReplicator

from test.unit.obj.common import BaseTest
from test.unit import patch_policies, make_timestamp_iter, skip_if_no_xattrs, \
    debug_logger


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


class FakeResponse(ssync_sender.SsyncBufferedHTTPResponse):

    def __init__(self, chunk_body=''):
        self.status = 200
        self.close_called = False
        if chunk_body:
            self.fp = six.StringIO(
                '%x\r\n%s\r\n0\r\n\r\n' % (len(chunk_body), chunk_body))
        self.ssync_response_buffer = ''
        self.ssync_response_chunk_left = 0

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


@patch_policies()
class TestSender(BaseTest):

    def setUp(self):
        skip_if_no_xattrs()
        super(TestSender, self).setUp()
        self.daemon_logger = debug_logger('test-ssync-sender')
        self.daemon = ObjectReplicator(self.daemon_conf,
                                       self.daemon_logger)
        job = {'policy': POLICIES.legacy}  # sufficient for Sender.__init__
        self.sender = ssync_sender.Sender(self.daemon, None, job, None)

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
        error_lines = self.daemon_logger.get_lines_for_level('error')
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
        error_lines = self.daemon_logger.get_lines_for_level('error')
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
        error_lines = self.daemon_logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                '1.2.3.4:5678/sda1/9 EXCEPTION in ssync.Sender: '))

    def test_call_catches_exception_handling_exception(self):
        self.sender.node = None  # Will cause inside exception handler to fail
        self.sender.suffixes = ['abc']
        self.sender.connect = 'cause exception'
        success, candidates = self.sender()
        self.assertFalse(success)
        self.assertEqual(candidates, {})
        error_lines = self.daemon_logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                'EXCEPTION in ssync.Sender'))

    def test_call_calls_others(self):
        connection = FakeConnection()
        response = FakeResponse()
        self.sender.suffixes = ['abc']
        self.sender.connect = mock.MagicMock(return_value=(connection,
                                                           response))
        self.sender.missing_check = mock.MagicMock(return_value=({}, {}))
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates, {})
        self.sender.connect.assert_called_once_with()
        self.sender.missing_check.assert_called_once_with(connection, response)
        self.sender.updates.assert_called_once_with(connection, response, {})
        self.sender.disconnect.assert_called_once_with(connection)

    def test_connect(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1', backend_index=0)
        job = dict(partition='9', policy=POLICIES[1])
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.SsyncBufferedHTTPConnection'
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
                'swift.obj.ssync_sender.SsyncBufferedHTTPConnection'
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
                'swift.obj.ssync_sender.SsyncBufferedHTTPConnection'
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
                'swift.obj.ssync_sender.SsyncBufferedHTTPConnection'
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
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEqual(expected_calls, mock_method.mock_calls,
                             'connection method "%s" got %r not %r' % (
                                 method_name, mock_method.mock_calls,
                                 expected_calls))

    def test_connect_handoff_none_frag_to_primary(self):
        node = dict(replication_ip='1.2.3.4', replication_port=5678,
                    device='sda1', backend_index=42)
        job = dict(partition='9', policy=POLICIES[1], frag_index=None)
        self.sender = ssync_sender.Sender(self.daemon, node, job, None)
        self.sender.suffixes = ['abc']
        with mock.patch(
                'swift.obj.ssync_sender.SsyncBufferedHTTPConnection'
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
                mock.call('X-Backend-Ssync-Frag-Index', 42),
                mock.call('X-Backend-Ssync-Node-Index', 42),
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
                'swift.obj.ssync_sender.SsyncBufferedHTTPConnection'
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
        def patch_sender(sender, available_map, send_map):
            connection = FakeConnection()
            response = FakeResponse()
            sender.connect = mock.MagicMock(return_value=(connection,
                                                          response))
            sender.missing_check = mock.MagicMock()
            sender.missing_check = mock.MagicMock(return_value=(available_map,
                                                                send_map))
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
        patch_sender(sender, available_map, {})
        success, candidates = sender()
        self.assertTrue(success)
        self.assertEqual({}, candidates)

        # all objs in sync
        sender = ssync_sender.Sender(
            self.daemon, node, job, ['ignored'], remote_check_objs=None)
        patch_sender(sender, available_map, {})
        success, candidates = sender()
        self.assertTrue(success)
        self.assertEqual(available_map, candidates)

        # one obj not in sync, sync'ing faked, all objs should be in return set
        wanted = '9d41d8cd98f00b204e9800998ecf0def'
        sender = ssync_sender.Sender(
            self.daemon, node, job, ['ignored'],
            remote_check_objs=None)
        patch_sender(sender, available_map, {wanted: []})
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
        patch_sender(sender, available_map, {wanted: []})
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
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000),
                     'ts_meta': Timestamp(1380155570.00005)})
            else:
                raise Exception(
                    'No match for %r %r %r' % (device, partition, suffixes))

        connection = FakeConnection()
        self.sender.node = {}
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.suffixes = ['abc']
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '9d41d8cd98f00b204e9800998ecf0abc\r\n'
                ':MISSING_CHECK: END\r\n'
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'
            ))
        self.sender.df_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock(return_value=(connection,
                                                           response))
        df = mock.MagicMock()
        df.content_length = 0
        self.sender.df_mgr.get_diskfile_from_hash = mock.MagicMock(
            return_value=df)
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        found_post = found_put = False
        for chunk in connection.sent:
            if 'POST' in chunk:
                found_post = True
            if 'PUT' in chunk:
                found_put = True
        self.assertFalse(found_post)
        self.assertTrue(found_put)

    def test_call_and_missing_check(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r' % (device, partition, suffixes))

        connection = FakeConnection()
        self.sender.node = {}
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.suffixes = ['abc']
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '9d41d8cd98f00b204e9800998ecf0abc d\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock(return_value=(connection,
                                                           response))
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_call_and_missing_check_with_obj_list(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
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
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock(return_value=(connection,
                                                           response))
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        success, candidates = self.sender()
        self.assertTrue(success)
        self.assertEqual(candidates,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_call_and_missing_check_with_obj_list_but_required(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if device == 'dev' and partition == '9' and suffixes == ['abc'] \
                    and policy == POLICIES.legacy:
                yield (
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
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '9d41d8cd98f00b204e9800998ecf0abc d\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        self.sender.connect = mock.MagicMock(return_value=(connection,
                                                           response))
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
        error_lines = self.daemon_logger.get_lines_for_level('error')
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
                ssync_sender, 'SsyncBufferedHTTPConnection',
                FakeBufferedHTTPConnection):
            success, candidates = self.sender()
            self.assertFalse(success)
            self.assertEqual(candidates, {})
        error_lines = self.daemon_logger.get_lines_for_level('error')
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
                ssync_sender, 'SsyncBufferedHTTPConnection',
                    FakeBufferedHTTPConnection):
                self.sender = ssync_sender.Sender(
                    self.daemon, node, job, ['abc'])
                success, candidates = self.sender()
                self.assertFalse(success)
                self.assertEqual(candidates, {})
        error_lines = self.daemon_logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith(
                '1.2.3.4:5678/sda1/9 Expected status 200; got 503'))
            self.assertIn('an error message', line)
        # sanity check that Sender did not proceed to missing_check exchange
        self.assertFalse(mock_missing_check.called)

    def test_readline_newline_in_buffer(self):
        response = FakeResponse()
        response.ssync_response_buffer = 'Has a newline already.\r\nOkay.'
        self.assertEqual(response.readline(), 'Has a newline already.\r\n')
        self.assertEqual(response.ssync_response_buffer, 'Okay.')

    def test_readline_buffer_exceeds_network_chunk_size_somehow(self):
        response = FakeResponse()
        response.ssync_response_buffer = '1234567890'
        self.assertEqual(response.readline(size=2), '1234567890')
        self.assertEqual(response.ssync_response_buffer, '')

    def test_readline_at_start_of_chunk(self):
        response = FakeResponse()
        response.fp = six.StringIO('2\r\nx\n\r\n')
        self.assertEqual(response.readline(), 'x\n')

    def test_readline_chunk_with_extension(self):
        response = FakeResponse()
        response.fp = six.StringIO(
            '2 ; chunk=extension\r\nx\n\r\n')
        self.assertEqual(response.readline(), 'x\n')

    def test_readline_broken_chunk(self):
        response = FakeResponse()
        response.fp = six.StringIO('q\r\nx\n\r\n')
        self.assertRaises(
            exceptions.ReplicationException, response.readline)
        self.assertTrue(response.close_called)

    def test_readline_terminated_chunk(self):
        response = FakeResponse()
        response.fp = six.StringIO('b\r\nnot enough')
        self.assertRaises(
            exceptions.ReplicationException, response.readline)
        self.assertTrue(response.close_called)

    def test_readline_all(self):
        response = FakeResponse()
        response.fp = six.StringIO('2\r\nx\n\r\n0\r\n\r\n')
        self.assertEqual(response.readline(), 'x\n')
        self.assertEqual(response.readline(), '')
        self.assertEqual(response.readline(), '')

    def test_readline_all_trailing_not_newline_termed(self):
        response = FakeResponse()
        response.fp = six.StringIO(
            '2\r\nx\n\r\n3\r\n123\r\n0\r\n\r\n')
        self.assertEqual(response.readline(), 'x\n')
        self.assertEqual(response.readline(), '123')
        self.assertEqual(response.readline(), '')
        self.assertEqual(response.readline(), '')

    def test_missing_check_timeout(self):
        connection = FakeConnection()
        connection.send = lambda d: eventlet.sleep(1)
        response = FakeResponse()
        self.sender.daemon.node_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.missing_check,
                          connection, response)

    def test_missing_check_has_empty_suffixes(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device != 'dev' or partition != '9' or
                    policy != POLICIES.legacy or
                    suffixes != ['abc', 'def']):
                yield  # Just here to make this a generator
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc', 'def']
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        available_map, send_map = self.sender.missing_check(connection,
                                                            response)
        self.assertEqual(
            ''.join(connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(send_map, {})
        self.assertEqual(available_map, {})

    def test_missing_check_has_suffixes(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc', 'def']):
                yield (
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
                yield (
                    '9d41d8cd98f00b204e9800998ecf0def',
                    {'ts_data': Timestamp(1380144472.22222),
                     'ts_meta': Timestamp(1380144473.22222)})
                yield (
                    '9d41d8cd98f00b204e9800998ecf1def',
                    {'ts_data': Timestamp(1380144474.44444),
                     'ts_ctype': Timestamp(1380144474.44448),
                     'ts_meta': Timestamp(1380144475.44444)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc', 'def']
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        available_map, send_map = self.sender.missing_check(connection,
                                                            response)
        self.assertEqual(
            ''.join(connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '3b\r\n9d41d8cd98f00b204e9800998ecf0def 1380144472.22222 '
            'm:186a0\r\n\r\n'
            '3f\r\n9d41d8cd98f00b204e9800998ecf1def 1380144474.44444 '
            'm:186a0,t:4\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(send_map, {})
        candidates = [('9d41d8cd98f00b204e9800998ecf0abc',
                       dict(ts_data=Timestamp(1380144470.00000))),
                      ('9d41d8cd98f00b204e9800998ecf0def',
                       dict(ts_data=Timestamp(1380144472.22222),
                            ts_meta=Timestamp(1380144473.22222))),
                      ('9d41d8cd98f00b204e9800998ecf1def',
                       dict(ts_data=Timestamp(1380144474.44444),
                            ts_meta=Timestamp(1380144475.44444),
                            ts_ctype=Timestamp(1380144474.44448)))]
        self.assertEqual(available_map, dict(candidates))

    def test_missing_check_far_end_disconnect(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.df_mgr.yield_hashes = yield_hashes
        response = FakeResponse(chunk_body='\r\n')
        exc = None
        try:
            self.sender.missing_check(connection, response)
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')

    def test_missing_check_far_end_disconnect2(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.df_mgr.yield_hashes = yield_hashes
        response = FakeResponse(
            chunk_body=':MISSING_CHECK: START\r\n')
        exc = None
        try:
            self.sender.missing_check(connection, response)
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')

    def test_missing_check_far_end_unexpected(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        self.sender.df_mgr.yield_hashes = yield_hashes
        response = FakeResponse(chunk_body='OH HAI\r\n')
        exc = None
        try:
            self.sender.missing_check(connection, response)
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'OH HAI'")
        self.assertEqual(
            ''.join(connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')

    def test_missing_check_send_map(self):
        def yield_hashes(device, partition, policy, suffixes=None, **kwargs):
            if (device == 'dev' and partition == '9' and
                    policy == POLICIES.legacy and
                    suffixes == ['abc']):
                yield (
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '0123abc dm\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        available_map, send_map = self.sender.missing_check(connection,
                                                            response)
        self.assertEqual(
            ''.join(connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(send_map, {'0123abc': {'data': True, 'meta': True}})
        self.assertEqual(available_map,
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
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    {'ts_data': Timestamp(1380144470.00000)})
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy, suffixes))

        connection = FakeConnection()
        self.sender.job = {
            'device': 'dev',
            'partition': '9',
            'policy': POLICIES.legacy,
        }
        self.sender.suffixes = ['abc']
        response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '0123abc d extra response parts\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.df_mgr.yield_hashes = yield_hashes
        available_map, send_map = self.sender.missing_check(connection,
                                                            response)
        self.assertEqual(send_map, {'0123abc': {'data': True}})
        self.assertEqual(available_map,
                         dict([('9d41d8cd98f00b204e9800998ecf0abc',
                                {'ts_data': Timestamp(1380144470.00000)})]))

    def test_updates_timeout(self):
        connection = FakeConnection()
        connection.send = lambda d: eventlet.sleep(1)
        response = FakeResponse()
        self.sender.daemon.node_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates,
                          connection, response, {})

    def test_updates_empty_send_map(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, {})
        self.assertEqual(
            ''.join(connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_unexpected_response_lines1(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                'abc\r\n'
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates(connection, response, {})
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'abc'")
        self.assertEqual(
            ''.join(connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_unexpected_response_lines2(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                'abc\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates(connection, response, {})
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'abc'")
        self.assertEqual(
            ''.join(connection.sent),
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
        connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        send_map = {object_hash: {'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, send_map)
        self.sender.send_delete.assert_called_once_with(
            connection, '/a/c/o', delete_timestamp)
        self.assertEqual(self.sender.send_put.mock_calls, [])
        # note that the delete line isn't actually sent since we mock
        # send_delete; send_delete is tested separately.
        self.assertEqual(
            ''.join(connection.sent),
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
        connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        send_map = {object_hash: {'data': True}}
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, send_map)
        self.assertEqual(
            ''.join(connection.sent),
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
        connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        # receiver requested data only
        send_map = {object_hash: {'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.send_post = mock.MagicMock()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, send_map)
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(self.sender.send_post.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_put.mock_calls))
        args, _kwargs = self.sender.send_put.call_args
        connection, path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertTrue(isinstance(df, diskfile.DiskFile))
        self.assertEqual(expected, df.get_metadata())
        # note that the put line isn't actually sent since we mock send_put;
        # send_put is tested separately.
        self.assertEqual(
            ''.join(connection.sent),
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
        connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        # receiver requested only meta
        send_map = {object_hash: {'meta': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.send_post = mock.MagicMock()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, send_map)
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(self.sender.send_put.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_post.mock_calls))
        args, _kwargs = self.sender.send_post.call_args
        connection, path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertIsInstance(df, diskfile.DiskFile)
        self.assertEqual(expected, df.get_metadata())
        # note that the post line isn't actually sent since we mock send_post;
        # send_post is tested separately.
        self.assertEqual(
            ''.join(connection.sent),
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
        connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES.legacy,
            'frag_index': 0,
        }
        self.sender.node = {}
        # receiver requested data and meta
        send_map = {object_hash: {'meta': True, 'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.send_post = mock.MagicMock()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, send_map)
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_put.mock_calls))
        self.assertEqual(1, len(self.sender.send_post.mock_calls))

        args, _kwargs = self.sender.send_put.call_args
        connection, path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertIsInstance(df, diskfile.DiskFile)
        self.assertEqual(expected, df.get_metadata())

        args, _kwargs = self.sender.send_post.call_args
        connection, path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertIsInstance(df, diskfile.DiskFile)
        self.assertEqual(expected, df.get_metadata())
        self.assertEqual(
            ''.join(connection.sent),
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
        connection = FakeConnection()
        self.sender.job = {
            'device': device,
            'partition': part,
            'policy': POLICIES[0],
            'frag_index': 0}
        self.sender.node = {}
        send_map = {object_hash: {'data': True}}
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates(connection, response, send_map)
        args, _kwargs = self.sender.send_put.call_args
        connection, path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assertTrue(isinstance(df, diskfile.DiskFile))
        self.assertEqual(expected, df.get_metadata())
        self.assertEqual(os.path.join(self.tx_testdir, 'dev/objects/9/',
                                      object_hash[-3:], object_hash),
                         df._datadir)

    def test_updates_read_response_timeout_start(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        orig_readline = response.readline

        def delayed_readline(*args, **kwargs):
            eventlet.sleep(1)
            return orig_readline(*args, **kwargs)

        response.readline = delayed_readline
        self.sender.daemon.http_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates,
                          connection, response, {})

    def test_updates_read_response_disconnect_start(self):
        connection = FakeConnection()
        response = FakeResponse(chunk_body='\r\n')
        exc = None
        try:
            self.sender.updates(connection, response, {})
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_read_response_unexp_start(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                'anything else\r\n'
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates(connection, response, {})
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'anything else'")
        self.assertEqual(
            ''.join(connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_read_response_timeout_end(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        orig_readline = response.readline

        def delayed_readline(*args, **kwargs):
            rv = orig_readline(*args, **kwargs)
            if rv == ':UPDATES: END\r\n':
                eventlet.sleep(1)
            return rv

        response.readline = delayed_readline
        self.sender.daemon.http_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates,
                          connection, response, {})

    def test_updates_read_response_disconnect_end(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                '\r\n'))
        exc = None
        try:
            self.sender.updates(connection, response, {})
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), 'Early disconnect')
        self.assertEqual(
            ''.join(connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_updates_read_response_unexp_end(self):
        connection = FakeConnection()
        response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                'anything else\r\n'
                ':UPDATES: END\r\n'))
        exc = None
        try:
            self.sender.updates(connection, response, {})
        except exceptions.ReplicationException as err:
            exc = err
        self.assertEqual(str(exc), "Unexpected response: 'anything else'")
        self.assertEqual(
            ''.join(connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    def test_send_delete_timeout(self):
        connection = FakeConnection()
        connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        exc = None
        try:
            self.sender.send_delete(connection, '/a/c/o',
                                    utils.Timestamp('1381679759.90941'))
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_delete')

    def test_send_delete(self):
        connection = FakeConnection()
        self.sender.send_delete(connection, '/a/c/o',
                                utils.Timestamp('1381679759.90941'))
        self.assertEqual(
            ''.join(connection.sent),
            '30\r\n'
            'DELETE /a/c/o\r\n'
            'X-Timestamp: 1381679759.90941\r\n'
            '\r\n\r\n')

    def test_send_put_initial_timeout(self):
        df = self._make_open_diskfile()
        df._disk_chunk_size = 2
        connection = FakeConnection()
        connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        exc = None
        try:
            self.sender.send_put(connection, '/a/c/o', df)
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_put')

    def test_send_put_chunk_timeout(self):
        df = self._make_open_diskfile()
        connection = FakeConnection()
        self.sender.daemon.node_timeout = 0.01

        one_shot = [None]

        def mock_send(data):
            try:
                one_shot.pop()
            except IndexError:
                eventlet.sleep(1)

        connection.send = mock_send

        exc = None
        try:
            self.sender.send_put(connection, '/a/c/o', df)
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_put chunk')

    def _check_send_put(self, obj_name, meta_value):
        ts_iter = make_timestamp_iter()
        t1 = next(ts_iter)
        body = 'test'
        extra_metadata = {'Some-Other-Header': 'value',
                          u'Unicode-Meta-Name': meta_value}
        df = self._make_open_diskfile(obj=obj_name, body=body,
                                      timestamp=t1,
                                      extra_metadata=extra_metadata)
        expected = dict(df.get_metadata())
        expected['body'] = body
        expected['chunk_size'] = len(body)
        expected['meta'] = meta_value
        path = six.moves.urllib.parse.quote(expected['name'])
        expected['path'] = path
        expected['length'] = format(145 + len(path) + len(meta_value), 'x')
        # .meta file metadata is not included in expected for data only PUT
        t2 = next(ts_iter)
        metadata = {'X-Timestamp': t2.internal, 'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        df.open()
        connection = FakeConnection()
        self.sender.send_put(connection, path, df)
        self.assertEqual(
            ''.join(connection.sent),
            '%(length)s\r\n'
            'PUT %(path)s\r\n'
            'Content-Length: %(Content-Length)s\r\n'
            'ETag: %(ETag)s\r\n'
            'Some-Other-Header: value\r\n'
            'Unicode-Meta-Name: %(meta)s\r\n'
            'X-Timestamp: %(X-Timestamp)s\r\n'
            '\r\n'
            '\r\n'
            '%(chunk_size)s\r\n'
            '%(body)s\r\n' % expected)

    def test_send_put(self):
        self._check_send_put('o', 'meta')

    def test_send_put_unicode(self):
        self._check_send_put(
            'o_with_caract\xc3\xa8res_like_in_french', 'm\xc3\xa8ta')

    def _check_send_post(self, obj_name, meta_value):
        ts_iter = make_timestamp_iter()
        # create .data file
        extra_metadata = {'X-Object-Meta-Foo': 'old_value',
                          'X-Object-Sysmeta-Test': 'test_sysmeta',
                          'Content-Type': 'test_content_type'}
        ts_0 = next(ts_iter)
        df = self._make_open_diskfile(obj=obj_name,
                                      extra_metadata=extra_metadata,
                                      timestamp=ts_0)
        # create .meta file
        ts_1 = next(ts_iter)
        newer_metadata = {u'X-Object-Meta-Foo': meta_value,
                          'X-Timestamp': ts_1.internal}
        df.write_metadata(newer_metadata)
        path = six.moves.urllib.parse.quote(df.read_metadata()['name'])
        length = format(61 + len(path) + len(meta_value), 'x')

        connection = FakeConnection()
        with df.open():
            self.sender.send_post(connection, path, df)
        self.assertEqual(
            ''.join(connection.sent),
            '%s\r\n'
            'POST %s\r\n'
            'X-Object-Meta-Foo: %s\r\n'
            'X-Timestamp: %s\r\n'
            '\r\n'
            '\r\n' % (length, path, meta_value, ts_1.internal))

    def test_send_post(self):
        self._check_send_post('o', 'meta')

    def test_send_post_unicode(self):
        self._check_send_post(
            'o_with_caract\xc3\xa8res_like_in_french', 'm\xc3\xa8ta')

    def test_disconnect_timeout(self):
        connection = FakeConnection()
        connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        self.sender.disconnect(connection)
        self.assertEqual(''.join(connection.sent), '')
        self.assertTrue(connection.closed)

    def test_disconnect(self):
        connection = FakeConnection()
        self.sender.disconnect(connection)
        self.assertEqual(''.join(connection.sent), '0\r\n\r\n')
        self.assertTrue(connection.closed)


class TestModuleMethods(unittest.TestCase):
    def test_encode_missing(self):
        object_hash = '9d41d8cd98f00b204e9800998ecf0abc'
        ts_iter = make_timestamp_iter()
        t_data = next(ts_iter)
        t_type = next(ts_iter)
        t_meta = next(ts_iter)
        d_meta_data = t_meta.raw - t_data.raw
        d_type_data = t_type.raw - t_data.raw

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

        # newer meta timestamp -> hex data delta encoded as extra message part
        # content type timestamp equals data timestamp -> no delta
        expected = '%s %s m:%x' % (object_hash, t_data.internal, d_meta_data)
        self.assertEqual(
            expected,
            ssync_sender.encode_missing(object_hash, t_data, t_meta, t_data))

        # content type timestamp newer data timestamp -> delta encoded
        expected = ('%s %s m:%x,t:%x'
                    % (object_hash, t_data.internal, d_meta_data, d_type_data))
        self.assertEqual(
            expected,
            ssync_sender.encode_missing(object_hash, t_data, t_meta, t_type))

        # content type timestamp equal to meta timestamp -> delta encoded
        expected = ('%s %s m:%x,t:%x'
                    % (object_hash, t_data.internal, d_meta_data, d_type_data))
        self.assertEqual(
            expected,
            ssync_sender.encode_missing(object_hash, t_data, t_meta, t_type))

        # test encode and decode functions invert
        expected = {'object_hash': object_hash, 'ts_meta': t_meta,
                    'ts_data': t_data, 'ts_ctype': t_type}
        msg = ssync_sender.encode_missing(**expected)
        actual = ssync_receiver.decode_missing(msg)
        self.assertEqual(expected, actual)

        expected = {'object_hash': object_hash, 'ts_meta': t_meta,
                    'ts_data': t_meta, 'ts_ctype': t_meta}
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

        # you don't really expect these next few...
        parts = ['md']
        expected = {'data': True, 'meta': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)

        parts = ['xcy', 'funny', {'business': True}]
        expected = {'data': True}
        self.assertEqual(ssync_sender.decode_wanted(parts), expected)


if __name__ == '__main__':
    unittest.main()
