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

import hashlib
import os
import shutil
import StringIO
import tempfile
import time
import unittest

import eventlet
import mock

from swift.common import exceptions, utils
from swift.obj import ssync_sender, diskfile

from test.unit import DebugLogger, patch_policies


class FakeReplicator(object):

    def __init__(self, testdir):
        self.logger = mock.MagicMock()
        self.conn_timeout = 1
        self.node_timeout = 2
        self.http_timeout = 3
        self.network_chunk_size = 65536
        self.disk_chunk_size = 4096
        conf = {
            'devices': testdir,
            'mount_check': 'false',
        }
        self._diskfile_mgr = diskfile.DiskFileManager(conf, DebugLogger())


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


class FakeResponse(object):

    def __init__(self, chunk_body=''):
        self.status = 200
        self.close_called = False
        if chunk_body:
            self.fp = StringIO.StringIO(
                '%x\r\n%s\r\n0\r\n\r\n' % (len(chunk_body), chunk_body))

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


class TestSender(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.testdir = os.path.join(self.tmpdir, 'tmp_test_ssync_sender')
        self.replicator = FakeReplicator(self.testdir)
        self.sender = ssync_sender.Sender(self.replicator, None, None, None)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=1)

    def _make_open_diskfile(self, device='dev', partition='9',
                            account='a', container='c', obj='o', body='test',
                            extra_metadata=None, policy_idx=0):
        object_parts = account, container, obj
        req_timestamp = utils.normalize_timestamp(time.time())
        df = self.sender.daemon._diskfile_mgr.get_diskfile(
            device, partition, *object_parts, policy_idx=policy_idx)
        content_length = len(body)
        etag = hashlib.md5(body).hexdigest()
        with df.create() as writer:
            writer.write(body)
            metadata = {
                'X-Timestamp': req_timestamp,
                'Content-Length': content_length,
                'ETag': etag,
            }
            if extra_metadata:
                metadata.update(extra_metadata)
            writer.put(metadata)
        df.open()
        return df

    def test_call_catches_MessageTimeout(self):

        def connect(self):
            exc = exceptions.MessageTimeout(1, 'test connect')
            # Cancels Eventlet's raising of this since we're about to do it.
            exc.cancel()
            raise exc

        with mock.patch.object(ssync_sender.Sender, 'connect', connect):
            node = dict(ip='1.2.3.4', port=5678, device='sda1')
            job = dict(partition='9')
            self.sender = ssync_sender.Sender(self.replicator, node, job, None)
            self.sender.suffixes = ['abc']
            self.assertFalse(self.sender())
        call = self.replicator.logger.error.mock_calls[0]
        self.assertEqual(
            call[1][:-1], ('%s:%s/%s/%s %s', '1.2.3.4', 5678, 'sda1', '9'))
        self.assertEqual(str(call[1][-1]), '1 second: test connect')

    def test_call_catches_ReplicationException(self):

        def connect(self):
            raise exceptions.ReplicationException('test connect')

        with mock.patch.object(ssync_sender.Sender, 'connect', connect):
            node = dict(ip='1.2.3.4', port=5678, device='sda1')
            job = dict(partition='9')
            self.sender = ssync_sender.Sender(self.replicator, node, job, None)
            self.sender.suffixes = ['abc']
            self.assertFalse(self.sender())
        call = self.replicator.logger.error.mock_calls[0]
        self.assertEqual(
            call[1][:-1], ('%s:%s/%s/%s %s', '1.2.3.4', 5678, 'sda1', '9'))
        self.assertEqual(str(call[1][-1]), 'test connect')

    def test_call_catches_other_exceptions(self):
        node = dict(ip='1.2.3.4', port=5678, device='sda1')
        job = dict(partition='9')
        self.sender = ssync_sender.Sender(self.replicator, node, job, None)
        self.sender.suffixes = ['abc']
        self.sender.connect = 'cause exception'
        self.assertFalse(self.sender())
        call = self.replicator.logger.exception.mock_calls[0]
        self.assertEqual(
            call[1],
            ('%s:%s/%s/%s EXCEPTION in replication.Sender', '1.2.3.4', 5678,
             'sda1', '9'))

    def test_call_catches_exception_handling_exception(self):
        node = dict(ip='1.2.3.4', port=5678, device='sda1')
        job = None  # Will cause inside exception handler to fail
        self.sender = ssync_sender.Sender(self.replicator, node, job, None)
        self.sender.suffixes = ['abc']
        self.sender.connect = 'cause exception'
        self.assertFalse(self.sender())
        self.replicator.logger.exception.assert_called_once_with(
            'EXCEPTION in replication.Sender')

    def test_call_calls_others(self):
        self.sender.suffixes = ['abc']
        self.sender.connect = mock.MagicMock()
        self.sender.missing_check = mock.MagicMock()
        self.sender.updates = mock.MagicMock()
        self.sender.disconnect = mock.MagicMock()
        self.assertTrue(self.sender())
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
        self.assertFalse(self.sender())
        self.sender.connect.assert_called_once_with()
        self.sender.missing_check.assert_called_once_with()
        self.sender.updates.assert_called_once_with()
        self.sender.disconnect.assert_called_once_with()

    @patch_policies
    def test_connect(self):
        node = dict(ip='1.2.3.4', port=5678, device='sda1')
        job = dict(partition='9', policy_idx=1)
        self.sender = ssync_sender.Sender(self.replicator, node, job, None)
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
                mock.call('REPLICATION', '/sda1/9'),
            ],
            'putheader': [
                mock.call('Transfer-Encoding', 'chunked'),
                mock.call('X-Backend-Storage-Policy-Index', 1),
            ],
            'endheaders': [mock.call()],
        }
        for method_name, expected_calls in expectations.items():
            mock_method = getattr(mock_conn, method_name)
            self.assertEquals(expected_calls, mock_method.mock_calls,
                              'connection method "%s" got %r not %r' % (
                                  method_name, mock_method.mock_calls,
                                  expected_calls))

    def test_connect_send_timeout(self):
        self.replicator.conn_timeout = 0.01
        node = dict(ip='1.2.3.4', port=5678, device='sda1')
        job = dict(partition='9')
        self.sender = ssync_sender.Sender(self.replicator, node, job, None)
        self.sender.suffixes = ['abc']

        def putrequest(*args, **kwargs):
            eventlet.sleep(0.1)

        with mock.patch.object(
                ssync_sender.bufferedhttp.BufferedHTTPConnection,
                'putrequest', putrequest):
            self.assertFalse(self.sender())
        call = self.replicator.logger.error.mock_calls[0]
        self.assertEqual(
            call[1][:-1], ('%s:%s/%s/%s %s', '1.2.3.4', 5678, 'sda1', '9'))
        self.assertEqual(str(call[1][-1]), '0.01 seconds: connect send')

    def test_connect_receive_timeout(self):
        self.replicator.node_timeout = 0.02
        node = dict(ip='1.2.3.4', port=5678, device='sda1')
        job = dict(partition='9')
        self.sender = ssync_sender.Sender(self.replicator, node, job, None)
        self.sender.suffixes = ['abc']

        class FakeBufferedHTTPConnection(NullBufferedHTTPConnection):

            def getresponse(*args, **kwargs):
                eventlet.sleep(0.1)

        with mock.patch.object(
                ssync_sender.bufferedhttp, 'BufferedHTTPConnection',
                FakeBufferedHTTPConnection):
            self.assertFalse(self.sender())
        call = self.replicator.logger.error.mock_calls[0]
        self.assertEqual(
            call[1][:-1], ('%s:%s/%s/%s %s', '1.2.3.4', 5678, 'sda1', '9'))
        self.assertEqual(str(call[1][-1]), '0.02 seconds: connect receive')

    def test_connect_bad_status(self):
        self.replicator.node_timeout = 0.02
        node = dict(ip='1.2.3.4', port=5678, device='sda1')
        job = dict(partition='9')
        self.sender = ssync_sender.Sender(self.replicator, node, job, None)
        self.sender.suffixes = ['abc']

        class FakeBufferedHTTPConnection(NullBufferedHTTPConnection):
            def getresponse(*args, **kwargs):
                response = FakeResponse()
                response.status = 503
                return response

        with mock.patch.object(
                ssync_sender.bufferedhttp, 'BufferedHTTPConnection',
                FakeBufferedHTTPConnection):
            self.assertFalse(self.sender())
        call = self.replicator.logger.error.mock_calls[0]
        self.assertEqual(
            call[1][:-1], ('%s:%s/%s/%s %s', '1.2.3.4', 5678, 'sda1', '9'))
        self.assertEqual(str(call[1][-1]), 'Expected status 200; got 503')

    def test_readline_newline_in_buffer(self):
        self.sender.response_buffer = 'Has a newline already.\r\nOkay.'
        self.assertEqual(self.sender.readline(), 'Has a newline already.\r\n')
        self.assertEqual(self.sender.response_buffer, 'Okay.')

    def test_readline_buffer_exceeds_network_chunk_size_somehow(self):
        self.replicator.network_chunk_size = 2
        self.sender.response_buffer = '1234567890'
        self.assertEqual(self.sender.readline(), '1234567890')
        self.assertEqual(self.sender.response_buffer, '')

    def test_readline_at_start_of_chunk(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = StringIO.StringIO('2\r\nx\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')

    def test_readline_chunk_with_extension(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = StringIO.StringIO(
            '2 ; chunk=extension\r\nx\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')

    def test_readline_broken_chunk(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = StringIO.StringIO('q\r\nx\n\r\n')
        self.assertRaises(
            exceptions.ReplicationException, self.sender.readline)
        self.assertTrue(self.sender.response.close_called)

    def test_readline_terminated_chunk(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = StringIO.StringIO('b\r\nnot enough')
        self.assertRaises(
            exceptions.ReplicationException, self.sender.readline)
        self.assertTrue(self.sender.response.close_called)

    def test_readline_all(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = StringIO.StringIO('2\r\nx\n\r\n0\r\n\r\n')
        self.assertEqual(self.sender.readline(), 'x\n')
        self.assertEqual(self.sender.readline(), '')
        self.assertEqual(self.sender.readline(), '')

    def test_readline_all_trailing_not_newline_termed(self):
        self.sender.response = FakeResponse()
        self.sender.response.fp = StringIO.StringIO(
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
        def yield_hashes(device, partition, policy_idx, suffixes=None):
            if (device != 'dev' or partition != '9' or policy_idx != 0 or
                    suffixes != ['abc', 'def']):
                yield  # Just here to make this a generator
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy_idx, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {'device': 'dev', 'partition': '9'}
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
        self.assertEqual(self.sender.send_list, [])

    def test_missing_check_has_suffixes(self):
        def yield_hashes(device, partition, policy_idx, suffixes=None):
            if (device == 'dev' and partition == '9' and policy_idx == 0 and
                    suffixes == ['abc', 'def']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '1380144470.00000')
                yield (
                    '/srv/node/dev/objects/9/def/'
                    '9d41d8cd98f00b204e9800998ecf0def',
                    '9d41d8cd98f00b204e9800998ecf0def',
                    '1380144472.22222')
                yield (
                    '/srv/node/dev/objects/9/def/'
                    '9d41d8cd98f00b204e9800998ecf1def',
                    '9d41d8cd98f00b204e9800998ecf1def',
                    '1380144474.44444')
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy_idx, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {'device': 'dev', 'partition': '9'}
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
            '33\r\n9d41d8cd98f00b204e9800998ecf1def 1380144474.44444\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.send_list, [])

    def test_missing_check_far_end_disconnect(self):
        def yield_hashes(device, partition, policy_idx, suffixes=None):
            if (device == 'dev' and partition == '9' and policy_idx == 0 and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '1380144470.00000')
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy_idx, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {'device': 'dev', 'partition': '9'}
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

    def test_missing_check_far_end_disconnect2(self):
        def yield_hashes(device, partition, policy_idx, suffixes=None):
            if (device == 'dev' and partition == '9' and policy_idx == 0 and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '1380144470.00000')
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy_idx, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {'device': 'dev', 'partition': '9'}
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

    def test_missing_check_far_end_unexpected(self):
        def yield_hashes(device, partition, policy_idx, suffixes=None):
            if (device == 'dev' and partition == '9' and policy_idx == 0 and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '1380144470.00000')
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy_idx, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {'device': 'dev', 'partition': '9'}
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

    def test_missing_check_send_list(self):
        def yield_hashes(device, partition, policy_idx, suffixes=None):
            if (device == 'dev' and partition == '9' and policy_idx == 0 and
                    suffixes == ['abc']):
                yield (
                    '/srv/node/dev/objects/9/abc/'
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '9d41d8cd98f00b204e9800998ecf0abc',
                    '1380144470.00000')
            else:
                raise Exception(
                    'No match for %r %r %r %r' % (device, partition,
                                                  policy_idx, suffixes))

        self.sender.connection = FakeConnection()
        self.sender.job = {'device': 'dev', 'partition': '9'}
        self.sender.suffixes = ['abc']
        self.sender.response = FakeResponse(
            chunk_body=(
                ':MISSING_CHECK: START\r\n'
                '0123abc\r\n'
                ':MISSING_CHECK: END\r\n'))
        self.sender.daemon._diskfile_mgr.yield_hashes = yield_hashes
        self.sender.missing_check()
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '17\r\n:MISSING_CHECK: START\r\n\r\n'
            '33\r\n9d41d8cd98f00b204e9800998ecf0abc 1380144470.00000\r\n\r\n'
            '15\r\n:MISSING_CHECK: END\r\n\r\n')
        self.assertEqual(self.sender.send_list, ['0123abc'])

    def test_updates_timeout(self):
        self.sender.connection = FakeConnection()
        self.sender.connection.send = lambda d: eventlet.sleep(1)
        self.sender.daemon.node_timeout = 0.01
        self.assertRaises(exceptions.MessageTimeout, self.sender.updates)

    def test_updates_empty_send_list(self):
        self.sender.connection = FakeConnection()
        self.sender.send_list = []
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
        self.sender.send_list = []
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
        self.sender.send_list = []
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
        self.sender.job = {'device': device, 'partition': part}
        self.sender.node = {}
        self.sender.send_list = [object_hash]
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

    def test_updates_put(self):
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        df = self._make_open_diskfile(device, part, *object_parts)
        object_hash = utils.hash_path(*object_parts)
        expected = df.get_metadata()
        self.sender.connection = FakeConnection()
        self.sender.job = {'device': device, 'partition': part}
        self.sender.node = {}
        self.sender.send_list = [object_hash]
        self.sender.send_delete = mock.MagicMock()
        self.sender.send_put = mock.MagicMock()
        self.sender.response = FakeResponse(
            chunk_body=(
                ':UPDATES: START\r\n'
                ':UPDATES: END\r\n'))
        self.sender.updates()
        self.assertEqual(self.sender.send_delete.mock_calls, [])
        self.assertEqual(1, len(self.sender.send_put.mock_calls))
        args, _kwargs = self.sender.send_put.call_args
        path, df = args
        self.assertEqual(path, '/a/c/o')
        self.assert_(isinstance(df, diskfile.DiskFile))
        self.assertEqual(expected, df.get_metadata())
        # note that the put line isn't actually sent since we mock send_put;
        # send_put is tested separately.
        self.assertEqual(
            ''.join(self.sender.connection.sent),
            '11\r\n:UPDATES: START\r\n\r\n'
            'f\r\n:UPDATES: END\r\n\r\n')

    @patch_policies
    def test_updates_storage_policy_index(self):
        device = 'dev'
        part = '9'
        object_parts = ('a', 'c', 'o')
        df = self._make_open_diskfile(device, part, *object_parts,
                                      policy_idx=1)
        object_hash = utils.hash_path(*object_parts)
        expected = df.get_metadata()
        self.sender.connection = FakeConnection()
        self.sender.job = {'device': device, 'partition': part,
                           'policy_idx': 1}
        self.sender.node = {}
        self.sender.send_list = [object_hash]
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
        self.assert_(isinstance(df, diskfile.DiskFile))
        self.assertEqual(expected, df.get_metadata())
        self.assertEqual(os.path.join(self.testdir, 'dev/objects-1/9/',
                                      object_hash[-3:], object_hash),
                         df._datadir)

    def test_updates_read_response_timeout_start(self):
        self.sender.connection = FakeConnection()
        self.sender.send_list = []
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
        self.sender.send_list = []
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
        self.sender.send_list = []
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
        self.sender.send_list = []
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
        self.sender.send_list = []
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
        self.sender.send_list = []
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
            self.sender.send_delete('/a/c/o', '1381679759.90941')
        except exceptions.MessageTimeout as err:
            exc = err
        self.assertEqual(str(exc), '0.01 seconds: send_delete')

    def test_send_delete(self):
        self.sender.connection = FakeConnection()
        self.sender.send_delete('/a/c/o', '1381679759.90941')
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
        body = 'test'
        extra_metadata = {'Some-Other-Header': 'value'}
        df = self._make_open_diskfile(body=body,
                                      extra_metadata=extra_metadata)
        expected = dict(df.get_metadata())
        expected['body'] = body
        expected['chunk_size'] = len(body)
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


if __name__ == '__main__':
    unittest.main()
