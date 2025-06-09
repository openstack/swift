# Copyright (c) 2010-2015 OpenStack Foundation
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

import contextlib
from unittest import mock

import time
import unittest
import os

from swift.common.base_storage_server import BaseStorageServer, \
    timing_stats, labeled_timing_stats
from swift.common.swob import Request, Response, HTTPInsufficientStorage
from test.debug_logger import debug_logger, debug_labeled_statsd_client

from tempfile import mkdtemp
from swift import __version__ as swift_version
from swift.common.utils import get_logger, public, replication
from shutil import rmtree


class FakeOPTIONS(BaseStorageServer):

    server_type = 'test-server'

    def __init__(self, conf, logger=None):
        super(FakeOPTIONS, self).__init__(conf)
        self.logger = logger or get_logger(conf, log_route='test-server')


class FakeANOTHER(FakeOPTIONS):

    @public
    def ANOTHER(self):
        """this is to test adding to allowed_methods"""
        pass

    @replication
    @public
    def REPLICATE(self):
        """this is to test replication_server"""
        pass

    @public
    @replication
    def REPLICATE2(self):
        """this is to test replication_server"""
        pass


class MockLabeledTimingController(object):
    def __init__(self, status, extra_labels=None):
        self.statsd = debug_labeled_statsd_client({})
        self.status = status
        self.extra_labels = extra_labels or {}

    def _update_labels(self, req, labels):
        labels.update(self.extra_labels)

    @labeled_timing_stats(metric='my_timing_metric')
    def handle_req(self, req, timing_stats_labels):
        self._update_labels(req, timing_stats_labels)
        if isinstance(self.status, Exception):
            raise self.status
        return Response(status=self.status)


class TestLabeledTimingStatsDecorator(unittest.TestCase):

    @contextlib.contextmanager
    def _patch_time(self):
        now = time.time()
        with mock.patch('swift.common.utils.time.time', return_value=now):
            yield now

    def test_labeled_timing_stats_get_200(self):
        req = Request.blank('/v1/a/c/o')
        mock_controller = MockLabeledTimingController(200)
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'GET',
                    'status': 200,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_head_500(self):
        req = Request.blank('/v1/a/c/o', method='HEAD')
        mock_controller = MockLabeledTimingController(500)
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'HEAD',
                    'status': 500,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_head_507_exception(self):
        req = Request.blank('/v1/a/c/o', method='HEAD')
        mock_controller = MockLabeledTimingController(
            HTTPInsufficientStorage())
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'HEAD',
                    'status': 507,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_extra_labels(self):
        req = Request.blank('/v1/AUTH_test/c/o')
        mock_controller = MockLabeledTimingController(
            206, extra_labels={'account': 'AUTH_test'})
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'account': 'AUTH_test',
                    'method': 'GET',
                    'status': 206,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_can_not_override_status(self):
        req = Request.blank('/v1/AUTH_test/c/o')
        mock_controller = MockLabeledTimingController(
            404, extra_labels={'status': 200})
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'GET',
                    'status': 404,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_can_not_override_method(self):
        req = Request.blank('/v1/AUTH_test/c/o', method='POST')
        mock_controller = MockLabeledTimingController(
            412, extra_labels={'method': 'GET'})
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'POST',
                    'status': 412,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_really_can_not_override_method(self):

        class MutilatingController(MockLabeledTimingController):

            def _update_labels(self, req, labels):
                req.method = 'BANANA'

        req = Request.blank('/v1/AUTH_test/c/o', method='POST')
        mock_controller = MutilatingController(412)
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual('BANANA', req.method)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'POST',
                    'status': 412,
                }
            })]},
            mock_controller.statsd.calls)

    def test_labeled_timing_stats_cannot_remove_labels(self):

        class MutilatingController(MockLabeledTimingController):

            def _update_labels(self, req, labels):
                labels.clear()

        req = Request.blank('/v1/AUTH_test/c/o', method='DELETE')
        mock_controller = MutilatingController('42 bad stuff')
        with self._patch_time() as now:
            mock_controller.handle_req(req)
        self.assertEqual(
            {'timing_since': [(('my_timing_metric', now), {
                'labels': {
                    'method': 'DELETE',
                    # resp.status_int knows how to do it
                    'status': 42,
                }
            })]},
            mock_controller.statsd.calls)


class TestTimingStatsDecorators(unittest.TestCase):
    def test_timing_stats(self):
        class MockController(object):
            def __init__(mock_self, status):
                mock_self.status = status
                mock_self.logger = debug_logger()

            @timing_stats()
            def METHOD(mock_self):
                if isinstance(mock_self.status, Exception):
                    raise mock_self.status
                return Response(status=mock_self.status)

        now = time.time()
        mock_controller = MockController(200)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(400)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(404)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(412)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(416)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(500)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual(
            {'timing_since': [(('METHOD.errors.timing', now), {})]},
            mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(507)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual(
            {'timing_since': [(('METHOD.errors.timing', now), {})]},
            mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(
            HTTPInsufficientStorage())
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual(
            {'timing_since': [(('METHOD.errors.timing', now), {})]},
            mock_controller.logger.statsd_client.calls)


class TestBaseStorageServer(unittest.TestCase):
    """Test swift.common.base_storage_server"""

    def setUp(self):
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(self.tmpdir,
                                    'tmp_test_base_storage_server')

    def tearDown(self):
        """Tear down for testing swift.common.base_storage_server"""
        rmtree(self.tmpdir)

    def test_server_type(self):
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        baseserver = BaseStorageServer(conf)
        msg = 'Storage nodes have not implemented the Server type.'
        try:
            baseserver.server_type
        except NotImplementedError as e:
            self.assertEqual(str(e), msg)

    def test_allowed_methods(self):
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'false'}

        # test what's available in the base class
        allowed_methods_test = FakeOPTIONS(conf).allowed_methods
        self.assertEqual(allowed_methods_test, ['OPTIONS'])

        # test that a subclass can add allowed methods
        allowed_methods_test = FakeANOTHER(conf).allowed_methods
        allowed_methods_test.sort()
        self.assertEqual(allowed_methods_test, [
            'ANOTHER', 'OPTIONS'])

        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'true'}

        # test what's available in the base class
        allowed_methods_test = FakeOPTIONS(conf).allowed_methods
        self.assertEqual(allowed_methods_test, ['OPTIONS'])

        # test that a subclass can add allowed methods
        allowed_methods_test = FakeANOTHER(conf).allowed_methods
        self.assertEqual(allowed_methods_test, [
            'ANOTHER', 'OPTIONS', 'REPLICATE', 'REPLICATE2'])

        conf = {'devices': self.testdir, 'mount_check': 'false'}

        # test what's available in the base class
        allowed_methods_test = FakeOPTIONS(conf).allowed_methods
        self.assertEqual(allowed_methods_test, ['OPTIONS'])

        # test that a subclass can add allowed methods
        allowed_methods_test = FakeANOTHER(conf).allowed_methods
        allowed_methods_test.sort()
        self.assertEqual(allowed_methods_test, [
            'ANOTHER', 'OPTIONS', 'REPLICATE', 'REPLICATE2'])

    def test_OPTIONS_error(self):
        msg = 'Storage nodes have not implemented the Server type.'
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'false'}

        baseserver = BaseStorageServer(conf)
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0

        try:
            baseserver.OPTIONS(req)
        except NotImplementedError as e:
            self.assertEqual(str(e), msg)

    def test_OPTIONS(self):
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'false'}
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0
        resp = FakeOPTIONS(conf).OPTIONS(req)
        self.assertEqual(resp.headers['Allow'], 'OPTIONS')
        self.assertEqual(resp.headers['Server'],
                         'test-server/' + swift_version)
