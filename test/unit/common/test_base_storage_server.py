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
    timing_stats, labeled_timing_stats, request_timing_logging, \
    TimingBreakdown
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


class TestTimingBreakdown(unittest.TestCase):

    def test_init_default_start_time(self):
        with mock.patch('time.time', return_value=1000.0):
            tb = TimingBreakdown()
        self.assertEqual(tb._breakdown, [])
        self.assertEqual(tb._last_time, 1000.0)

    def test_init_explicit_start_time(self):
        tb = TimingBreakdown(start_time=500.0)
        self.assertEqual(tb._breakdown, [])
        self.assertEqual(tb._last_time, 500.0)

    def test_record_single_event(self):
        tb = TimingBreakdown(start_time=1000.0)
        with mock.patch('time.time', return_value=1001.5):
            tb.record('event1')
        self.assertEqual(tb.breakdown, [('event1', 1.5)])
        self.assertEqual(tb._last_time, 1001.5)

    def test_record_multiple_events(self):
        tb = TimingBreakdown(start_time=1000.0)
        with mock.patch('time.time', side_effect=[1001.5, 1003.0]):
            tb.record('event1')
            tb.record('event2')
        expected = [('event1', 1.5), ('event2', 1.5)]
        self.assertEqual(tb.breakdown, expected)
        self.assertEqual(tb._last_time, 1003.0)

    def test_record_preserves_order(self):
        tb = TimingBreakdown(start_time=1000.0)
        with mock.patch('time.time', side_effect=[1001.0, 1002.0, 1003.0]):
            tb.record('third')
            tb.record('first')
            tb.record('second')
        # Check that order is preserved
        breakdown_events = [event for event, duration in tb.breakdown]
        self.assertEqual(breakdown_events, ['third', 'first', 'second'])

    def test_record_allows_duplicate_keys(self):
        tb = TimingBreakdown(start_time=1000.0)
        with mock.patch('time.time', side_effect=[1001.0, 1002.5]):
            tb.record('event1')
            tb.record('event1')  # Same key again
        expected = [('event1', 1.0), ('event1', 1.5)]
        self.assertEqual(tb.breakdown, expected)

    def test_zero_elapsed_time(self):
        tb = TimingBreakdown(start_time=1000.0)
        with mock.patch('time.time', return_value=1000.0):  # Same time
            tb.record('instant')
        self.assertEqual(tb.breakdown, [('instant', 0.0)])


class TestRequestTimingLoggingDecorator(unittest.TestCase):

    def test_slow_operation_with_breakdown(self):

        class SlowController(object):
            slow_threshold = 0.1

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_slow_req(self, req, timing_breakdown):
                # Simulate operations with specific durations
                timing_breakdown.record('operation_1')  # 0.011s
                timing_breakdown.record('operation_2')  # 0.070s
                timing_breakdown.record('operation_3')  # 0.020s
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = SlowController(200)
        with mock.patch('time.time', side_effect=[
                1000.0,      # start_time in decorator
                1000.011,    # operation_1 record
                1000.081,    # operation_2 record
                1000.101,    # operation_3 record
                1000.101]):  # total_time in decorator
            controller.handle_slow_req(req)

        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertEqual(['Slow GET (0.101s) for /v1/a/c/o, status 200, '
                          'start_time=1000.000, operation_1=0.011s, '
                          'operation_2=0.070s, operation_3=0.020s'],
                         error_lines)

    def test_slow_operation_empty_breakdown(self):

        class SlowController(object):
            slow_threshold = 0.1

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_slow_req(self, req, timing_breakdown):
                # Don't record any timing events
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = SlowController(200)
        now = 1000.0
        with mock.patch('time.time', side_effect=[now, now + 0.15]):
            controller.handle_slow_req(req)

        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertEqual(['Slow GET (0.150s) for /v1/a/c/o, status 200, '
                          'start_time=1000.000, '],
                         error_lines)

    def test_fast_operation_no_warning(self):

        class SlowController(object):
            slow_threshold = 0.1

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_slow_req(self, req, timing_breakdown):
                # Simulate a fast operation
                timing_breakdown.record('operation_1')  # 0.099s
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = SlowController(200)
        # Mock time.time() calls: start_time, operation_1, end_time
        with mock.patch('time.time', side_effect=[1000.0, 1000.099, 1000.099]):
            controller.handle_slow_req(req)

        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertFalse(error_lines)

    def test_disabled_threshold_no_warning(self):

        class DisabledController(object):
            slow_threshold = -1  # Disabled threshold

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_slow_req(self, req, timing_breakdown):
                # Simulate very slow operations
                timing_breakdown.record('operation_1')  # 5.0s
                timing_breakdown.record('operation_2')  # 10.0s
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = DisabledController(200)
        with mock.patch('time.time', side_effect=[
                1000.0,      # start_time in decorator
                1005.0,      # operation_1 record (5s)
                1015.0,      # operation_2 record (10s)
                1020.0]):    # total_time in decorator (20s total)
            controller.handle_slow_req(req)

        # Should not log any warnings despite very slow operation
        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertFalse(error_lines)

    def test_zero_threshold_logs_everything(self):

        class ZeroThresholdController(object):
            slow_threshold = 0

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_slow_req(self, req, timing_breakdown):
                # Simulate tiny operation
                timing_breakdown.record('operation_1')  # 0.001s
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = ZeroThresholdController(200)
        with mock.patch('time.time', side_effect=[
                1000.0,      # start_time in decorator
                1000.001,    # operation_1 record
                1000.002]):  # total_time in decorator
            controller.handle_slow_req(req)

        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertEqual(['Slow GET (0.002s) for /v1/a/c/o, status 200, '
                          'start_time=1000.000, operation_1=0.001s'],
                         error_lines)

    def test_no_slow_threshold_attr(self):

        class NormalController(object):
            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            def handle_req(self, req, timing_breakdown):
                with mock.patch('time.time', return_value=1059.0):
                    timing_breakdown.record('operation_1')  # 59.0s
                return Response(status=self.status)

        controller = NormalController(200)
        with self.assertRaises(TypeError):
            # Decorator is applied without threshold_attr
            _ = request_timing_logging()(controller.handle_req)

    def test_missing_threshold_value(self):

        class NormalController(object):
            # No slow_threshold attribute

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_req(self, req, timing_breakdown):
                with mock.patch('time.time', return_value=1059.0):
                    timing_breakdown.record('operation_1')  # 59.0s
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = NormalController(200)

        # Should raise AttributeError: 'slow_threshold' doesn't exist
        with self.assertRaises(AttributeError):
            controller.handle_req(req)

    def test_slow_operation_with_exception(self):

        class SlowExceptionController(object):
            slow_threshold = 0.1

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_req(self, req, timing_breakdown):
                # Simulate operation before error
                timing_breakdown.record('before_error')  # 0.05s
                raise HTTPInsufficientStorage()

        req = Request.blank('/v1/a/c/o')
        controller = SlowExceptionController(None)
        # Mock time.time() calls: start_time, before_error, end_time
        with mock.patch('time.time', side_effect=[1000.0, 1000.05, 1000.15]):
            controller.handle_req(req)

        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertEqual(['Slow GET (0.150s) for /v1/a/c/o, status 507, '
                          'start_time=1000.000, before_error=0.050s'],
                         error_lines)

    def test_slow_operation_with_duplicate_events(self):

        class SlowController(object):
            slow_threshold = 0.1

            def __init__(self, status):
                self.status = status
                self.logger = debug_logger()

            @request_timing_logging(threshold_attr='slow_threshold')
            def handle_slow_req(self, req, timing_breakdown):
                # Simulate operations with duplicate event names
                timing_breakdown.record('db_query')  # 0.030s
                timing_breakdown.record('cache_check')  # 0.020s
                timing_breakdown.record('db_query')  # 0.040s
                return Response(status=self.status)

        req = Request.blank('/v1/a/c/o')
        controller = SlowController(200)
        with mock.patch('time.time', side_effect=[
                1000.0,      # start_time in decorator
                1000.030,    # first db_query record
                1000.050,    # cache_check record
                1000.090,    # second db_query record
                1000.110]):  # total_time in decorator
            controller.handle_slow_req(req)

        error_lines = controller.logger.get_lines_for_level('warning')
        self.assertEqual(['Slow GET (0.110s) for /v1/a/c/o, status 200, '
                          'start_time=1000.000, db_query=0.030s, '
                          'cache_check=0.020s, db_query=0.040s'],
                         error_lines)


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
