# Copyright (c) 2022 NVIDIA
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

# Used by get_swift_info and register_swift_info to store information about
# the swift cluster.
import time
import unittest
from collections import defaultdict

import mock

from swift.common.middleware import backend_ratelimit
from swift.common.middleware.backend_ratelimit import \
    BackendRateLimitMiddleware
from swift.common.swob import Request, HTTPOk
from test.debug_logger import debug_logger
from test.unit.common.middleware.helpers import FakeSwift


class FakeApp(object):
    def __init__(self):
        self.calls = []

    def __call__(self, env, start_response):
        start_response('200 OK', {})
        return ['']


class TestBackendRatelimitMiddleware(unittest.TestCase):
    def setUp(self):
        super(TestBackendRatelimitMiddleware, self).setUp()
        self.swift = FakeSwift()

    def test_init(self):
        conf = {}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual(0.0, rl.requests_per_device_per_second)
        self.assertEqual(1.0, rl.requests_per_device_rate_buffer)

        conf = {'requests_per_device_per_second': 1.3,
                'requests_per_device_rate_buffer': 2.4}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual(1.3, rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)

        conf = {'requests_per_device_per_second': -1}
        factory = backend_ratelimit.filter_factory(conf)
        with self.assertRaises(ValueError) as cm:
            factory(self.swift)
        self.assertEqual(
            'Value must be a non-negative float number, not "-1.0".',
            str(cm.exception))

        conf = {'requests_per_device_rate_buffer': -1}
        factory = backend_ratelimit.filter_factory(conf)
        with self.assertRaises(ValueError):
            factory(self.swift)
        self.assertEqual(
            'Value must be a non-negative float number, not "-1.0".',
            str(cm.exception))

    def _do_test_ratelimit(self, method, req_per_sec, rate_buffer):
        # send 20 requests, time increments by 0.01 between each request
        start = time.time()
        fake_time = [start]

        def mock_time():
            return fake_time[0]

        app = FakeSwift()
        logger = debug_logger()
        # apply a ratelimit
        conf = {'requests_per_device_per_second': req_per_sec,
                'requests_per_device_rate_buffer': rate_buffer}
        rl = BackendRateLimitMiddleware(app, conf, logger)
        success = defaultdict(int)
        ratelimited = 0

        with mock.patch('swift.common.utils.time.time', mock_time):
            for i in range(20):
                for dev in ['sda1', 'sda2', 'sda3']:
                    req = Request.blank('/%s/99/a/c/o' % dev,
                                        environ={'REQUEST_METHOD': method})
                    app.register(method, req.path, HTTPOk, {})
                    resp = req.get_response(rl)
                    if resp.status_int == 200:
                        success[dev] += 1
                    else:
                        self.assertEqual(529, resp.status_int)
                        self.assertTrue(resp.status.startswith(
                            '529 Too Many Backend Requests'))
                        ratelimited += 1
                fake_time[0] += 0.01
        self.assertEqual(
            ratelimited,
            logger.get_increment_counts().get('backend.ratelimit', 0))
        return success

    def test_ratelimited(self):
        def do_test_ratelimit(method):
            # no rate-limiting
            success_per_dev = self._do_test_ratelimit(method, 0, 0)
            self.assertEqual([20] * 3, list(success_per_dev.values()))

            # rate-limited
            success_per_dev = self._do_test_ratelimit(method, 1, 0)
            self.assertEqual([1] * 3, list(success_per_dev.values()))

            success_per_dev = self._do_test_ratelimit(method, 10, 0)
            self.assertEqual([2] * 3, list(success_per_dev.values()))

            success_per_dev = self._do_test_ratelimit(method, 101, 0)
            self.assertEqual([20] * 3, list(success_per_dev.values()))

            # startup burst of 1 seconds allowance plus current allowance...
            success_per_dev = self._do_test_ratelimit(method, 1, 1)
            self.assertEqual([2] * 3, list(success_per_dev.values()))
            success_per_dev = self._do_test_ratelimit(method, 10, 1)
            self.assertEqual([12] * 3, list(success_per_dev.values()))

        do_test_ratelimit('GET')
        do_test_ratelimit('HEAD')
        do_test_ratelimit('PUT')
        do_test_ratelimit('POST')
        do_test_ratelimit('DELETE')
        do_test_ratelimit('UPDATE')
        do_test_ratelimit('REPLICATE')

    def test_not_ratelimited(self):
        def do_test_no_ratelimit(method):
            # verify no rate-limiting
            success_per_dev = self._do_test_ratelimit(method, 1, 0)
            self.assertEqual([20] * 3, list(success_per_dev.values()))

        do_test_no_ratelimit('OPTIONS')
        do_test_no_ratelimit('SSYNC')

    def test_unhandled_request(self):
        app = FakeSwift()
        logger = debug_logger()
        conf = {'requests_per_device_per_second': 1,
                'requests_per_device_rate_buffer': 1}

        def do_test(path):
            rl = BackendRateLimitMiddleware(app, conf, logger)
            req = Request.blank(path)
            app.register('GET', req.path, HTTPOk, {})
            for i in range(10):
                resp = req.get_response(rl)
                self.assertEqual(200, resp.status_int)
            self.assertEqual(
                0, logger.get_increment_counts().get('backend.ratelimit', 0))

        do_test('/recon/version')
        do_test('/healthcheck')
        do_test('/v1/a/c/o')
