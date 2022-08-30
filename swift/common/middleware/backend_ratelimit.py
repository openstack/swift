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

import time
from collections import defaultdict

from swift.common.request_helpers import split_and_validate_path
from swift.common.swob import Request, HTTPTooManyBackendRequests
from swift.common.utils import get_logger, non_negative_float, \
    EventletRateLimiter

RATE_LIMITED_METHODS = ('GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'UPDATE',
                        'REPLICATE')


class BackendRateLimitMiddleware(object):
    """
    Backend rate-limiting middleware.

    Rate-limits requests to backend storage node devices. Each device is
    independently rate-limited. All requests with a 'GET', 'HEAD', 'PUT',
    'POST', 'DELETE', 'UPDATE' or 'REPLICATE' method are included in a device's
    rate limit.

    If a request would cause the rate-limit to be exceeded then a response with
    a 529 status code is returned.
    """
    def __init__(self, app, conf, logger=None):
        self.app = app
        self.logger = logger or get_logger(conf, log_route='backend_ratelimit')
        self.requests_per_device_per_second = non_negative_float(
            conf.get('requests_per_device_per_second', 0.0))
        self.requests_per_device_rate_buffer = non_negative_float(
            conf.get('requests_per_device_rate_buffer', 1.0))

        # map device -> RateLimiter
        self.rate_limiters = defaultdict(
            lambda: EventletRateLimiter(
                max_rate=self.requests_per_device_per_second,
                rate_buffer=self.requests_per_device_rate_buffer,
                running_time=time.time(),
                burst_after_idle=True))

    def __call__(self, env, start_response):
        """
        WSGI entry point.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        req = Request(env)
        handler = self.app
        if req.method in RATE_LIMITED_METHODS:
            try:
                device, partition, _ = split_and_validate_path(req, 1, 3, True)
                int(partition)  # check it's a valid partition
                rate_limiter = self.rate_limiters[device]
                if not rate_limiter.is_allowed():
                    self.logger.increment('backend.ratelimit')
                    handler = HTTPTooManyBackendRequests()
            except Exception:  # noqa
                # request may not have device/partition e.g. a healthcheck req
                pass
        return handler(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def backend_ratelimit_filter(app):
        return BackendRateLimitMiddleware(app, conf)

    return backend_ratelimit_filter
