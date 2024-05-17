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
import os
import time

from swift.common.request_helpers import split_and_validate_path
from swift.common.swob import Request, HTTPTooManyBackendRequests, \
    HTTPException
from swift.common.utils import get_logger, non_negative_float, \
    EventletRateLimiter, readconf

RATE_LIMITED_METHODS = ('GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'UPDATE',
                        'REPLICATE')
BACKEND_RATELIMIT_CONFIG_SECTION = 'backend_ratelimit'
DEFAULT_BACKEND_RATELIMIT_CONF_FILE = 'backend-ratelimit.conf'
DEFAULT_CONFIG_RELOAD_INTERVAL = 60.0
DEFAULT_REQUESTS_PER_DEVICE_PER_SECOND = 0.0
DEFAULT_REQUESTS_PER_DEVICE_RATE_BUFFER = 1.0


class BackendRateLimitMiddleware(object):
    """
    Backend rate-limiting middleware.

    Rate-limits requests to backend storage node devices. Each (device, request
    method) combination is independently rate-limited. All requests with a
    'GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'UPDATE' or 'REPLICATE' method are
    rate limited on a per-device basis by both a method-specific rate and an
    overall device rate limit.

    If a request would cause the rate-limit to be exceeded for the method
    and/or device then a response with a 529 status code is returned.
    """
    def __init__(self, app, filter_conf, logger=None):
        self.app = app
        self.filter_conf = filter_conf
        self.logger = logger or get_logger(self.filter_conf,
                                           log_route='backend_ratelimit')
        self.requests_per_device_rate_buffer = \
            DEFAULT_REQUESTS_PER_DEVICE_RATE_BUFFER
        # map (device, method) -> rate
        self.requests_per_device_per_second = {}
        # map (device, method) -> RateLimiter, populated on-demand
        self.rate_limiters = {}

        # some config options are *only* read from filter conf at startup...
        default_conf_path = os.path.join(
            self.filter_conf.get('swift_dir', '/etc/swift'),
            DEFAULT_BACKEND_RATELIMIT_CONF_FILE)
        try:
            self.conf_path = self.filter_conf['backend_ratelimit_conf_path']
            self.is_config_file_expected = True
        except KeyError:
            self.conf_path = default_conf_path
            self.is_config_file_expected = False
        self.config_reload_interval = non_negative_float(
            filter_conf.get('config_reload_interval',
                            DEFAULT_CONFIG_RELOAD_INTERVAL))

        # other conf options are read from filter section at startup but may
        # also be overridden by options in a separate config file...
        self._last_config_reload_attempt = time.time()
        self._apply_config(self.filter_conf)
        self._load_config_file()

    def _refresh_ratelimiters(self):
        # note: if we ever wanted to prune the ratelimiters (in case devices
        # have been removed) we could inspect each ratelimiter's running_time
        # and remove those with very old running_time
        for (dev, method), rl in self.rate_limiters.items():
            rl.set_max_rate(self.requests_per_device_per_second[method])
            rl.set_rate_buffer(self.requests_per_device_rate_buffer)

    def _apply_config(self, conf):
        modified = False
        reqs_per_device_rate_buffer = non_negative_float(
            conf.get('requests_per_device_rate_buffer',
                     DEFAULT_REQUESTS_PER_DEVICE_RATE_BUFFER))

        # note: 'None' key holds the aggregate per-device limit for all methods
        reqs_per_device_per_second = {None: non_negative_float(
            conf.get('requests_per_device_per_second', 0.0))}
        for method in RATE_LIMITED_METHODS:
            val = non_negative_float(
                conf.get('%s_requests_per_device_per_second'
                         % method.lower(), 0.0))
            reqs_per_device_per_second[method] = val

        if reqs_per_device_rate_buffer != self.requests_per_device_rate_buffer:
            self.requests_per_device_rate_buffer = reqs_per_device_rate_buffer
            modified = True
        if reqs_per_device_per_second != self.requests_per_device_per_second:
            self.requests_per_device_per_second = reqs_per_device_per_second
            self.is_any_rate_limit_configured = any(
                self.requests_per_device_per_second.values())
            modified = True
        if modified:
            self._refresh_ratelimiters()
        return modified

    def _load_config_file(self):
        # If conf file can be read then apply its options to the filter conf
        # options, discarding *all* options previously loaded from the conf
        # file i.e. options deleted from the conf file will revert to the
        # filter conf value or default value. If the conf file cannot be read
        # or is invalid, then the current config is left unchanged.
        try:
            new_conf = dict(self.filter_conf)  # filter_conf not current conf
            new_conf.update(
                readconf(self.conf_path, BACKEND_RATELIMIT_CONFIG_SECTION))
            modified = self._apply_config(new_conf)
            if modified:
                self.logger.info('Loaded config file %s, config changed',
                                 self.conf_path)
            elif not self.is_config_file_expected:
                self.logger.info('Loaded new config file %s, config unchanged',
                                 self.conf_path)
            else:
                self.logger.debug(
                    'Loaded existing config file %s, config unchanged',
                    self.conf_path)
            self.is_config_file_expected = True
        except IOError as err:
            if self.is_config_file_expected:
                self.logger.warning(
                    'Failed to load config file, config unchanged: %s', err)
            self.is_config_file_expected = False
        except ValueError as err:
            # ...but if it exists it should be valid
            self.logger.warning('Invalid config file %s, config unchanged: %s',
                                self.conf_path, err)

    def _maybe_reload_config(self):
        if self.config_reload_interval:
            now = time.time()
            if (now - self._last_config_reload_attempt
                    >= self.config_reload_interval):
                try:
                    self._load_config_file()
                except Exception:  # noqa
                    self.logger.exception('Error reloading config file')
                finally:
                    # always reset last loaded time to avoid re-try storm
                    self._last_config_reload_attempt = now

    def _get_ratelimiter(self, device, method=None):
        """
        Get a rate limiter for the (device, method) combination. If a rate
        limiter does not yet exist for the given (device, method) combination
        then it is created and added to the map of rate limiters.

        :param: the device.
        :method: the request method; if None then the aggregate rate limiter
            for all requests to the device is returned.
        :returns: an instance of ``EventletRateLimiter``.
        """
        try:
            rl = self.rate_limiters[(device, method)]
        except KeyError:
            rl = EventletRateLimiter(
                max_rate=self.requests_per_device_per_second[method],
                rate_buffer=self.requests_per_device_rate_buffer,
                running_time=time.time(),
                burst_after_idle=True)
            self.rate_limiters[(device, method)] = rl
        return rl

    def _is_allowed(self, device, method):
        """
        Evaluate backend rate-limiting policies for the incoming request.

        A request is allowed when neither the per-(device, method) rate-limit
        nor the per-device rate-limit has been reached.

        Note: a request will be disallowed if the aggregate per-device
        rate-limit has been reached, even if the per-(device, method)
        rate-limit has not been reached for the request's method.

        :param: the device.
        :method: the request method.
        :returns: boolean, is_allowed.
        """
        return (self._get_ratelimiter(device, None).is_allowed()
                and self._get_ratelimiter(device, method).is_allowed())

    def __call__(self, env, start_response):
        """
        WSGI entry point.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        self._maybe_reload_config()
        req = Request(env)
        handler = self.app
        if (self.is_any_rate_limit_configured
                and req.method in RATE_LIMITED_METHODS):
            try:
                device, partition, _ = split_and_validate_path(req, 1, 3, True)
                int(partition)  # check it's a valid partition
            except (ValueError, HTTPException):
                # request may not have device/partition e.g. a healthcheck req
                pass
            else:
                if not self._is_allowed(device, req.method):
                    self.logger.increment('backend.ratelimit')
                    handler = HTTPTooManyBackendRequests()
        return handler(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def backend_ratelimit_filter(app):
        return BackendRateLimitMiddleware(app, conf)

    return backend_ratelimit_filter
