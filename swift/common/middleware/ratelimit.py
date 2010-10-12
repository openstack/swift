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
from webob import Request, Response

from swift.common.utils import split_path, cache_from_env, get_logger
from swift.proxy.server import get_container_memcache_key


class MaxSleepTimeHit(Exception):
    pass


class RateLimitMiddleware(object):
    """
    Rate limiting middleware
    """

    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf)
        self.account_rate_limit = float(conf.get('account_ratelimit', 200.0))
        self.max_sleep_time_seconds = float(conf.get('max_sleep_time_seconds',
                                                   60))
        self.clock_accuracy = int(conf.get('clock_accuracy', 1000))
        self.rate_limit_whitelist = [acc.strip() for acc in
            conf.get('account_whitelist', '').split(',')
            if acc.strip()]
        self.rate_limit_blacklist = [acc.strip() for acc in
            conf.get('account_blacklist', '').split(',')
            if acc.strip()]
        self.memcache_client = None
        conf_limits = []
        for conf_key in conf.keys():
            if conf_key.startswith('container_limit_'):
                cont_size = int(conf_key[len('container_limit_'):])
                rate = float(conf[conf_key])
                conf_limits.append((cont_size, rate))

        conf_limits.sort()
        self.container_limits = []
        while conf_limits:
            cur_size, cur_rate = conf_limits.pop(0)
            if conf_limits:
                next_size, next_rate = conf_limits[0]
                slope = (float(next_rate) - float(cur_rate)) \
                      / (next_size - cur_size)

                def new_scope(cur_size, slope, cur_rate):
                    # making new scope for variables
                    return lambda x: (x - cur_size) * slope + cur_rate
                line_func = new_scope(cur_size, slope, cur_rate)
            else:
                line_func = lambda x: cur_rate

            self.container_limits.append((cur_size, cur_rate, line_func))

    def get_container_maxrate(self, container_size):
        last_func = None
        if container_size:
            container_size = int(container_size)
            for size, rate, func in self.container_limits:
                if container_size < size:
                    break
                last_func = func

            if last_func:
                return last_func(container_size)
        return None

    def get_ratelimitable_key_tuples(self, req_method,
                                     account_name, container_name, obj_name):
        """
        Returns a list of key (used in memcache), ratelimit tuples. Keys 
        should be checked in order.
        """
        keys = []
        if account_name and (
            not (container_name or obj_name) or
            (container_name and not obj_name and req_method == 'PUT')):
            keys.append(("ratelimit/%s" % account_name,
                         self.account_rate_limit))

        if account_name and container_name and (
            (not obj_name and req_method in ('GET', 'HEAD')) or
            (obj_name and req_method in ('PUT', 'DELETE'))):
            container_size = None
            memcache_key = get_container_memcache_key(account_name,
                                                      container_name)
            container_info = self.memcache_client.get(memcache_key)
            if type(container_info) == dict:
                container_size = container_info.get('container_size', 0)
                container_rate = self.get_container_maxrate(container_size)
                if container_rate:
                    keys.append(("ratelimit/%s/%s" % (account_name,
                                                      container_name),
                                 container_rate))
        return keys

    def _get_sleep_time(self, key, max_rate):
        now_m = int(round(time.time() * self.clock_accuracy))
        time_per_request_m = int(round(self.clock_accuracy / max_rate))
        running_time_m = self.memcache_client.incr(key,
                                                   delta=time_per_request_m)
        need_to_sleep_m = 0
        request_time_limit = now_m + (time_per_request_m * max_rate)
        if running_time_m < now_m:
            next_avail_time = int(now_m + time_per_request_m)
            self.memcache_client.set(key, str(next_avail_time),
                                     serialize=False)
        elif running_time_m - now_m - time_per_request_m > 0:
            need_to_sleep_m = running_time_m - now_m - time_per_request_m

        max_sleep_m = self.max_sleep_time_seconds * self.clock_accuracy
        if max_sleep_m - need_to_sleep_m <= self.clock_accuracy * 0.01:
            # treat as no-op decrement time
            self.memcache_client.incr(key, delta=-time_per_request_m)
            raise MaxSleepTimeHit("Max Sleep Time Exceeded: %s" %
                                  need_to_sleep_m)

        return float(need_to_sleep_m) / self.clock_accuracy

    def handle_rate_limit(self, req, account_name, container_name, obj_name):
        if account_name in self.rate_limit_blacklist:
            self.logger.error('Returning 497 because of blacklisting')
            return Response(status='497 Blacklisted',
                body='Your account has been blacklisted', request=req)
        if account_name in self.rate_limit_whitelist:
            return None

        for key, max_rate in self.get_ratelimitable_key_tuples(req.method,
                                                               account_name,
                                                               container_name,
                                                               obj_name):
            try:
                need_to_sleep = self._get_sleep_time(key, max_rate)
                if need_to_sleep > 0:
                    time.sleep(need_to_sleep)
            except MaxSleepTimeHit, e:
                self.logger.error('Returning 498 because of ops ' + \
                                   'rate limiting (Max Sleep) %s' % e)
                error_resp = Response(status='498 Rate Limited',
                                      body='Slow down', request=req)
                return error_resp

        return None

    def __call__(self, env, start_response):
        req = Request(env)
        if self.memcache_client is None:
            self.memcache_client = cache_from_env(env)
        version, account, container, obj = split_path(req.path, 1, 4, True)

        rate_limit_resp = self.handle_rate_limit(req, account, container,
                                                 obj)
        if rate_limit_resp is None:
            return self.app(env, start_response)
        else:
            return rate_limit_resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def limit_filter(app):
        return RateLimitMiddleware(app, conf)
    return limit_filter
