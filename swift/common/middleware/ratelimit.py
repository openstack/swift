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
import eventlet
from webob import Request, Response
from webob.exc import HTTPNotFound

from swift.common.utils import split_path, cache_from_env, get_logger
from swift.proxy.server import get_container_memcache_key
from swift.common.memcached import MemcacheConnectionError


class MaxSleepTimeHitError(Exception):
    pass


class RateLimitMiddleware(object):
    """
    Rate limiting middleware

    Rate limits requests on both an Account and Container level.  Limits are
    configurable.
    """

    BLACK_LIST_SLEEP = 1

    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='ratelimit')
        self.account_ratelimit = float(conf.get('account_ratelimit', 0))
        self.max_sleep_time_seconds = \
            float(conf.get('max_sleep_time_seconds', 60))
        self.log_sleep_time_seconds = \
            float(conf.get('log_sleep_time_seconds', 0))
        self.clock_accuracy = int(conf.get('clock_accuracy', 1000))
        self.rate_buffer_seconds = int(conf.get('rate_buffer_seconds', 5))
        self.ratelimit_whitelist = [acc.strip() for acc in
            conf.get('account_whitelist', '').split(',') if acc.strip()]
        self.ratelimit_blacklist = [acc.strip() for acc in
            conf.get('account_blacklist', '').split(',') if acc.strip()]
        self.memcache_client = None
        conf_limits = []
        for conf_key in conf.keys():
            if conf_key.startswith('container_ratelimit_'):
                cont_size = int(conf_key[len('container_ratelimit_'):])
                rate = float(conf[conf_key])
                conf_limits.append((cont_size, rate))

        conf_limits.sort()
        self.container_ratelimits = []
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

            self.container_ratelimits.append((cur_size, cur_rate, line_func))

    def get_container_maxrate(self, container_size):
        """
        Returns number of requests allowed per second for given container size.
        """
        last_func = None
        if container_size:
            container_size = int(container_size)
            for size, rate, func in self.container_ratelimits:
                if container_size < size:
                    break
                last_func = func
            if last_func:
                return last_func(container_size)
        return None

    def get_ratelimitable_key_tuples(self, req_method, account_name,
                                     container_name=None, obj_name=None):
        """
        Returns a list of key (used in memcache), ratelimit tuples. Keys
        should be checked in order.

        :param req_method: HTTP method
        :param account_name: account name from path
        :param container_name: container name from path
        :param obj_name: object name from path
        """
        keys = []
        # COPYs are not limited
        if self.account_ratelimit and \
                account_name and container_name and not obj_name and \
                req_method in ('PUT', 'DELETE'):
            keys.append(("ratelimit/%s" % account_name,
                         self.account_ratelimit))

        if account_name and container_name and obj_name and \
                req_method in ('PUT', 'DELETE', 'POST'):
            container_size = None
            memcache_key = get_container_memcache_key(account_name,
                                                      container_name)
            container_info = self.memcache_client.get(memcache_key)
            if isinstance(container_info, dict):
                container_size = container_info.get('container_size', 0)
                container_rate = self.get_container_maxrate(container_size)
                if container_rate:
                    keys.append(("ratelimit/%s/%s" % (account_name,
                                                      container_name),
                                 container_rate))
        return keys

    def _get_sleep_time(self, key, max_rate):
        '''
        Returns the amount of time (a float in seconds) that the app
        should sleep.

        :param key: a memcache key
        :param max_rate: maximum rate allowed in requests per second
        :raises: MaxSleepTimeHitError if max sleep time is exceeded.
        '''
        try:
            now_m = int(round(time.time() * self.clock_accuracy))
            time_per_request_m = int(round(self.clock_accuracy / max_rate))
            running_time_m = self.memcache_client.incr(key,
                             delta=time_per_request_m)
            need_to_sleep_m = 0
            if (now_m - running_time_m >
                    self.rate_buffer_seconds * self.clock_accuracy):
                next_avail_time = int(now_m + time_per_request_m)
                self.memcache_client.set(key, str(next_avail_time),
                                         serialize=False)
            else:
                need_to_sleep_m = \
                    max(running_time_m - now_m - time_per_request_m, 0)

            max_sleep_m = self.max_sleep_time_seconds * self.clock_accuracy
            if max_sleep_m - need_to_sleep_m <= self.clock_accuracy * 0.01:
                # treat as no-op decrement time
                self.memcache_client.decr(key, delta=time_per_request_m)
                raise MaxSleepTimeHitError("Max Sleep Time Exceeded: %.2f" %
                    (float(need_to_sleep_m) / self.clock_accuracy))

            return float(need_to_sleep_m) / self.clock_accuracy
        except MemcacheConnectionError:
            return 0

    def handle_ratelimit(self, req, account_name, container_name, obj_name):
        '''
        Performs rate limiting and account white/black listing.  Sleeps
        if necessary.

        :param account_name: account name from path
        :param container_name: container name from path
        :param obj_name: object name from path
        '''
        if account_name in self.ratelimit_blacklist:
            self.logger.error(_('Returning 497 because of blacklisting: %s'),
                              account_name)
            eventlet.sleep(self.BLACK_LIST_SLEEP)
            return Response(status='497 Blacklisted',
                body='Your account has been blacklisted', request=req)
        if account_name in self.ratelimit_whitelist:
            return None
        for key, max_rate in self.get_ratelimitable_key_tuples(
                req.method, account_name, container_name=container_name,
                obj_name=obj_name):
            try:
                need_to_sleep = self._get_sleep_time(key, max_rate)
                if self.log_sleep_time_seconds and \
                        need_to_sleep > self.log_sleep_time_seconds:
                    self.logger.warning(_("Ratelimit sleep log: %(sleep)s for "
                        "%(account)s/%(container)s/%(object)s"),
                        {'sleep': need_to_sleep, 'account': account_name,
                         'container': container_name, 'object': obj_name})
                if need_to_sleep > 0:
                    eventlet.sleep(need_to_sleep)
            except MaxSleepTimeHitError, e:
                self.logger.error(_('Returning 498 for %(meth)s to '
                    '%(acc)s/%(cont)s/%(obj)s . Ratelimit (Max Sleep) %(e)s'),
                    {'meth': req.method, 'acc': account_name,
                     'cont': container_name, 'obj': obj_name, 'e': str(e)})
                error_resp = Response(status='498 Rate Limited',
                                      body='Slow down', request=req)
                return error_resp
        return None

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in webob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        req = Request(env)
        if self.memcache_client is None:
            self.memcache_client = cache_from_env(env)
        if not self.memcache_client:
            self.logger.warning(
                _('Warning: Cannot ratelimit without a memcached client'))
            return self.app(env, start_response)
        try:
            version, account, container, obj = split_path(req.path, 1, 4, True)
        except ValueError:
            return HTTPNotFound()(env, start_response)
        ratelimit_resp = self.handle_ratelimit(req, account, container, obj)
        if ratelimit_resp is None:
            return self.app(env, start_response)
        else:
            return ratelimit_resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def limit_filter(app):
        return RateLimitMiddleware(app, conf)
    return limit_filter
