# Copyright (c) 2010-2013 OpenStack Foundation
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
from swift import gettext_ as _

import eventlet

from swift.common.utils import cache_from_env, get_logger, register_swift_info
from swift.proxy.controllers.base import get_account_info, get_container_info
from swift.common.constraints import valid_api_version
from swift.common.memcached import MemcacheConnectionError
from swift.common.swob import Request, Response


def interpret_conf_limits(conf, name_prefix, info=None):
    """
    Parses general parms for rate limits looking for things that
    start with the provided name_prefix within the provided conf
    and returns lists for both internal use and for /info

    :param conf: conf dict to parse
    :param name_prefix: prefix of config parms to look for
    :param info: set to return extra stuff for /info registration
    """
    conf_limits = []
    for conf_key in conf:
        if conf_key.startswith(name_prefix):
            cont_size = int(conf_key[len(name_prefix):])
            rate = float(conf[conf_key])
            conf_limits.append((cont_size, rate))

    conf_limits.sort()
    ratelimits = []
    conf_limits_info = list(conf_limits)
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

        ratelimits.append((cur_size, cur_rate, line_func))
    if info is None:
        return ratelimits
    else:
        return ratelimits, conf_limits_info


def get_maxrate(ratelimits, size):
    """
    Returns number of requests allowed per second for given size.
    """
    last_func = None
    if size:
        size = int(size)
        for ratesize, rate, func in ratelimits:
            if size < ratesize:
                break
            last_func = func
        if last_func:
            return last_func(size)
    return None


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
        self.logger = logger or get_logger(conf, log_route='ratelimit')
        self.memcache_client = None
        self.account_ratelimit = float(conf.get('account_ratelimit', 0))
        self.max_sleep_time_seconds = \
            float(conf.get('max_sleep_time_seconds', 60))
        self.log_sleep_time_seconds = \
            float(conf.get('log_sleep_time_seconds', 0))
        self.clock_accuracy = int(conf.get('clock_accuracy', 1000))
        self.rate_buffer_seconds = int(conf.get('rate_buffer_seconds', 5))
        self.ratelimit_whitelist = \
            [acc.strip() for acc in
                conf.get('account_whitelist', '').split(',') if acc.strip()]
        if self.ratelimit_whitelist:
            self.logger.warning('Option account_whitelist is deprecated. Use '
                                'an internal client to POST a `X-Account-'
                                'Sysmeta-Global-Write-Ratelimit: WHITELIST` '
                                'header to the specific accounts instead.')
        self.ratelimit_blacklist = \
            [acc.strip() for acc in
                conf.get('account_blacklist', '').split(',') if acc.strip()]
        if self.ratelimit_blacklist:
            self.logger.warning('Option account_blacklist is deprecated. Use '
                                'an internal client to POST a `X-Account-'
                                'Sysmeta-Global-Write-Ratelimit: BLACKLIST` '
                                'header to the specific accounts instead.')
        self.container_ratelimits = interpret_conf_limits(
            conf, 'container_ratelimit_')
        self.container_listing_ratelimits = interpret_conf_limits(
            conf, 'container_listing_ratelimit_')

    def get_container_size(self, env):
        rv = 0
        container_info = get_container_info(
            env, self.app, swift_source='RL')
        if isinstance(container_info, dict):
            rv = container_info.get(
                'object_count', container_info.get('container_size', 0))
        return rv

    def get_ratelimitable_key_tuples(self, req, account_name,
                                     container_name=None, obj_name=None,
                                     global_ratelimit=None):
        """
        Returns a list of key (used in memcache), ratelimit tuples. Keys
        should be checked in order.

        :param req: swob request
        :param account_name: account name from path
        :param container_name: container name from path
        :param obj_name: object name from path
        :param global_ratelimit: this account has an account wide
                                 ratelimit on all writes combined
        """
        keys = []
        # COPYs are not limited

        if self.account_ratelimit and \
                account_name and container_name and not obj_name and \
                req.method in ('PUT', 'DELETE'):
            keys.append(("ratelimit/%s" % account_name,
                         self.account_ratelimit))

        if account_name and container_name and obj_name and \
                req.method in ('PUT', 'DELETE', 'POST', 'COPY'):
            container_size = self.get_container_size(req.environ)
            container_rate = get_maxrate(
                self.container_ratelimits, container_size)
            if container_rate:
                keys.append((
                    "ratelimit/%s/%s" % (account_name, container_name),
                    container_rate))

        if account_name and container_name and not obj_name and \
                req.method == 'GET':
            container_size = self.get_container_size(req.environ)
            container_rate = get_maxrate(
                self.container_listing_ratelimits, container_size)
            if container_rate:
                keys.append((
                    "ratelimit_listing/%s/%s" % (account_name, container_name),
                    container_rate))

        if account_name and req.method in ('PUT', 'DELETE', 'POST', 'COPY'):
            if global_ratelimit:
                try:
                    global_ratelimit = float(global_ratelimit)
                    if global_ratelimit > 0:
                        keys.append((
                            "ratelimit/global-write/%s" % account_name,
                            global_ratelimit))
                except ValueError:
                    pass

        return keys

    def _get_sleep_time(self, key, max_rate):
        """
        Returns the amount of time (a float in seconds) that the app
        should sleep.

        :param key: a memcache key
        :param max_rate: maximum rate allowed in requests per second
        :raises MaxSleepTimeHitError: if max sleep time is exceeded.
        """
        try:
            now_m = int(round(time.time() * self.clock_accuracy))
            time_per_request_m = int(round(self.clock_accuracy / max_rate))
            running_time_m = self.memcache_client.incr(
                key, delta=time_per_request_m)
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
                raise MaxSleepTimeHitError(
                    "Max Sleep Time Exceeded: %.2f" %
                    (float(need_to_sleep_m) / self.clock_accuracy))

            return float(need_to_sleep_m) / self.clock_accuracy
        except MemcacheConnectionError:
            return 0

    def handle_ratelimit(self, req, account_name, container_name, obj_name):
        """
        Performs rate limiting and account white/black listing.  Sleeps
        if necessary. If self.memcache_client is not set, immediately returns
        None.

        :param account_name: account name from path
        :param container_name: container name from path
        :param obj_name: object name from path
        """
        if not self.memcache_client:
            return None

        try:
            account_info = get_account_info(req.environ, self.app,
                                            swift_source='RL')
            account_global_ratelimit = \
                account_info.get('sysmeta', {}).get('global-write-ratelimit')
        except ValueError:
            account_global_ratelimit = None

        if account_name in self.ratelimit_whitelist or \
                account_global_ratelimit == 'WHITELIST':
            return None

        if account_name in self.ratelimit_blacklist or \
                account_global_ratelimit == 'BLACKLIST':
            self.logger.error(_('Returning 497 because of blacklisting: %s'),
                              account_name)
            eventlet.sleep(self.BLACK_LIST_SLEEP)
            return Response(status='497 Blacklisted',
                            body='Your account has been blacklisted',
                            request=req)

        for key, max_rate in self.get_ratelimitable_key_tuples(
                req, account_name, container_name=container_name,
                obj_name=obj_name, global_ratelimit=account_global_ratelimit):
            try:
                need_to_sleep = self._get_sleep_time(key, max_rate)
                if self.log_sleep_time_seconds and \
                        need_to_sleep > self.log_sleep_time_seconds:
                    self.logger.warning(
                        _("Ratelimit sleep log: %(sleep)s for "
                          "%(account)s/%(container)s/%(object)s"),
                        {'sleep': need_to_sleep, 'account': account_name,
                         'container': container_name, 'object': obj_name})
                if need_to_sleep > 0:
                    eventlet.sleep(need_to_sleep)
            except MaxSleepTimeHitError as e:
                self.logger.error(
                    _('Returning 498 for %(meth)s to %(acc)s/%(cont)s/%(obj)s '
                      '. Ratelimit (Max Sleep) %(e)s'),
                    {'meth': req.method, 'acc': account_name,
                     'cont': container_name, 'obj': obj_name, 'e': str(e)})
                error_resp = Response(status='498 Rate Limited',
                                      body='Slow down', request=req)
                return error_resp
        return None

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in swob.Request object and passes it down.

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
            version, account, container, obj = req.split_path(1, 4, True)
        except ValueError:
            return self.app(env, start_response)
        if not valid_api_version(version):
            return self.app(env, start_response)
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

    account_ratelimit = float(conf.get('account_ratelimit', 0))
    max_sleep_time_seconds = \
        float(conf.get('max_sleep_time_seconds', 60))
    container_ratelimits, cont_limit_info = interpret_conf_limits(
        conf, 'container_ratelimit_', info=1)
    container_listing_ratelimits, cont_list_limit_info = \
        interpret_conf_limits(conf, 'container_listing_ratelimit_', info=1)
    # not all limits are exposed (intentionally)
    register_swift_info('ratelimit',
                        account_ratelimit=account_ratelimit,
                        max_sleep_time_seconds=max_sleep_time_seconds,
                        container_ratelimits=cont_limit_info,
                        container_listing_ratelimits=cont_list_limit_info)

    def limit_filter(app):
        return RateLimitMiddleware(app, conf)

    return limit_filter
