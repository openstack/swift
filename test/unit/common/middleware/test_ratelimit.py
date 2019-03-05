# Copyright (c) 2010-2012 OpenStack Foundation
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

import unittest
import time
import eventlet
import mock
from contextlib import contextmanager

from test.unit import FakeLogger
from swift.common.middleware import ratelimit
from swift.proxy.controllers.base import get_cache_key, \
    headers_to_container_info
from swift.common.memcached import MemcacheConnectionError
from swift.common.swob import Request
from swift.common import utils

threading = eventlet.patcher.original('threading')


class FakeMemcache(object):

    def __init__(self):
        self.store = {}
        self.error_on_incr = False
        self.init_incr_return_neg = False

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, serialize=False, time=0):
        self.store[key] = value
        return True

    def incr(self, key, delta=1, time=0):
        if self.error_on_incr:
            raise MemcacheConnectionError('Memcache restarting')
        if self.init_incr_return_neg:
            # simulate initial hit, force reset of memcache
            self.init_incr_return_neg = False
            return -10000000
        self.store[key] = int(self.store.setdefault(key, 0)) + int(delta)
        if self.store[key] < 0:
            self.store[key] = 0
        return int(self.store[key])

    def decr(self, key, delta=1, time=0):
        return self.incr(key, delta=-delta, time=time)

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


class FakeApp(object):

    def __call__(self, env, start_response):
        start_response('200 OK', [])
        return [b'Some Content']


def start_response(*args):
    pass


time_ticker = 0
time_override = []


def mock_sleep(x):
    global time_ticker
    time_ticker += x


def mock_time():
    global time_override
    global time_ticker
    if time_override:
        cur_time = time_override.pop(0)
        if cur_time is None:
            time_override = [None if i is None else i + time_ticker
                             for i in time_override]
            return time_ticker
        return cur_time
    return time_ticker


class TestRateLimit(unittest.TestCase):

    def _reset_time(self):
        global time_ticker
        time_ticker = 0

    def setUp(self):
        self.was_sleep = eventlet.sleep
        eventlet.sleep = mock_sleep
        self.was_time = time.time
        time.time = mock_time
        self._reset_time()

    def tearDown(self):
        eventlet.sleep = self.was_sleep
        time.time = self.was_time

    def _run(self, callable_func, num, rate, check_time=True):
        global time_ticker
        begin = time.time()
        for x in range(num):
            callable_func()
        end = time.time()
        total_time = float(num) / rate - 1.0 / rate  # 1st request not limited
        # Allow for one second of variation in the total time.
        time_diff = abs(total_time - (end - begin))
        if check_time:
            self.assertEqual(round(total_time, 1), round(time_ticker, 1))
        return time_diff

    def test_get_maxrate(self):
        conf_dict = {'container_ratelimit_10': 200,
                     'container_ratelimit_50': 100,
                     'container_ratelimit_75': 30}
        test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        test_ratelimit.logger = FakeLogger()
        self.assertIsNone(ratelimit.get_maxrate(
            test_ratelimit.container_ratelimits, 0))
        self.assertIsNone(ratelimit.get_maxrate(
            test_ratelimit.container_ratelimits, 5))
        self.assertEqual(ratelimit.get_maxrate(
            test_ratelimit.container_ratelimits, 10), 200)
        self.assertEqual(ratelimit.get_maxrate(
            test_ratelimit.container_ratelimits, 60), 72)
        self.assertEqual(ratelimit.get_maxrate(
            test_ratelimit.container_ratelimits, 160), 30)

    def test_get_ratelimitable_key_tuples(self):
        current_rate = 13
        conf_dict = {'account_ratelimit': current_rate,
                     'container_ratelimit_3': 200}
        fake_memcache = FakeMemcache()
        fake_memcache.store[get_cache_key('a', 'c')] = \
            {'object_count': '5'}
        the_app = ratelimit.filter_factory(conf_dict)(FakeApp())
        the_app.memcache_client = fake_memcache
        req = lambda: None
        req.environ = {'swift.cache': fake_memcache, 'PATH_INFO': '/v1/a/c/o'}
        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            req.method = 'DELETE'
            self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
                req, 'a', None, None)), 0)
            req.method = 'PUT'
            self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
                req, 'a', 'c', None)), 1)
            req.method = 'DELETE'
            self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
                req, 'a', 'c', None)), 1)
            req.method = 'GET'
            self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
                req, 'a', 'c', 'o')), 0)
            req.method = 'PUT'
            self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
                req, 'a', 'c', 'o')), 1)

        req.method = 'PUT'
        self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
            req, 'a', 'c', None, global_ratelimit=10)), 2)
        self.assertEqual(the_app.get_ratelimitable_key_tuples(
            req, 'a', 'c', None, global_ratelimit=10)[1],
            ('ratelimit/global-write/a', 10))

        req.method = 'PUT'
        self.assertEqual(len(the_app.get_ratelimitable_key_tuples(
            req, 'a', 'c', None, global_ratelimit='notafloat')), 1)

    def test_memcached_container_info_dict(self):
        mdict = headers_to_container_info({'x-container-object-count': '45'})
        self.assertEqual(mdict['object_count'], '45')

    def test_ratelimit_old_memcache_format(self):
        current_rate = 13
        conf_dict = {'account_ratelimit': current_rate,
                     'container_ratelimit_3': 200}
        fake_memcache = FakeMemcache()
        fake_memcache.store[get_cache_key('a', 'c')] = \
            {'container_size': 5}
        the_app = ratelimit.filter_factory(conf_dict)(FakeApp())
        the_app.memcache_client = fake_memcache
        req = lambda: None
        req.method = 'PUT'
        req.environ = {'PATH_INFO': '/v1/a/c/o', 'swift.cache': fake_memcache}
        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            tuples = the_app.get_ratelimitable_key_tuples(req, 'a', 'c', 'o')
            self.assertEqual(tuples, [('ratelimit/a/c', 200.0)])

    def test_account_ratelimit(self):
        current_rate = 5
        num_calls = 50
        conf_dict = {'account_ratelimit': current_rate}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        with mock.patch('swift.common.middleware.ratelimit.get_container_info',
                        lambda *args, **kwargs: {}):
            with mock.patch(
                    'swift.common.middleware.ratelimit.get_account_info',
                    lambda *args, **kwargs: {}):
                for meth, exp_time in [('DELETE', 9.8), ('GET', 0),
                                       ('POST', 0), ('PUT', 9.8)]:
                    req = Request.blank('/v1/a%s/c' % meth)
                    req.method = meth
                    req.environ['swift.cache'] = FakeMemcache()
                    make_app_call = lambda: self.test_ratelimit(req.environ,
                                                                start_response)
                    begin = time.time()
                    self._run(make_app_call, num_calls, current_rate,
                              check_time=bool(exp_time))
                    self.assertEqual(round(time.time() - begin, 1), exp_time)
                    self._reset_time()

    def test_ratelimit_set_incr(self):
        current_rate = 5
        num_calls = 50
        conf_dict = {'account_ratelimit': current_rate}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        req = Request.blank('/v1/a/c')
        req.method = 'PUT'
        req.environ['swift.cache'] = FakeMemcache()
        req.environ['swift.cache'].init_incr_return_neg = True
        make_app_call = lambda: self.test_ratelimit(req.environ,
                                                    start_response)
        begin = time.time()
        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            self._run(make_app_call, num_calls, current_rate, check_time=False)
            self.assertEqual(round(time.time() - begin, 1), 9.8)

    def test_ratelimit_old_white_black_list(self):
        global time_ticker
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2,
                     'account_whitelist': 'a',
                     'account_blacklist': 'b'}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        req = Request.blank('/')
        with mock.patch.object(self.test_ratelimit,
                               'memcache_client', FakeMemcache()):
            self.assertEqual(
                self.test_ratelimit.handle_ratelimit(req, 'a', 'c', 'o'),
                None)
            self.assertEqual(
                self.test_ratelimit.handle_ratelimit(
                    req, 'b', 'c', 'o').status_int,
                497)

    def test_ratelimit_whitelist_sysmeta(self):
        global time_ticker
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2,
                     'account_whitelist': 'a',
                     'account_blacklist': 'b'}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        req = Request.blank('/v1/a/c')
        req.environ['swift.cache'] = FakeMemcache()

        class rate_caller(threading.Thread):

            def __init__(self, parent):
                threading.Thread.__init__(self)
                self.parent = parent

            def run(self):
                self.result = self.parent.test_ratelimit(req.environ,
                                                         start_response)

        def get_fake_ratelimit(*args, **kwargs):
            return {'sysmeta': {'global-write-ratelimit': 'WHITELIST'}}

        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        get_fake_ratelimit):
            nt = 5
            threads = []
            for i in range(nt):
                rc = rate_caller(self)
                rc.start()
                threads.append(rc)
            for thread in threads:
                thread.join()
            the_498s = [
                t for t in threads
                if b''.join(t.result).startswith(b'Slow down')]
            self.assertEqual(len(the_498s), 0)
            self.assertEqual(time_ticker, 0)

    def test_ratelimit_blacklist(self):
        global time_ticker
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2,
                     'account_whitelist': 'a',
                     'account_blacklist': 'b'}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        self.test_ratelimit.logger = FakeLogger()
        self.test_ratelimit.BLACK_LIST_SLEEP = 0
        req = Request.blank('/v1/b/c')
        req.environ['swift.cache'] = FakeMemcache()

        class rate_caller(threading.Thread):

            def __init__(self, parent):
                threading.Thread.__init__(self)
                self.parent = parent

            def run(self):
                self.result = self.parent.test_ratelimit(req.environ,
                                                         start_response)

        def get_fake_ratelimit(*args, **kwargs):
            return {'sysmeta': {'global-write-ratelimit': 'BLACKLIST'}}

        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        get_fake_ratelimit):
            nt = 5
            threads = []
            for i in range(nt):
                rc = rate_caller(self)
                rc.start()
                threads.append(rc)
            for thread in threads:
                thread.join()
            the_497s = [
                t for t in threads
                if b''.join(t.result).startswith(b'Your account')]
            self.assertEqual(len(the_497s), 5)
            self.assertEqual(time_ticker, 0)

    def test_ratelimit_max_rate_double(self):
        global time_ticker
        global time_override
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'clock_accuracy': 100,
                     'max_sleep_time_seconds': 1}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        self.test_ratelimit.log_sleep_time_seconds = .00001
        req = Request.blank('/v1/a/c')
        req.method = 'PUT'
        req.environ['swift.cache'] = FakeMemcache()

        time_override = [0, 0, 0, 0, None]
        # simulates 4 requests coming in at same time, then sleeping
        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            r = self.test_ratelimit(req.environ, start_response)
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Slow down')
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Slow down')
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            print(repr(r))
            self.assertEqual(r[0], b'Some Content')

    def test_ratelimit_max_rate_double_container(self):
        global time_ticker
        global time_override
        current_rate = 2
        conf_dict = {'container_ratelimit_0': current_rate,
                     'clock_accuracy': 100,
                     'max_sleep_time_seconds': 1}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        self.test_ratelimit.log_sleep_time_seconds = .00001
        req = Request.blank('/v1/a/c/o')
        req.method = 'PUT'
        req.environ['swift.cache'] = FakeMemcache()
        req.environ['swift.cache'].set(
            get_cache_key('a', 'c'),
            {'object_count': 1})

        time_override = [0, 0, 0, 0, None]
        # simulates 4 requests coming in at same time, then sleeping
        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            r = self.test_ratelimit(req.environ, start_response)
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Slow down')
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Slow down')
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Some Content')

    def test_ratelimit_max_rate_double_container_listing(self):
        global time_ticker
        global time_override
        current_rate = 2
        conf_dict = {'container_listing_ratelimit_0': current_rate,
                     'clock_accuracy': 100,
                     'max_sleep_time_seconds': 1}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        self.test_ratelimit.log_sleep_time_seconds = .00001
        req = Request.blank('/v1/a/c')
        req.method = 'GET'
        req.environ['swift.cache'] = FakeMemcache()
        req.environ['swift.cache'].set(
            get_cache_key('a', 'c'),
            {'object_count': 1})

        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            time_override = [0, 0, 0, 0, None]
            # simulates 4 requests coming in at same time, then sleeping
            r = self.test_ratelimit(req.environ, start_response)
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Slow down')
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Slow down')
            mock_sleep(.1)
            r = self.test_ratelimit(req.environ, start_response)
            self.assertEqual(r[0], b'Some Content')
            mc = self.test_ratelimit.memcache_client
            try:
                self.test_ratelimit.memcache_client = None
                self.assertIsNone(
                    self.test_ratelimit.handle_ratelimit(req, 'n', 'c', None))
            finally:
                self.test_ratelimit.memcache_client = mc

    def test_ratelimit_max_rate_multiple_acc(self):
        num_calls = 4
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2}
        fake_memcache = FakeMemcache()

        the_app = ratelimit.filter_factory(conf_dict)(FakeApp())
        the_app.memcache_client = fake_memcache
        req = lambda: None
        req.method = 'PUT'
        req.environ = {}

        class rate_caller(threading.Thread):

            def __init__(self, name):
                self.myname = name
                threading.Thread.__init__(self)

            def run(self):
                for j in range(num_calls):
                    self.result = the_app.handle_ratelimit(req, self.myname,
                                                           'c', None)

        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            nt = 15
            begin = time.time()
            threads = []
            for i in range(nt):
                rc = rate_caller('a%s' % i)
                rc.start()
                threads.append(rc)
            for thread in threads:
                thread.join()

            time_took = time.time() - begin
            self.assertEqual(1.5, round(time_took, 1))

    def test_call_invalid_path(self):
        env = {'REQUEST_METHOD': 'GET',
               'SCRIPT_NAME': '',
               'PATH_INFO': '//v1/AUTH_1234567890',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '80',
               'swift.cache': FakeMemcache(),
               'SERVER_PROTOCOL': 'HTTP/1.0'}

        app = lambda *args, **kwargs: ['fake_app']
        rate_mid = ratelimit.filter_factory({})(app)

        class a_callable(object):

            def __call__(self, *args, **kwargs):
                pass
        resp = rate_mid.__call__(env, a_callable())
        self.assertEqual('fake_app', resp[0])

    def test_call_non_swift_api_path(self):
        env = {'REQUEST_METHOD': 'GET',
               'SCRIPT_NAME': '',
               'PATH_INFO': '/ive/got/a/lovely/bunch/of/coconuts',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '80',
               'swift.cache': FakeMemcache(),
               'SERVER_PROTOCOL': 'HTTP/1.0'}

        app = lambda *args, **kwargs: ['some response']
        rate_mid = ratelimit.filter_factory({})(app)

        class a_callable(object):

            def __call__(self, *args, **kwargs):
                pass

        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        side_effect=Exception("you shouldn't call this")):
            resp = rate_mid(env, a_callable())
        self.assertEqual(resp[0], 'some response')

    def test_no_memcache(self):
        current_rate = 13
        num_calls = 5
        conf_dict = {'account_ratelimit': current_rate}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        req = Request.blank('/v1/a')
        req.environ['swift.cache'] = None
        make_app_call = lambda: self.test_ratelimit(req.environ,
                                                    start_response)
        begin = time.time()
        self._run(make_app_call, num_calls, current_rate, check_time=False)
        time_took = time.time() - begin
        self.assertEqual(round(time_took, 1), 0)  # no memcache, no limiting

    def test_restarting_memcache(self):
        current_rate = 2
        num_calls = 5
        conf_dict = {'account_ratelimit': current_rate}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        req = Request.blank('/v1/a/c')
        req.method = 'PUT'
        req.environ['swift.cache'] = FakeMemcache()
        req.environ['swift.cache'].error_on_incr = True
        make_app_call = lambda: self.test_ratelimit(req.environ,
                                                    start_response)
        begin = time.time()
        with mock.patch('swift.common.middleware.ratelimit.get_account_info',
                        lambda *args, **kwargs: {}):
            self._run(make_app_call, num_calls, current_rate, check_time=False)
            time_took = time.time() - begin
            self.assertEqual(round(time_took, 1), 0)  # no memcache, no limit


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):

        def check_key_is_absent(key):
            try:
                swift_info[key]
            except KeyError as err:
                if key not in str(err):
                    raise

        test_limits = {'account_ratelimit': 1,
                       'max_sleep_time_seconds': 60,
                       'container_ratelimit_0': 0,
                       'container_ratelimit_10': 10,
                       'container_ratelimit_50': 50,
                       'container_listing_ratelimit_0': 0,
                       'container_listing_ratelimit_10': 10,
                       'container_listing_ratelimit_50': 50}

        ratelimit.filter_factory(test_limits)('have to pass in an app')
        swift_info = utils.get_swift_info()
        self.assertIn('ratelimit', swift_info)
        self.assertEqual(swift_info['ratelimit']
                         ['account_ratelimit'], 1.0)
        self.assertEqual(swift_info['ratelimit']
                         ['max_sleep_time_seconds'], 60.0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_ratelimits'][0][0], 0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_ratelimits'][0][1], 0.0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_ratelimits'][1][0], 10)
        self.assertEqual(swift_info['ratelimit']
                         ['container_ratelimits'][1][1], 10.0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_ratelimits'][2][0], 50)
        self.assertEqual(swift_info['ratelimit']
                         ['container_ratelimits'][2][1], 50.0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_listing_ratelimits'][0][0], 0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_listing_ratelimits'][0][1], 0.0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_listing_ratelimits'][1][0], 10)
        self.assertEqual(swift_info['ratelimit']
                         ['container_listing_ratelimits'][1][1], 10.0)
        self.assertEqual(swift_info['ratelimit']
                         ['container_listing_ratelimits'][2][0], 50)
        self.assertEqual(swift_info['ratelimit']
                         ['container_listing_ratelimits'][2][1], 50.0)

        # these were left out on purpose
        for key in ['log_sleep_time_seconds', 'clock_accuracy',
                    'rate_buffer_seconds', 'ratelimit_whitelis',
                    'ratelimit_blacklist']:
            check_key_is_absent(key)


if __name__ == '__main__':
    unittest.main()
