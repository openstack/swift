# Copyright (c) 2010-2011 OpenStack, LLC.
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
from contextlib import contextmanager
from threading import Thread
from webob import Request

from swift.common.middleware import ratelimit
from swift.proxy.server import get_container_memcache_key


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, serialize=False, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, delta=1, timeout=0):
        self.store[key] = int(self.store.setdefault(key, 0)) + int(delta)
        if self.store[key] < 0:
            self.store[key] = 0
        return int(self.store[key])

    def decr(self, key, delta=1, timeout=0):
        return self.incr(key, delta=-delta, timeout=timeout)

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


def mock_http_connect(response, headers=None, with_exc=False):

    class FakeConn(object):

        def __init__(self, status, headers, with_exc):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = '1234'
            self.with_exc = with_exc
            self.headers = headers
            if self.headers is None:
                self.headers = {}

        def getresponse(self):
            if self.with_exc:
                raise Exception('test')
            return self

        def getheader(self, header):
            return self.headers[header]

        def read(self, amt=None):
            return ''

        def close(self):
            return
    return lambda *args, **kwargs: FakeConn(response, headers, with_exc)


class FakeApp(object):

    def __call__(self, env, start_response):
        return ['204 No Content']


class FakeLogger(object):
    # a thread safe logger

    def error(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


def start_response(*args):
    pass


def dummy_filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def limit_filter(app):
        return ratelimit.RateLimitMiddleware(app, conf, logger=FakeLogger())
    return limit_filter


class TestRateLimit(unittest.TestCase):

    def _run(self, callable_func, num, rate, extra_sleep=0,
             total_time=None, check_time=True):
        begin = time.time()
        for x in range(0, num):
            result = callable_func()
            # Extra sleep is here to test with different call intervals.
            time.sleep(extra_sleep)
        end = time.time()
        if total_time is None:
            total_time = num / rate
        # Allow for one second of variation in the total time.
        time_diff = abs(total_time - (end - begin))
        if check_time:
            self.assertTrue(time_diff < 1)
        return time_diff

    def test_get_container_maxrate(self):
        conf_dict = {'container_ratelimit_10': 200,
                     'container_ratelimit_50': 100,
                     'container_ratelimit_75': 30}
        test_ratelimit = dummy_filter_factory(conf_dict)(FakeApp())
        self.assertEquals(test_ratelimit.get_container_maxrate(0), None)
        self.assertEquals(test_ratelimit.get_container_maxrate(5), None)
        self.assertEquals(test_ratelimit.get_container_maxrate(10), 200)
        self.assertEquals(test_ratelimit.get_container_maxrate(60), 72)
        self.assertEquals(test_ratelimit.get_container_maxrate(160), 30)

    def test_get_ratelimitable_key_tuples(self):
        current_rate = 13
        conf_dict = {'account_ratelimit': current_rate,
                     'container_ratelimit_3': 200}
        fake_memcache = FakeMemcache()
        fake_memcache.store[get_container_memcache_key('a', 'c')] = \
            {'container_size': 5}
        the_app = ratelimit.RateLimitMiddleware(None, conf_dict,
                                                logger=FakeLogger())
        the_app.memcache_client = fake_memcache
        self.assertEquals(len(the_app.get_ratelimitable_key_tuples(
                    'GET', 'a', None, None)), 1)
        self.assertEquals(len(the_app.get_ratelimitable_key_tuples(
                    'POST', 'a', 'c', None)), 0)
        self.assertEquals(len(the_app.get_ratelimitable_key_tuples(
                    'PUT', 'a', 'c', None)), 1)
        self.assertEquals(len(the_app.get_ratelimitable_key_tuples(
                    'GET', 'a', 'c', None)), 1)
        self.assertEquals(len(the_app.get_ratelimitable_key_tuples(
                    'GET', 'a', 'c', 'o')), 0)
        self.assertEquals(len(the_app.get_ratelimitable_key_tuples(
                    'PUT', 'a', 'c', 'o')), 1)

    def test_ratelimit(self):
        current_rate = 13
        num_calls = 5
        conf_dict = {'account_ratelimit': current_rate}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        ratelimit.http_connect = mock_http_connect(204)
        req = Request.blank('/v/a')
        req.environ['swift.cache'] = FakeMemcache()
        make_app_call = lambda: self.test_ratelimit(req.environ,
                                                    start_response)
        self._run(make_app_call, num_calls, current_rate)

    def test_ratelimit_whitelist(self):
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2,
                     'account_whitelist': 'a',
                     'account_blacklist': 'b'}
        self.test_ratelimit = dummy_filter_factory(conf_dict)(FakeApp())
        ratelimit.http_connect = mock_http_connect(204)
        req = Request.blank('/v/a/c')
        req.environ['swift.cache'] = FakeMemcache()

        class rate_caller(Thread):

            def __init__(self, parent):
                Thread.__init__(self)
                self.parent = parent

            def run(self):
                self.result = self.parent.test_ratelimit(req.environ,
                                                         start_response)
        nt = 5
        begin = time.time()
        threads = []
        for i in range(nt):
            rc = rate_caller(self)
            rc.start()
            threads.append(rc)
        for thread in threads:
            thread.join()
        the_498s = [t for t in threads if \
                        ''.join(t.result).startswith('Slow down')]
        self.assertEquals(len(the_498s), 0)
        time_took = time.time() - begin
        self.assert_(time_took < 1)

    def test_ratelimit_blacklist(self):
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2,
                     'account_whitelist': 'a',
                     'account_blacklist': 'b'}
        self.test_ratelimit = dummy_filter_factory(conf_dict)(FakeApp())
        self.test_ratelimit.BLACK_LIST_SLEEP = 0
        ratelimit.http_connect = mock_http_connect(204)
        req = Request.blank('/v/b/c')
        req.environ['swift.cache'] = FakeMemcache()

        class rate_caller(Thread):

            def __init__(self, parent):
                Thread.__init__(self)
                self.parent = parent

            def run(self):
                self.result = self.parent.test_ratelimit(req.environ,
                                                         start_response)
        nt = 5
        begin = time.time()
        threads = []
        for i in range(nt):
            rc = rate_caller(self)
            rc.start()
            threads.append(rc)
        for thread in threads:
            thread.join()
        the_497s = [t for t in threads if \
                        ''.join(t.result).startswith('Your account')]
        self.assertEquals(len(the_497s), 5)
        time_took = time.time() - begin
        self.assert_(round(time_took, 1) == 0)

    def test_ratelimit_max_rate_double(self):
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'clock_accuracy': 100,
                     'max_sleep_time_seconds': 1}
        # making clock less accurate for nosetests running slow
        self.test_ratelimit = dummy_filter_factory(conf_dict)(FakeApp())
        ratelimit.http_connect = mock_http_connect(204)
        self.test_ratelimit.log_sleep_time_seconds = .00001
        req = Request.blank('/v/a')
        req.environ['swift.cache'] = FakeMemcache()
        begin = time.time()

        class rate_caller(Thread):

            def __init__(self, parent, name):
                Thread.__init__(self)
                self.parent = parent
                self.name = name

            def run(self):
                self.result1 = self.parent.test_ratelimit(req.environ,
                                                          start_response)
                time.sleep(.1)
                self.result2 = self.parent.test_ratelimit(req.environ,
                                                          start_response)
        nt = 3
        threads = []
        for i in range(nt):
            rc = rate_caller(self, "thread %s" % i)
            rc.start()
            threads.append(rc)
        for thread in threads:
            thread.join()
        all_results = [''.join(t.result1) for t in threads]
        all_results += [''.join(t.result2) for t in threads]
        the_498s = [t for t in all_results if t.startswith('Slow down')]
        self.assertEquals(len(the_498s), 2)
        time_took = time.time() - begin
        self.assert_(1.5 <= round(time_took, 1) < 1.7, time_took)

    def test_ratelimit_max_rate_multiple_acc(self):
        num_calls = 4
        current_rate = 2
        conf_dict = {'account_ratelimit': current_rate,
                     'max_sleep_time_seconds': 2}
        fake_memcache = FakeMemcache()

        the_app = ratelimit.RateLimitMiddleware(None, conf_dict,
                                                logger=FakeLogger())
        the_app.memcache_client = fake_memcache
        req = lambda: None
        req.method = 'GET'

        class rate_caller(Thread):

            def __init__(self, name):
                self.myname = name
                Thread.__init__(self)

            def run(self):
                for j in range(num_calls):
                    self.result = the_app.handle_ratelimit(req, self.myname,
                                                           None, None)

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
        # the all 15 threads still take 1.5 secs
        self.assert_(1.5 <= round(time_took, 1) < 1.7)

    def test_ratelimit_acc_vrs_container(self):
        conf_dict = {'clock_accuracy': 1000,
                     'account_ratelimit': 10,
                     'max_sleep_time_seconds': 4,
                     'container_ratelimit_10': 6,
                     'container_ratelimit_50': 2,
                     'container_ratelimit_75': 1}
        self.test_ratelimit = dummy_filter_factory(conf_dict)(FakeApp())
        ratelimit.http_connect = mock_http_connect(204)
        req = Request.blank('/v/a/c')
        req.environ['swift.cache'] = FakeMemcache()
        cont_key = get_container_memcache_key('a', 'c')

        class rate_caller(Thread):

            def __init__(self, parent, name):
                Thread.__init__(self)
                self.parent = parent
                self.name = name

            def run(self):
                self.result = self.parent.test_ratelimit(req.environ,
                                                         start_response)

        def runthreads(threads, nt):
            for i in range(nt):
                rc = rate_caller(self, "thread %s" % i)
                rc.start()
                threads.append(rc)
            for thread in threads:
                thread.join()

        begin = time.time()
        req.environ['swift.cache'].set(cont_key, {'container_size': 20})
        begin = time.time()
        threads = []
        runthreads(threads, 3)
        time_took = time.time() - begin
        self.assert_(round(time_took, 1) == .4)

    def test_call_invalid_path(self):
        env = {'REQUEST_METHOD': 'GET',
               'SCRIPT_NAME': '',
               'PATH_INFO': '//v1/AUTH_1234567890',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '80',
               'swift.cache': FakeMemcache(),
               'SERVER_PROTOCOL': 'HTTP/1.0'}

        app = lambda *args, **kwargs: None
        rate_mid = ratelimit.RateLimitMiddleware(app, {},
                                                 logger=FakeLogger())

        class a_callable(object):

            def __call__(self, *args, **kwargs):
                pass
        resp = rate_mid.__call__(env, a_callable())
        self.assert_('404 Not Found' in resp[0])

    def test_no_memcache(self):
        current_rate = 13
        num_calls = 5
        conf_dict = {'account_ratelimit': current_rate}
        self.test_ratelimit = ratelimit.filter_factory(conf_dict)(FakeApp())
        ratelimit.http_connect = mock_http_connect(204)
        req = Request.blank('/v/a')
        req.environ['swift.cache'] = None
        make_app_call = lambda: self.test_ratelimit(req.environ,
                                                    start_response)
        self._run(make_app_call, num_calls, current_rate)


if __name__ == '__main__':
    unittest.main()
