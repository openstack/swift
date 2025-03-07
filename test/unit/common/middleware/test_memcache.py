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

import os
from textwrap import dedent
import unittest

from eventlet.green import ssl
from unittest import mock

from swift.common.middleware import memcache
from swift.common.memcached import MemcacheRing
from swift.common.swob import Request
from swift.common.wsgi import loadapp

from test.unit import with_tempdir, patch_policies


class FakeApp(object):
    def __call__(self, env, start_response):
        return env


def start_response(*args):
    pass


class TestCacheMiddleware(unittest.TestCase):

    def setUp(self):
        self.app = memcache.MemcacheMiddleware(FakeApp(), {})

    def test_cache_middleware(self):
        req = Request.blank('/something', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertTrue('swift.cache' in resp)
        self.assertIsInstance(resp['swift.cache'], MemcacheRing)

    def test_filter_factory(self):
        factory = memcache.filter_factory({'max_connections': '3'},
                                          memcache_servers='10.10.10.10:10')
        thefilter = factory('myapp')
        self.assertEqual(thefilter.app, 'myapp')
        self.assertEqual(thefilter.memcache.memcache_servers,
                         ['10.10.10.10:10'])
        self.assertEqual(
            thefilter.memcache._client_cache['10.10.10.10:10'].max_size, 3)

    @patch_policies
    def _loadapp(self, proxy_config_path):
        """
        Load a proxy from an app.conf to get the memcache_ring

        :returns: the memcache_ring of the memcache middleware filter
        """
        with mock.patch('swift.proxy.server.Ring'):
            app = loadapp(proxy_config_path)
        memcache_ring = None
        while True:
            memcache_ring = getattr(app, 'memcache', None)
            if memcache_ring:
                break
            app = app.app
        return memcache_ring

    @with_tempdir
    def test_real_config(self, tempdir):
        config = """
        [pipeline:main]
        pipeline = cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache
        """
        config_path = os.path.join(tempdir, 'test.conf')
        with open(config_path, 'w') as f:
            f.write(dedent(config))
        memcache_ring = self._loadapp(config_path)
        # only one server by default
        self.assertEqual(list(memcache_ring._client_cache.keys()),
                         ['127.0.0.1:11211'])
        # extra options
        self.assertEqual(memcache_ring._connect_timeout, 0.3)
        self.assertEqual(memcache_ring._pool_timeout, 1.0)
        # tries is limited to server count
        self.assertEqual(memcache_ring._tries, 1)
        self.assertEqual(memcache_ring._io_timeout, 2.0)
        self.assertEqual(memcache_ring.item_size_warning_threshold, -1)

    @with_tempdir
    def test_real_config_with_options(self, tempdir):
        config = """
        [pipeline:main]
        pipeline = cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache
        memcache_servers = 10.0.0.1:11211,10.0.0.2:11211,10.0.0.3:11211,
            10.0.0.4:11211
        connect_timeout = 1.0
        pool_timeout = 0.5
        tries = 4
        io_timeout = 1.0
        tls_enabled = true
        item_size_warning_threshold = 1000
        """
        config_path = os.path.join(tempdir, 'test.conf')
        with open(config_path, 'w') as f:
            f.write(dedent(config))
        memcache_ring = self._loadapp(config_path)
        self.assertEqual(sorted(memcache_ring._client_cache.keys()),
                         ['10.0.0.%d:11211' % i for i in range(1, 5)])
        # extra options
        self.assertEqual(memcache_ring._connect_timeout, 1.0)
        self.assertEqual(memcache_ring._pool_timeout, 0.5)
        # tries is limited to server count
        self.assertEqual(memcache_ring._tries, 4)
        self.assertEqual(memcache_ring._io_timeout, 1.0)
        self.assertEqual(memcache_ring._error_limit_count, 10)
        self.assertEqual(memcache_ring._error_limit_time, 60)
        self.assertEqual(memcache_ring._error_limit_duration, 60)
        self.assertIsInstance(
            list(memcache_ring._client_cache.values())[0]._tls_context,
            ssl.SSLContext)
        self.assertEqual(memcache_ring.item_size_warning_threshold, 1000)

    @with_tempdir
    def test_real_memcache_config(self, tempdir):
        proxy_config = """
        [DEFAULT]
        swift_dir = %s

        [pipeline:main]
        pipeline = cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache
        connect_timeout = 1.0
        """ % tempdir
        proxy_config_path = os.path.join(tempdir, 'test.conf')
        with open(proxy_config_path, 'w') as f:
            f.write(dedent(proxy_config))

        memcache_config = """
        [memcache]
        memcache_servers = 10.0.0.1:11211,10.0.0.2:11211,10.0.0.3:11211,
            10.0.0.4:11211
        connect_timeout = 0.5
        io_timeout = 1.0
        error_suppression_limit = 0
        error_suppression_interval = 1.5
        item_size_warning_threshold = 50
        """
        memcache_config_path = os.path.join(tempdir, 'memcache.conf')
        with open(memcache_config_path, 'w') as f:
            f.write(dedent(memcache_config))
        memcache_ring = self._loadapp(proxy_config_path)
        self.assertEqual(sorted(memcache_ring._client_cache.keys()),
                         ['10.0.0.%d:11211' % i for i in range(1, 5)])
        # proxy option takes precedence
        self.assertEqual(memcache_ring._connect_timeout, 1.0)
        # default tries are not limited by servers
        self.assertEqual(memcache_ring._tries, 3)
        # memcache conf options are defaults
        self.assertEqual(memcache_ring._io_timeout, 1.0)
        self.assertEqual(memcache_ring._error_limit_count, 0)
        self.assertEqual(memcache_ring._error_limit_time, 1.5)
        self.assertEqual(memcache_ring._error_limit_duration, 1.5)
        self.assertEqual(memcache_ring.item_size_warning_threshold, 50)


if __name__ == '__main__':
    unittest.main()
