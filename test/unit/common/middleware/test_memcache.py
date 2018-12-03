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

import mock
from six.moves.configparser import NoSectionError, NoOptionError

from swift.common.middleware import memcache
from swift.common.memcached import MemcacheRing
from swift.common.swob import Request
from swift.common.wsgi import loadapp

from test.unit import with_tempdir, patch_policies


class FakeApp(object):
    def __call__(self, env, start_response):
        return env


class ExcConfigParser(object):

    def read(self, path):
        raise Exception('read called with %r' % path)


class EmptyConfigParser(object):

    def read(self, path):
        return False


def get_config_parser(memcache_servers='1.2.3.4:5',
                      memcache_serialization_support='1',
                      memcache_max_connections='4',
                      section='memcache'):
    _srvs = memcache_servers
    _sers = memcache_serialization_support
    _maxc = memcache_max_connections
    _section = section

    class SetConfigParser(object):

        def items(self, section_name):
            if section_name != section:
                raise NoSectionError(section_name)
            return {
                'memcache_servers': memcache_servers,
                'memcache_serialization_support':
                memcache_serialization_support,
                'memcache_max_connections': memcache_max_connections,
            }

        def read(self, path):
            return True

        def get(self, section, option):
            if _section == section:
                if option == 'memcache_servers':
                    if _srvs == 'error':
                        raise NoOptionError(option, section)
                    return _srvs
                elif option == 'memcache_serialization_support':
                    if _sers == 'error':
                        raise NoOptionError(option, section)
                    return _sers
                elif option in ('memcache_max_connections',
                                'max_connections'):
                    if _maxc == 'error':
                        raise NoOptionError(option, section)
                    return _maxc
                else:
                    raise NoOptionError(option, section)
            else:
                raise NoSectionError(option)

    return SetConfigParser


def start_response(*args):
    pass


class TestCacheMiddleware(unittest.TestCase):

    def setUp(self):
        self.app = memcache.MemcacheMiddleware(FakeApp(), {})

    def test_cache_middleware(self):
        req = Request.blank('/something', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertTrue('swift.cache' in resp)
        self.assertTrue(isinstance(resp['swift.cache'], MemcacheRing))

    def test_conf_default_read(self):
        with mock.patch.object(memcache, 'ConfigParser', ExcConfigParser):
            for d in ({},
                      {'memcache_servers': '6.7.8.9:10'},
                      {'memcache_serialization_support': '0'},
                      {'memcache_max_connections': '30'},
                      {'memcache_servers': '6.7.8.9:10',
                       'memcache_serialization_support': '0'},
                      {'memcache_servers': '6.7.8.9:10',
                       'memcache_max_connections': '30'},
                      {'memcache_serialization_support': '0',
                       'memcache_max_connections': '30'}
                      ):
                with self.assertRaises(Exception) as catcher:
                    memcache.MemcacheMiddleware(FakeApp(), d)
                self.assertEqual(
                    str(catcher.exception),
                    "read called with '/etc/swift/memcache.conf'")

    def test_conf_set_no_read(self):
        with mock.patch.object(memcache, 'ConfigParser', ExcConfigParser):
            exc = None
            try:
                memcache.MemcacheMiddleware(
                    FakeApp(), {'memcache_servers': '1.2.3.4:5',
                                'memcache_serialization_support': '2',
                                'memcache_max_connections': '30'})
            except Exception as err:
                exc = err
        self.assertIsNone(exc)

    def test_conf_default(self):
        with mock.patch.object(memcache, 'ConfigParser', EmptyConfigParser):
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        self.assertEqual(app.memcache_servers, '127.0.0.1:11211')
        self.assertEqual(app.memcache._allow_pickle, False)
        self.assertEqual(app.memcache._allow_unpickle, False)
        self.assertEqual(
            app.memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_inline(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser()):
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0',
                 'memcache_max_connections': '5'})
        self.assertEqual(app.memcache_servers, '6.7.8.9:10')
        self.assertEqual(app.memcache._allow_pickle, True)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 5)

    def test_conf_extra_no_section(self):
        with mock.patch.object(memcache, 'ConfigParser',
                               get_config_parser(section='foobar')):
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        self.assertEqual(app.memcache_servers, '127.0.0.1:11211')
        self.assertEqual(app.memcache._allow_pickle, False)
        self.assertEqual(app.memcache._allow_unpickle, False)
        self.assertEqual(
            app.memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_extra_no_option(self):
        replacement_parser = get_config_parser(
            memcache_servers='error', memcache_serialization_support='error',
            memcache_max_connections='error')
        with mock.patch.object(memcache, 'ConfigParser', replacement_parser):
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        self.assertEqual(app.memcache_servers, '127.0.0.1:11211')
        self.assertEqual(app.memcache._allow_pickle, False)
        self.assertEqual(app.memcache._allow_unpickle, False)
        self.assertEqual(
            app.memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_inline_other_max_conn(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser()):
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0',
                 'max_connections': '5'})
        self.assertEqual(app.memcache_servers, '6.7.8.9:10')
        self.assertEqual(app.memcache._allow_pickle, True)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 5)

    def test_conf_inline_bad_max_conn(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser()):
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0',
                 'max_connections': 'bad42'})
        self.assertEqual(app.memcache_servers, '6.7.8.9:10')
        self.assertEqual(app.memcache._allow_pickle, True)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 4)

    def test_conf_from_extra_conf(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser()):
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        self.assertEqual(app.memcache_servers, '1.2.3.4:5')
        self.assertEqual(app.memcache._allow_pickle, False)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['1.2.3.4:5'].max_size, 4)

    def test_conf_from_extra_conf_bad_max_conn(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser(
                memcache_max_connections='bad42')):
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        self.assertEqual(app.memcache_servers, '1.2.3.4:5')
        self.assertEqual(app.memcache._allow_pickle, False)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['1.2.3.4:5'].max_size, 2)

    def test_conf_from_inline_and_maxc_from_extra_conf(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser()):
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0'})
        self.assertEqual(app.memcache_servers, '6.7.8.9:10')
        self.assertEqual(app.memcache._allow_pickle, True)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 4)

    def test_conf_from_inline_and_sers_from_extra_conf(self):
        with mock.patch.object(memcache, 'ConfigParser', get_config_parser()):
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_max_connections': '42'})
        self.assertEqual(app.memcache_servers, '6.7.8.9:10')
        self.assertEqual(app.memcache._allow_pickle, False)
        self.assertEqual(app.memcache._allow_unpickle, True)
        self.assertEqual(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 42)

    def test_filter_factory(self):
        factory = memcache.filter_factory({'max_connections': '3'},
                                          memcache_servers='10.10.10.10:10',
                                          memcache_serialization_support='1')
        thefilter = factory('myapp')
        self.assertEqual(thefilter.app, 'myapp')
        self.assertEqual(thefilter.memcache_servers, '10.10.10.10:10')
        self.assertEqual(thefilter.memcache._allow_pickle, False)
        self.assertEqual(thefilter.memcache._allow_unpickle, True)
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


if __name__ == '__main__':
    unittest.main()
