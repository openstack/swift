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
from ConfigParser import NoSectionError, NoOptionError

from swift.common.middleware import memcache
from swift.common.memcached import MemcacheRing
from swift.common.swob import Request


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
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = ExcConfigParser
        count = 0
        try:
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
                try:
                    memcache.MemcacheMiddleware(FakeApp(), d)
                except Exception as err:
                    self.assertEquals(
                        str(err),
                        "read called with '/etc/swift/memcache.conf'")
                    count += 1
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(count, 7)

    def test_conf_set_no_read(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = ExcConfigParser
        exc = None
        try:
            memcache.MemcacheMiddleware(
                FakeApp(), {'memcache_servers': '1.2.3.4:5',
                            'memcache_serialization_support': '2',
                            'memcache_max_connections': '30'})
        except Exception as err:
            exc = err
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(exc, None)

    def test_conf_default(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = EmptyConfigParser
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '127.0.0.1:11211')
        self.assertEquals(app.memcache._allow_pickle, False)
        self.assertEquals(app.memcache._allow_unpickle, False)
        self.assertEquals(
            app.memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_inline(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser()
        try:
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0',
                 'memcache_max_connections': '5'})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '6.7.8.9:10')
        self.assertEquals(app.memcache._allow_pickle, True)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 5)

    def test_conf_extra_no_section(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser(section='foobar')
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '127.0.0.1:11211')
        self.assertEquals(app.memcache._allow_pickle, False)
        self.assertEquals(app.memcache._allow_unpickle, False)
        self.assertEquals(
            app.memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_extra_no_option(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser(
            memcache_servers='error', memcache_serialization_support='error',
            memcache_max_connections='error')
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '127.0.0.1:11211')
        self.assertEquals(app.memcache._allow_pickle, False)
        self.assertEquals(app.memcache._allow_unpickle, False)
        self.assertEquals(
            app.memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_inline_other_max_conn(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser()
        try:
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0',
                 'max_connections': '5'})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '6.7.8.9:10')
        self.assertEquals(app.memcache._allow_pickle, True)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 5)

    def test_conf_inline_bad_max_conn(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser()
        try:
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0',
                 'max_connections': 'bad42'})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '6.7.8.9:10')
        self.assertEquals(app.memcache._allow_pickle, True)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 4)

    def test_conf_from_extra_conf(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser()
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '1.2.3.4:5')
        self.assertEquals(app.memcache._allow_pickle, False)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['1.2.3.4:5'].max_size, 4)

    def test_conf_from_extra_conf_bad_max_conn(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser(
            memcache_max_connections='bad42')
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '1.2.3.4:5')
        self.assertEquals(app.memcache._allow_pickle, False)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['1.2.3.4:5'].max_size, 2)

    def test_conf_from_inline_and_maxc_from_extra_conf(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser()
        try:
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_serialization_support': '0'})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '6.7.8.9:10')
        self.assertEquals(app.memcache._allow_pickle, True)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 4)

    def test_conf_from_inline_and_sers_from_extra_conf(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = get_config_parser()
        try:
            app = memcache.MemcacheMiddleware(
                FakeApp(),
                {'memcache_servers': '6.7.8.9:10',
                 'memcache_max_connections': '42'})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '6.7.8.9:10')
        self.assertEquals(app.memcache._allow_pickle, False)
        self.assertEquals(app.memcache._allow_unpickle, True)
        self.assertEquals(
            app.memcache._client_cache['6.7.8.9:10'].max_size, 42)

    def test_filter_factory(self):
        factory = memcache.filter_factory({'max_connections': '3'},
                                          memcache_servers='10.10.10.10:10',
                                          memcache_serialization_support='1')
        thefilter = factory('myapp')
        self.assertEquals(thefilter.app, 'myapp')
        self.assertEquals(thefilter.memcache_servers, '10.10.10.10:10')
        self.assertEquals(thefilter.memcache._allow_pickle, False)
        self.assertEquals(thefilter.memcache._allow_unpickle, True)
        self.assertEquals(
            thefilter.memcache._client_cache['10.10.10.10:10'].max_size, 3)

if __name__ == '__main__':
    unittest.main()
