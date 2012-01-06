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
from ConfigParser import NoSectionError, NoOptionError

from webob import Request

from swift.common.middleware import memcache
from swift.common.memcached import MemcacheRing

class FakeApp(object):
    def __call__(self, env, start_response):
        return env


class ExcConfigParser(object):

    def read(self, path):
        raise Exception('read called with %r' % path)


class EmptyConfigParser(object):

    def read(self, path):
        return False


class SetConfigParser(object):

    def read(self, path):
        return True

    def get(self, section, option):
        if section == 'memcache':
            if option == 'memcache_servers':
                return '1.2.3.4:5'
            else:
                raise NoOptionError(option)
        else:
            raise NoSectionError(option)


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
        exc = None
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        except Exception, err:
            exc = err
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(str(exc),
            "read called with '/etc/swift/memcache.conf'")

    def test_conf_set_no_read(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = ExcConfigParser
        exc = None
        try:
            app = memcache.MemcacheMiddleware(
                    FakeApp(), {'memcache_servers': '1.2.3.4:5'})
        except Exception, err:
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

    def test_conf_from_extra_conf(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = SetConfigParser
        try:
            app = memcache.MemcacheMiddleware(FakeApp(), {})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '1.2.3.4:5')

    def test_conf_from_inline_conf(self):
        orig_parser = memcache.ConfigParser
        memcache.ConfigParser = SetConfigParser
        try:
            app = memcache.MemcacheMiddleware(
                    FakeApp(), {'memcache_servers': '6.7.8.9:10'})
        finally:
            memcache.ConfigParser = orig_parser
        self.assertEquals(app.memcache_servers, '6.7.8.9:10')


if __name__ == '__main__':
    unittest.main()
