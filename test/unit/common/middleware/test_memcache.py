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

from webob import Request

from swift.common.middleware import memcache
from swift.common.memcached import MemcacheRing

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
        self.assertTrue(isinstance(resp['swift.cache'], MemcacheRing))

if __name__ == '__main__':
    unittest.main()
