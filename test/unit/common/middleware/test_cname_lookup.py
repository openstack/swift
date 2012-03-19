# Copyright (c) 2010-2012 OpenStack, LLC.
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
from nose import SkipTest

from webob import Request

try:
    # this test requires the dnspython package to be installed
    from swift.common.middleware import cname_lookup
    skip = False
except ImportError:
    skip = True

class FakeApp(object):

    def __call__(self, env, start_response):
        return "FAKE APP"


def start_response(*args):
    pass


class TestCNAMELookup(unittest.TestCase):

    def setUp(self):
        if skip:
            raise SkipTest
        self.app = cname_lookup.CNAMELookupMiddleware(FakeApp(),
                                                      {'lookup_depth': 2})

    def test_passthrough(self):

        def my_lookup(d):
            return 0, d
        cname_lookup.lookup_cname = my_lookup

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'foo.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'foo.example.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

    def test_good_lookup(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        
        def my_lookup(d):
            return 0, '%s.example.com' % d
        cname_lookup.lookup_cname = my_lookup
        
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

    def test_lookup_chain_too_long(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        
        def my_lookup(d):
            if d == 'mysite.com':
                site = 'level1.foo.com'
            elif d == 'level1.foo.com':
                site = 'level2.foo.com'
            elif d == 'level2.foo.com':
                site = 'bar.example.com'
            return 0, site
        cname_lookup.lookup_cname = my_lookup
        
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, ['CNAME lookup failed after 2 tries'])

    def test_lookup_chain_bad_target(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        
        def my_lookup(d):
            return 0, 'some.invalid.site.com'
        cname_lookup.lookup_cname = my_lookup
        
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp,
                         ['CNAME lookup failed to resolve to a valid domain'])

    def test_something_weird(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        
        def my_lookup(d):
            return 0, None
        cname_lookup.lookup_cname = my_lookup
        
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp,
                         ['CNAME lookup failed to resolve to a valid domain'])

    def test_with_memcache(self):
        def my_lookup(d):
            return 0, '%s.example.com' % d
        cname_lookup.lookup_cname = my_lookup
        class memcache_stub(object):
            def __init__(self):
                self.cache = {}
            def get(self, key):
                return self.cache.get(key, None)
            def set(self, key, value, *a, **kw):
                self.cache[key] = value
        memcache = memcache_stub()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'swift.cache': memcache},
                            headers={'Host': 'mysite.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'swift.cache': memcache},
                            headers={'Host': 'mysite.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

    def test_cname_matching_ending_not_domain(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'foo.com'})

        def my_lookup(d):
            return 0, 'c.aexample.com'
        cname_lookup.lookup_cname = my_lookup

        resp = self.app(req.environ, start_response)
        self.assertEquals(resp,
                         ['CNAME lookup failed to resolve to a valid domain'])

    def test_cname_configured_with_empty_storage_domain(self):
        app = cname_lookup.CNAMELookupMiddleware(FakeApp(),
                                                {'storage_domain': '',
                                                 'lookup_depth': 2})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.a.example.com'})

        def my_lookup(d):
            return 0, None
        cname_lookup.lookup_cname = my_lookup

        resp = app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')
