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
import mock
from nose import SkipTest

try:
    # this test requires the dnspython package to be installed
    import dns.resolver  # noqa
except ImportError:
    skip = True
else:  # executed if the try has no errors
    skip = False
from swift.common.middleware import cname_lookup
from swift.common.swob import Request


class FakeApp(object):

    def __call__(self, env, start_response):
        return "FAKE APP"


def start_response(*args):
    pass


original_lookup = cname_lookup.lookup_cname


class TestCNAMELookup(unittest.TestCase):

    def setUp(self):
        if skip:
            raise SkipTest
        self.app = cname_lookup.CNAMELookupMiddleware(FakeApp(),
                                                      {'lookup_depth': 2})

    def test_pass_ip_addresses(self):
        cname_lookup.lookup_cname = original_lookup

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': '10.134.23.198'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'fc00:7ea1:f155::6321:8841'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

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
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'foo.example.com'},
                            headers={'Host': None})
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
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'mysite.com'},
                            headers={'Host': None})
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

    def test_storage_domains_conf_format(self):
        conf = {'storage_domain': 'foo.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEquals(app.storage_domain, ['.foo.com'])

        conf = {'storage_domain': 'foo.com, '}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEquals(app.storage_domain, ['.foo.com'])

        conf = {'storage_domain': 'foo.com, bar.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEquals(app.storage_domain, ['.foo.com', '.bar.com'])

        conf = {'storage_domain': 'foo.com, .bar.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEquals(app.storage_domain, ['.foo.com', '.bar.com'])

        conf = {'storage_domain': '.foo.com, .bar.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEquals(app.storage_domain, ['.foo.com', '.bar.com'])

    def test_multiple_storage_domains(self):
        conf = {'storage_domain': 'storage1.com, storage2.com',
                'lookup_depth': 2}
        app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)

        def do_test(lookup_back):
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                                headers={'Host': 'c.a.example.com'})
            module = 'swift.common.middleware.cname_lookup.lookup_cname'
            with mock.patch(module, lambda x: (0, lookup_back)):
                return app(req.environ, start_response)

        resp = do_test('c.storage1.com')
        self.assertEquals(resp, 'FAKE APP')

        resp = do_test('c.storage2.com')
        self.assertEquals(resp, 'FAKE APP')

        bad_domain = ['CNAME lookup failed to resolve to a valid domain']
        resp = do_test('c.badtest.com')
        self.assertEquals(resp, bad_domain)
