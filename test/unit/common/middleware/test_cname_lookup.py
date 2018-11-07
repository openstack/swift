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

try:
    # this test requires the dnspython package to be installed
    import dns.resolver  # noqa
    import dns.exception
except ImportError:
    skip = True
else:  # executed if the try has no errors
    skip = False
from swift.common import utils
from swift.common.middleware import cname_lookup
from swift.common.swob import Request, HTTPMovedPermanently


class FakeApp(object):

    def __call__(self, env, start_response):
        start_response('200 OK', [])
        return [b"FAKE APP"]


class RedirectSlashApp(object):

    def __call__(self, env, start_response):
        loc = env['PATH_INFO'] + '/'
        return HTTPMovedPermanently(location=loc)(env, start_response)


def start_response(*args):
    pass


class TestCNAMELookup(unittest.TestCase):

    @unittest.skipIf(skip, "can't import dnspython")
    def setUp(self):
        self.app = cname_lookup.CNAMELookupMiddleware(FakeApp(),
                                                      {'lookup_depth': 2})

    def test_pass_ip_addresses(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': '10.134.23.198'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'fc00:7ea1:f155::6321:8841'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, d))
    def test_passthrough(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'foo.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'foo.example.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'foo.example.com'},
                            headers={'Host': None})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, '%s.example.com' % d))
    def test_good_lookup(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'mysite.com'},
                            headers={'Host': None})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])

    def test_lookup_chain_too_long(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})

        def my_lookup(d, r):
            if d == 'mysite.com':
                site = 'level1.foo.com'
            elif d == 'level1.foo.com':
                site = 'level2.foo.com'
            elif d == 'level2.foo.com':
                site = 'bar.example.com'
            return 0, site

        with mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                        new=my_lookup):
            resp = self.app(req.environ, start_response)
            self.assertEqual(resp, [b'CNAME lookup failed after 2 tries'])

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, 'some.invalid.site.com'))
    def test_lookup_chain_bad_target(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp,
                         [b'CNAME lookup failed to resolve to a valid domain'])

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, None))
    def test_something_weird(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp,
                         [b'CNAME lookup failed to resolve to a valid domain'])

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, '%s.example.com' % d))
    def test_with_memcache(self):
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
        self.assertEqual(resp, [b'FAKE APP'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'swift.cache': memcache},
                            headers={'Host': 'mysite.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])

    def test_caching(self):
        fail_to_resolve = [b'CNAME lookup failed to resolve to a valid domain']

        class memcache_stub(object):
            def __init__(self):
                self.cache = {}

            def get(self, key):
                return self.cache.get(key, None)

            def set(self, key, value, *a, **kw):
                self.cache[key] = value

        module = 'swift.common.middleware.cname_lookup.lookup_cname'
        dns_module = 'dns.resolver.Resolver.query'
        memcache = memcache_stub()

        with mock.patch(module) as m:
            m.return_value = (3600, 'c.example.com')
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'swift.cache': memcache},
                                headers={'Host': 'mysite2.com'})
            resp = self.app(req.environ, start_response)
            self.assertEqual(resp, [b'FAKE APP'])
            self.assertEqual(m.call_count, 1)
            self.assertEqual(memcache.cache.get('cname-mysite2.com'),
                             'c.example.com')
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'swift.cache': memcache},
                                headers={'Host': 'mysite2.com'})
            resp = self.app(req.environ, start_response)
            self.assertEqual(resp, [b'FAKE APP'])
            self.assertEqual(m.call_count, 1)
            self.assertEqual(memcache.cache.get('cname-mysite2.com'),
                             'c.example.com')

        for exc, num in ((dns.resolver.NXDOMAIN(), 3),
                         (dns.resolver.NoAnswer(), 4)):
            with mock.patch(dns_module) as m:
                m.side_effect = exc
                req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                                  'swift.cache': memcache},
                                    headers={'Host': 'mysite%d.com' % num})
                resp = self.app(req.environ, start_response)
                self.assertEqual(resp, fail_to_resolve)
                self.assertEqual(m.call_count, 1)
                self.assertEqual(memcache.cache.get('cname-mysite3.com'),
                                 False)
                req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                                  'swift.cache': memcache},
                                    headers={'Host': 'mysite%d.com' % num})
                resp = self.app(req.environ, start_response)
                self.assertEqual(resp, fail_to_resolve)
                self.assertEqual(m.call_count, 1)
                self.assertEqual(
                    memcache.cache.get('cname-mysite%d.com' % num), False)

        with mock.patch(dns_module) as m:
            m.side_effect = dns.exception.DNSException()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'swift.cache': memcache},
                                headers={'Host': 'mysite5.com'})
            resp = self.app(req.environ, start_response)
            self.assertEqual(resp, fail_to_resolve)
            self.assertEqual(m.call_count, 1)
            self.assertFalse('cname-mysite5.com' in memcache.cache)
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'swift.cache': memcache},
                                headers={'Host': 'mysite5.com'})
            resp = self.app(req.environ, start_response)
            self.assertEqual(resp, fail_to_resolve)
            self.assertEqual(m.call_count, 2)
            self.assertFalse('cname-mysite5.com' in memcache.cache)

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, 'c.aexample.com'))
    def test_cname_matching_ending_not_domain(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'foo.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp,
                         [b'CNAME lookup failed to resolve to a valid domain'])

    @mock.patch('swift.common.middleware.cname_lookup.lookup_cname',
                new=lambda d, r: (0, None))
    def test_cname_configured_with_empty_storage_domain(self):
        app = cname_lookup.CNAMELookupMiddleware(FakeApp(),
                                                 {'storage_domain': '',
                                                  'lookup_depth': 2})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.a.example.com'})
        resp = app(req.environ, start_response)
        self.assertEqual(resp, [b'FAKE APP'])

    def test_storage_domains_conf_format(self):
        conf = {'storage_domain': 'foo.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com'])

        conf = {'storage_domain': 'foo.com, '}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com'])

        conf = {'storage_domain': 'foo.com, bar.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com', '.bar.com'])

        conf = {'storage_domain': 'foo.com, .bar.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com', '.bar.com'])

        conf = {'storage_domain': '.foo.com, .bar.com'}
        app = cname_lookup.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com', '.bar.com'])

    def test_multiple_storage_domains(self):
        conf = {'storage_domain': 'storage1.com, storage2.com',
                'lookup_depth': 2}
        app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)

        def do_test(lookup_back):
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                                headers={'Host': 'c.a.example.com'})
            module = 'swift.common.middleware.cname_lookup.lookup_cname'
            with mock.patch(module, lambda d, r: (0, lookup_back)):
                return app(req.environ, start_response)

        resp = do_test('c.storage1.com')
        self.assertEqual(resp, [b'FAKE APP'])

        resp = do_test('c.storage2.com')
        self.assertEqual(resp, [b'FAKE APP'])

        bad_domain = [b'CNAME lookup failed to resolve to a valid domain']
        resp = do_test('c.badtest.com')
        self.assertEqual(resp, bad_domain)

    def test_host_is_storage_domain(self):
        conf = {'storage_domain': 'storage.example.com',
                'lookup_depth': 2}
        app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)

        def do_test(host):
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                                headers={'Host': host})
            return app(req.environ, start_response)

        bad_domain = [b'CNAME lookup failed to resolve to a valid domain']
        resp = do_test('c.badtest.com')
        self.assertEqual(resp, bad_domain)

        resp = do_test('storage.example.com')
        self.assertEqual(resp, [b'FAKE APP'])

    def test_resolution_to_storage_domain_exactly(self):
        conf = {'storage_domain': 'example.com',
                'lookup_depth': 1}
        app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'mysite.com'})
        module = 'swift.common.middleware.cname_lookup.lookup_cname'
        with mock.patch(module, lambda d, r: (0, 'example.com')):
            resp = app(req.environ, start_response)
            self.assertEqual(resp, [b'FAKE APP'])

    def test_redirect(self):
        app = cname_lookup.CNAMELookupMiddleware(RedirectSlashApp(), {})

        module = 'swift.common.middleware.cname_lookup.lookup_cname'
        with mock.patch(module, lambda d, r: (0, 'cont.acct.example.com')):
            req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                                headers={'Host': 'mysite.com'})
            resp = req.get_response(app)
            self.assertEqual(resp.status_int, 301)
            self.assertEqual(resp.headers.get('Location'),
                             'http://mysite.com/test/')

    def test_configured_nameservers(self):
        class MockedResolver(object):
            def __init__(self):
                self.nameservers = None
                self.nameserver_ports = None

            def query(self, *args, **kwargs):
                raise Exception('Stop processing')

            def reset(self):
                self.nameservers = None
                self.nameserver_ports = None

        mocked_resolver = MockedResolver()
        dns_module = 'dns.resolver.Resolver'

        # If no nameservers provided in conf, resolver nameservers is unset
        for conf in [{}, {'nameservers': ''}]:
            mocked_resolver.reset()
            with mock.patch(dns_module, return_value=mocked_resolver):
                app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
            self.assertIs(app.resolver, mocked_resolver)
            self.assertIsNone(mocked_resolver.nameservers)

        # If invalid nameservers provided, resolver nameservers is unset
        mocked_resolver.reset()
        conf = {'nameservers': '127.0.0.1, 127.0.0.2, a.b.c.d'}
        with mock.patch(dns_module, return_value=mocked_resolver):
            with self.assertRaises(ValueError) as exc_mgr:
                app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
            self.assertIn('Invalid cname_lookup/nameservers configuration',
                          str(exc_mgr.exception))

        # If nameservers provided in conf, resolver nameservers is set
        mocked_resolver.reset()
        conf = {'nameservers': '127.0.0.1'}
        with mock.patch(dns_module, return_value=mocked_resolver):
            app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
        self.assertIs(app.resolver, mocked_resolver)
        self.assertEqual(mocked_resolver.nameservers, ['127.0.0.1'])
        self.assertEqual(mocked_resolver.nameserver_ports, {})

        # IPv6 is OK
        mocked_resolver.reset()
        conf = {'nameservers': '[::1]'}
        with mock.patch(dns_module, return_value=mocked_resolver):
            app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
        self.assertIs(app.resolver, mocked_resolver)
        self.assertEqual(mocked_resolver.nameservers, ['::1'])
        self.assertEqual(mocked_resolver.nameserver_ports, {})

        # As are port overrides
        mocked_resolver.reset()
        conf = {'nameservers': '127.0.0.1:5354'}
        with mock.patch(dns_module, return_value=mocked_resolver):
            app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
        self.assertIs(app.resolver, mocked_resolver)
        self.assertEqual(mocked_resolver.nameservers, ['127.0.0.1'])
        self.assertEqual(mocked_resolver.nameserver_ports, {'127.0.0.1': 5354})

        # And IPv6 with port overrides
        mocked_resolver.reset()
        conf = {'nameservers': '[2001:db8::ff00:42:8329]:1234'}
        with mock.patch(dns_module, return_value=mocked_resolver):
            app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
        self.assertIs(app.resolver, mocked_resolver)
        self.assertEqual(mocked_resolver.nameservers, [
            '2001:db8::ff00:42:8329'])
        self.assertEqual(mocked_resolver.nameserver_ports, {
            '2001:db8::ff00:42:8329': 1234})

        # Also accept lists, and bring it all together
        mocked_resolver.reset()
        conf = {'nameservers': '[::1], 127.0.0.1:5354, '
                               '[2001:db8::ff00:42:8329]:1234'}
        with mock.patch(dns_module, return_value=mocked_resolver):
            app = cname_lookup.CNAMELookupMiddleware(FakeApp(), conf)
        self.assertIs(app.resolver, mocked_resolver)
        self.assertEqual(mocked_resolver.nameservers, [
            '::1', '127.0.0.1', '2001:db8::ff00:42:8329'])
        self.assertEqual(mocked_resolver.nameserver_ports, {
            '127.0.0.1': 5354, '2001:db8::ff00:42:8329': 1234})


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        cname_lookup.filter_factory({})
        swift_info = utils.get_swift_info()
        self.assertIn('cname_lookup', swift_info)
        self.assertEqual(swift_info['cname_lookup'].get('lookup_depth'), 1)

    def test_registered_nondefaults(self):
        cname_lookup.filter_factory({'lookup_depth': '2'})
        swift_info = utils.get_swift_info()
        self.assertIn('cname_lookup', swift_info)
        self.assertEqual(swift_info['cname_lookup'].get('lookup_depth'), 2)
