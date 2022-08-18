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

from swift.common.swob import Request, HTTPMovedPermanently
from swift.common.middleware import domain_remap
from swift.common import registry


class FakeApp(object):

    def __call__(self, env, start_response):
        start_response('200 OK', [])
        return [env['PATH_INFO'].encode('latin-1')]


class RedirectSlashApp(object):

    def __call__(self, env, start_response):
        loc = env['PATH_INFO'] + '/'
        return HTTPMovedPermanently(location=loc)(env, start_response)


def start_response(*args):
    pass


class TestDomainRemap(unittest.TestCase):

    def setUp(self):
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), {})

    def test_domain_remap_passthrough(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'example.com'},
                            headers={'Host': None})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'example.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/'])

    def test_domain_remap_account(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'AUTH_a.example.com'},
                            headers={'Host': None})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/'])
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH-uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_uuid/'])

    def test_domain_remap_account_container(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/'])

    def test_domain_remap_extra_subdomains(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'x.y.c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'Bad domain in host header'])

    def test_domain_remap_account_with_path_root_container(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/v1'])

    def test_domain_remap_account_with_path_root_unicode_container(self):
        req = Request.blank('/%E4%BD%A0%E5%A5%BD',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/\xe4\xbd\xa0\xe5\xa5\xbd'])

    def test_domain_remap_account_container_with_path_root_obj(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/v1'])

    def test_domain_remap_account_container_with_path_obj_slash_v1(self):
        # Include http://localhost because urlparse used in Request.__init__
        # parse //v1 as http://v1
        req = Request.blank('http://localhost//v1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c//v1'])

    def test_domain_remap_account_container_with_root_path_obj_slash_v1(self):
        req = Request.blank('/v1//v1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/v1//v1'])

    def test_domain_remap_account_container_with_path_trailing_slash(self):
        req = Request.blank('/obj/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/obj/'])

    def test_domain_remap_account_container_with_path(self):
        req = Request.blank('/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/obj'])

    def test_domain_remap_account_container_with_path_root_and_path(self):
        req = Request.blank('/v1/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/v1/obj'])

    def test_domain_remap_with_path_root_and_path_no_slash(self):
        req = Request.blank('/v1obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/v1obj'])

    def test_domain_remap_account_matching_ending_not_domain(self):
        req = Request.blank('/dontchange', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.aexample.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/dontchange'])

    def test_domain_remap_configured_with_empty_storage_domain(self):
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(),
                                                      {'storage_domain': ''})
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/test'])

    def test_storage_domains_conf_format(self):
        conf = {'storage_domain': 'foo.com'}
        app = domain_remap.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com'])

        conf = {'storage_domain': 'foo.com, '}
        app = domain_remap.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com'])

        conf = {'storage_domain': 'foo.com, bar.com'}
        app = domain_remap.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com', '.bar.com'])

        conf = {'storage_domain': 'foo.com, .bar.com'}
        app = domain_remap.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com', '.bar.com'])

        conf = {'storage_domain': '.foo.com, .bar.com'}
        app = domain_remap.filter_factory(conf)(FakeApp())
        self.assertEqual(app.storage_domain, ['.foo.com', '.bar.com'])

    def test_domain_remap_configured_with_prefixes(self):
        conf = {'reseller_prefixes': 'PREFIX'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.prefix_uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/PREFIX_uuid/c/test'])

    def test_domain_remap_configured_with_bad_prefixes(self):
        conf = {'reseller_prefixes': 'UNKNOWN'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.prefix_uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/test'])

    def test_domain_remap_configured_with_no_prefixes(self):
        conf = {'reseller_prefixes': ''}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/uuid/c/test'])

    def test_domain_remap_add_prefix(self):
        conf = {'default_reseller_prefix': 'FOO'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/FOO_uuid/test'])

    def test_domain_remap_add_prefix_already_there(self):
        conf = {'default_reseller_prefix': 'AUTH'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'auth-uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_uuid/test'])

    def test_multiple_storage_domains(self):
        conf = {'storage_domain': 'storage1.com, storage2.com'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)

        def do_test(host):
            req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                                headers={'Host': host})
            return self.app(req.environ, start_response)

        resp = do_test('auth-uuid.storage1.com')
        self.assertEqual(resp, [b'/v1/AUTH_uuid/test'])

        resp = do_test('auth-uuid.storage2.com')
        self.assertEqual(resp, [b'/v1/AUTH_uuid/test'])

        resp = do_test('auth-uuid.storage3.com')
        self.assertEqual(resp, [b'/test'])

    def test_domain_remap_redirect(self):
        app = domain_remap.DomainRemapMiddleware(RedirectSlashApp(), {})

        req = Request.blank('/cont', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'auth-uuid.example.com'})
        resp = req.get_response(app)
        self.assertEqual(resp.status_int, 301)
        self.assertEqual(resp.headers.get('Location'),
                         'http://auth-uuid.example.com/cont/')

        req = Request.blank('/cont/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'auth-uuid.example.com'})
        resp = req.get_response(app)
        self.assertEqual(resp.status_int, 301)
        self.assertEqual(resp.headers.get('Location'),
                         'http://auth-uuid.example.com/cont/test/')

        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'cont.auth-uuid.example.com'})
        resp = req.get_response(app)
        self.assertEqual(resp.status_int, 301)
        self.assertEqual(resp.headers.get('Location'),
                         'http://cont.auth-uuid.example.com/test/')


class TestDomainRemapClientMangling(unittest.TestCase):
    def setUp(self):
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), {
            'mangle_client_paths': True})

    def test_domain_remap_account_with_path_root_container(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/'])

    def test_domain_remap_account_container_with_path_root_obj(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/'])

    def test_domain_remap_account_container_with_path_obj_slash_v1(self):
        # Include http://localhost because urlparse used in Request.__init__
        # parse //v1 as http://v1
        req = Request.blank('http://localhost//v1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c//v1'])

    def test_domain_remap_account_container_with_root_path_obj_slash_v1(self):
        req = Request.blank('/v1//v1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c//v1'])

    def test_domain_remap_account_container_with_path_trailing_slash(self):
        req = Request.blank('/obj/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/obj/'])

    def test_domain_remap_account_container_with_path_root_and_path(self):
        req = Request.blank('/v1/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/obj'])

    def test_domain_remap_with_path_root_and_path_no_slash(self):
        req = Request.blank('/v1obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'/v1/AUTH_a/c/v1obj'])


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        registry._swift_info = {}
        registry._swift_admin_info = {}

    def test_registered_defaults(self):
        domain_remap.filter_factory({})
        swift_info = registry.get_swift_info()
        self.assertIn('domain_remap', swift_info)
        self.assertEqual(swift_info['domain_remap'], {
            'default_reseller_prefix': None})

    def test_registered_nondefaults(self):
        domain_remap.filter_factory({'default_reseller_prefix': 'cupcake',
                                     'mangle_client_paths': 'yes'})
        swift_info = registry.get_swift_info()
        self.assertIn('domain_remap', swift_info)
        self.assertEqual(swift_info['domain_remap'], {
            'default_reseller_prefix': 'cupcake'})


if __name__ == '__main__':
    unittest.main()
