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

from swift.common.swob import Request
from swift.common.middleware import domain_remap
from swift.common import utils


class FakeApp(object):

    def __call__(self, env, start_response):
        return env['PATH_INFO']


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
        self.assertEqual(resp, '/')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'example.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/')

    def test_domain_remap_account(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'SERVER_NAME': 'AUTH_a.example.com'},
                            headers={'Host': None})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH-uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_uuid')

    def test_domain_remap_account_container(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a/c')

    def test_domain_remap_extra_subdomains(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'x.y.c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, ['Bad domain in host header'])

    def test_domain_remap_account_with_path_root(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a')

    def test_domain_remap_account_container_with_path_root(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a/c')

    def test_domain_remap_account_container_with_path(self):
        req = Request.blank('/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a/c/obj')

    def test_domain_remap_account_container_with_path_root_and_path(self):
        req = Request.blank('/v1/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_a/c/obj')

    def test_domain_remap_account_matching_ending_not_domain(self):
        req = Request.blank('/dontchange', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.aexample.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/dontchange')

    def test_domain_remap_configured_with_empty_storage_domain(self):
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(),
                                                      {'storage_domain': ''})
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/test')

    def test_domain_remap_configured_with_prefixes(self):
        conf = {'reseller_prefixes': 'PREFIX'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.prefix_uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/PREFIX_uuid/c/test')

    def test_domain_remap_configured_with_bad_prefixes(self):
        conf = {'reseller_prefixes': 'UNKNOWN'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.prefix_uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/test')

    def test_domain_remap_configured_with_no_prefixes(self):
        conf = {'reseller_prefixes': ''}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/uuid/c/test')

    def test_domain_remap_add_prefix(self):
        conf = {'default_reseller_prefix': 'FOO'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/FOO_uuid/test')

    def test_domain_remap_add_prefix_already_there(self):
        conf = {'default_reseller_prefix': 'AUTH'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'auth-uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, '/v1/AUTH_uuid/test')


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        domain_remap.filter_factory({})
        swift_info = utils.get_swift_info()
        self.assertTrue('domain_remap' in swift_info)
        self.assertTrue(
            swift_info['domain_remap'].get('default_reseller_prefix') is None)

    def test_registered_nondefaults(self):
        domain_remap.filter_factory({'default_reseller_prefix': 'cupcake'})
        swift_info = utils.get_swift_info()
        self.assertTrue('domain_remap' in swift_info)
        self.assertEqual(
            swift_info['domain_remap'].get('default_reseller_prefix'),
            'cupcake')


if __name__ == '__main__':
    unittest.main()
