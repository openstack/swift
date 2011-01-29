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

from swift.common.middleware import domain_remap


class FakeApp(object):

    def __call__(self, env, start_response):
        return env['PATH_INFO']


def start_response(*args):
    pass


class TestDomainRemap(unittest.TestCase):

    def setUp(self):
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), {})

    def test_domain_remap_passthrough(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'example.com:8080'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/')

    def test_domain_remap_account(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_a')
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH-uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_uuid')

    def test_domain_remap_account_container(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_a/c')

    def test_domain_remap_extra_subdomains(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'x.y.c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, ['Bad domain in host header'])

    def test_domain_remap_account_with_path_root(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_a')

    def test_domain_remap_account_container_with_path_root(self):
        req = Request.blank('/v1', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_a/c')

    def test_domain_remap_account_container_with_path(self):
        req = Request.blank('/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_a/c/obj')

    def test_domain_remap_account_container_with_path_root_and_path(self):
        req = Request.blank('/v1/obj', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/AUTH_a/c/obj')

    def test_domain_remap_account_matching_ending_not_domain(self):
        req = Request.blank('/dontchange', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.aexample.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/dontchange')

    def test_domain_remap_configured_with_empty_storage_domain(self):
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(),
                                                      {'storage_domain': ''})
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.AUTH_a.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/test')

    def test_domain_remap_configured_with_prefixes(self):
        conf = {'reseller_prefixes': 'PREFIX'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.prefix_uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/v1/PREFIX_uuid/c/test')

    def test_domain_remap_configured_with_bad_prefixes(self):
        conf = {'reseller_prefixes': 'UNKNOWN'}
        self.app = domain_remap.DomainRemapMiddleware(FakeApp(), conf)
        req = Request.blank('/test', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Host': 'c.prefix_uuid.example.com'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, '/test')


if __name__ == '__main__':
    unittest.main()
