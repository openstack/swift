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
from mock import patch
from swift.proxy.controllers.base import headers_to_container_info, \
    headers_to_account_info, get_container_info, get_container_memcache_key, \
    get_account_info, get_account_memcache_key, _get_cache_key, get_info, \
    Controller
from swift.common.swob import Request
from swift.common.utils import split_path
from test.unit import fake_http_connect, FakeRing, FakeMemcache
from swift.proxy import server as proxy_server


FakeResponse_status_int = 201


class FakeResponse(object):
    def __init__(self, headers, env, account, container):
        self.headers = headers
        self.status_int = FakeResponse_status_int
        self.environ = env
        cache_key, env_key = _get_cache_key(account, container)
        if container:
            info = headers_to_container_info(headers, FakeResponse_status_int)
        else:
            info = headers_to_account_info(headers, FakeResponse_status_int)
        env[env_key] = info


class FakeRequest(object):
    def __init__(self, env, path):
        self.environ = env
        (version, account, container, obj) = split_path(path, 2, 4, True)
        self.account = account
        self.container = container
        stype = container and 'container' or 'account'
        self.headers = {'x-%s-object-count' % (stype): 1000,
                        'x-%s-bytes-used' % (stype): 6666}

    def get_response(self, app):
        return FakeResponse(self.headers, self.environ, self.account,
                            self.container)


class FakeCache(object):
    def __init__(self, val):
        self.val = val

    def get(self, *args):
        return self.val


class TestFuncs(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
                                            account_ring=FakeRing(),
                                            container_ring=FakeRing(),
                                            object_ring=FakeRing)

    def test_GETorHEAD_base(self):
        base = Controller(self.app)
        req = Request.blank('/a/c')
        with patch('swift.proxy.controllers.base.'
                   'http_connect', fake_http_connect(200)):
            resp = base.GETorHEAD_base(req, 'container', FakeRing(), 'part',
                                       '/a/c')
        self.assertTrue('swift.container/a/c' in resp.environ)
        self.assertEqual(resp.environ['swift.container/a/c']['status'], 200)

        req = Request.blank('/a')
        with patch('swift.proxy.controllers.base.'
                   'http_connect', fake_http_connect(200)):
            resp = base.GETorHEAD_base(req, 'account', FakeRing(), 'part',
                                       '/a')
        self.assertTrue('swift.account/a' in resp.environ)
        self.assertEqual(resp.environ['swift.account/a']['status'], 200)

    def test_get_info(self):
        global FakeResponse_status_int
        # Do a non cached call to account
        env = {}
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            info_a = get_info(None, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 201)
        self.assertEquals(info_a['bytes'], 6666)
        self.assertEquals(info_a['total_object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env, {'swift.account/a': info_a})

        # Do an env cached call to account
        info_a = get_info(None, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 201)
        self.assertEquals(info_a['bytes'], 6666)
        self.assertEquals(info_a['total_object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env, {'swift.account/a': info_a})

        # This time do env cached call to account and non cached to container
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            info_c = get_info(None, env, 'a', 'c')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 201)
        self.assertEquals(info_c['bytes'], 6666)
        self.assertEquals(info_c['object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env['swift.account/a'], info_a)
        self.assertEquals(env['swift.container/a/c'], info_c)

        # This time do a non cached call to account than non cached to container
        env = {} # abandon previous call to env
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            info_c = get_info(None, env, 'a', 'c')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 201)
        self.assertEquals(info_c['bytes'], 6666)
        self.assertEquals(info_c['object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env['swift.account/a'], info_a)
        self.assertEquals(env['swift.container/a/c'], info_c)

        # This time do an env cached call to container while account is not cached
        del(env['swift.account/a'])
        info_c = get_info(None, env, 'a', 'c')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 201)
        self.assertEquals(info_c['bytes'], 6666)
        self.assertEquals(info_c['object_count'], 1000)
        # Make sure the env cache is set and account still not cached
        self.assertEquals(env, {'swift.container/a/c': info_c})

        # Do a non cached call to account not found with ret_not_found
        env = {}
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            try:
                FakeResponse_status_int = 404
                info_a = get_info(None, env, 'a', ret_not_found=True)
            finally:
                FakeResponse_status_int = 201
        # Check that you got proper info
        self.assertEquals(info_a['status'], 404)
        self.assertEquals(info_a['bytes'], 6666)
        self.assertEquals(info_a['total_object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env, {'swift.account/a': info_a})

        # Do a cached call to account not found with ret_not_found
        info_a = get_info(None, env, 'a', ret_not_found=True)
        # Check that you got proper info
        self.assertEquals(info_a['status'], 404)
        self.assertEquals(info_a['bytes'], 6666)
        self.assertEquals(info_a['total_object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env, {'swift.account/a': info_a})

        # Do a non cached call to account not found without ret_not_found
        env = {}
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            try:
                FakeResponse_status_int = 404
                info_a = get_info(None, env, 'a')
            finally:
                FakeResponse_status_int = 201
        # Check that you got proper info
        self.assertEquals(info_a, None)
        self.assertEquals(env['swift.account/a']['status'], 404)

        # Do a cached call to account not found without ret_not_found
        info_a = get_info(None, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a, None)
        self.assertEquals(env['swift.account/a']['status'], 404)

    def test_get_container_info_no_cache(self):
        req = Request.blank("/v1/AUTH_account/cont",
                            environ={'swift.cache': FakeCache({})})
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            resp = get_container_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 6666)
        self.assertEquals(resp['object_count'], 1000)

    def test_get_container_info_cache(self):
        cached = {'status': 404,
                  'bytes': 3333,
                  'object_count': 10}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            resp = get_container_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3333)
        self.assertEquals(resp['object_count'], 10)
        self.assertEquals(resp['status'], 404)

    def test_get_container_info_env(self):
        cache_key = get_container_memcache_key("account", "cont")
        env_key = 'swift.%s' % cache_key
        req = Request.blank("/v1/account/cont",
                            environ={ env_key: {'bytes': 3867},
                                      'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3867)

    def test_get_account_info_no_cache(self):
        req = Request.blank("/v1/AUTH_account",
                            environ={'swift.cache': FakeCache({})})
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            resp = get_account_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 6666)
        self.assertEquals(resp['total_object_count'], 1000)

    def test_get_account_info_cache(self):
        # The original test that we prefer to preserve
        cached = {'status': 404,
                  'bytes': 3333,
                  'total_object_count': 10}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            resp = get_account_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3333)
        self.assertEquals(resp['total_object_count'], 10)
        self.assertEquals(resp['status'], 404)

        # Here is a more realistic test
        cached = {'status': 404,
                  'bytes': '3333',
                  'container_count': '234',
                  'total_object_count': '10',
                  'meta': {}}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        with patch('swift.proxy.controllers.base.'
                   '_prepare_pre_auth_info_request', FakeRequest):
            resp = get_account_info(req.environ, 'xxx')
        self.assertEquals(resp['status'], 404)
        self.assertEquals(resp['bytes'], '3333')
        self.assertEquals(resp['container_count'], 234)
        self.assertEquals(resp['meta'], {})
        self.assertEquals(resp['total_object_count'], '10')

    def test_get_account_info_env(self):
        cache_key = get_account_memcache_key("account")
        env_key = 'swift.%s' % cache_key
        req = Request.blank("/v1/account",
                            environ={ env_key: {'bytes': 3867},
                                      'swift.cache': FakeCache({})})
        resp = get_account_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3867)

    def test_headers_to_container_info_missing(self):
        resp = headers_to_container_info({}, 404)
        self.assertEquals(resp['status'], 404)
        self.assertEquals(resp['read_acl'], None)
        self.assertEquals(resp['write_acl'], None)

    def test_headers_to_container_info_meta(self):
        headers = {'X-Container-Meta-Whatevs': 14,
                   'x-container-meta-somethingelse': 0}
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEquals(len(resp['meta']), 2)
        self.assertEquals(resp['meta']['whatevs'], 14)
        self.assertEquals(resp['meta']['somethingelse'], 0)

    def test_headers_to_container_info_values(self):
        headers = {
            'x-container-read': 'readvalue',
            'x-container-write': 'writevalue',
            'x-container-sync-key': 'keyvalue',
            'x-container-meta-access-control-allow-origin': 'here',
        }
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEquals(resp['read_acl'], 'readvalue')
        self.assertEquals(resp['write_acl'], 'writevalue')
        self.assertEquals(resp['cors']['allow_origin'], 'here')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEquals(
            resp,
            headers_to_container_info(headers.items(), 200))

    def test_headers_to_account_info_missing(self):
        resp = headers_to_account_info({}, 404)
        self.assertEquals(resp['status'], 404)
        self.assertEquals(resp['bytes'], None)
        self.assertEquals(resp['container_count'], None)

    def test_headers_to_account_info_meta(self):
        headers = {'X-Account-Meta-Whatevs': 14,
                   'x-account-meta-somethingelse': 0}
        resp = headers_to_account_info(headers.items(), 200)
        self.assertEquals(len(resp['meta']), 2)
        self.assertEquals(resp['meta']['whatevs'], 14)
        self.assertEquals(resp['meta']['somethingelse'], 0)

    def test_headers_to_account_info_values(self):
        headers = {
            'x-account-object-count': '10',
            'x-account-container-count': '20',
        }
        resp = headers_to_account_info(headers.items(), 200)
        self.assertEquals(resp['total_object_count'], '10')
        self.assertEquals(resp['container_count'], '20')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEquals(
            resp,
            headers_to_account_info(headers.items(), 200))
