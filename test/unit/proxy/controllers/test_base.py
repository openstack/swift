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

import swift.proxy.controllers.base
from swift.proxy.controllers.base import headers_to_container_info, \
    headers_to_account_info, get_container_info, get_container_memcache_key, \
    get_account_info, get_account_memcache_key
from swift.common.swob import Request
from swift.common.utils import split_path


class FakeResponse(object):
    def __init__(self, headers):
        self.headers = headers
        self.status_int = 201


class FakeRequest(object):
    def __init__(self, env, method, path, swift_source=None):
        (version, account,
         container, obj) = split_path(env['PATH_INFO'], 2, 4, True)
        stype = container and 'container' or 'account'
        self.headers = {'x-%s-object-count' % (stype): 1000,
                        'x-%s-bytes-used' % (stype): 6666}

    def get_response(self, app):
        return FakeResponse(self.headers)


class FakeCache(object):
    def __init__(self, val):
        self.val = val

    def get(self, *args):
        return self.val


class TestFuncs(unittest.TestCase):
    def test_get_container_info_no_cache(self):
        swift.proxy.controllers.base.make_pre_authed_request = FakeRequest
        req = Request.blank("/v1/AUTH_account/cont",
                            environ={'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 6666)
        self.assertEquals(resp['object_count'], 1000)

    def test_get_container_info_cache(self):
        swift.proxy.controllers.base.make_pre_authed_request = FakeRequest
        cached = {'status': 404,
                  'bytes': 3333,
                  'object_count': 10}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
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
        swift.proxy.controllers.base.make_pre_authed_request = FakeRequest
        req = Request.blank("/v1/AUTH_account",
                            environ={'swift.cache': FakeCache({})})
        resp = get_account_info(req.environ, 'xxx')
        print resp
        self.assertEquals(resp['bytes'], 6666)
        self.assertEquals(resp['total_object_count'], 1000)

    def test_get_account_info_cache(self):
        swift.proxy.controllers.base.make_pre_authed_request = FakeRequest
        cached = {'status': 404,
                  'bytes': 3333,
                  'total_object_count': 10}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        resp = get_account_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3333)
        self.assertEquals(resp['total_object_count'], 10)
        self.assertEquals(resp['status'], 404)

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
