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

import itertools
from collections import defaultdict
import unittest
from mock import patch
from swift.proxy.controllers.base import headers_to_container_info, \
    headers_to_account_info, headers_to_object_info, get_container_info, \
    get_container_memcache_key, get_account_info, get_account_memcache_key, \
    get_object_env_key, get_info, get_object_info, \
    Controller, GetOrHeadHandler, _set_info_cache, _set_object_info_cache, \
    bytes_to_skip
from swift.common.swob import Request, HTTPException, HeaderKeyDict, \
    RESPONSE_REASONS
from swift.common import exceptions
from swift.common.utils import split_path
from swift.common.http import is_success
from swift.common.storage_policy import StoragePolicy
from test.unit import fake_http_connect, FakeRing, FakeMemcache
from swift.proxy import server as proxy_server
from swift.common.request_helpers import get_sys_meta_prefix

from test.unit import patch_policies


class FakeResponse(object):

    base_headers = {}

    def __init__(self, status_int=200, headers=None, body=''):
        self.status_int = status_int
        self._headers = headers or {}
        self.body = body

    @property
    def headers(self):
        if is_success(self.status_int):
            self._headers.update(self.base_headers)
        return self._headers


class AccountResponse(FakeResponse):

    base_headers = {
        'x-account-container-count': 333,
        'x-account-object-count': 1000,
        'x-account-bytes-used': 6666,
    }


class ContainerResponse(FakeResponse):

    base_headers = {
        'x-container-object-count': 1000,
        'x-container-bytes-used': 6666,
    }


class ObjectResponse(FakeResponse):

    base_headers = {
        'content-length': 5555,
        'content-type': 'text/plain'
    }


class DynamicResponseFactory(object):

    def __init__(self, *statuses):
        if statuses:
            self.statuses = iter(statuses)
        else:
            self.statuses = itertools.repeat(200)
        self.stats = defaultdict(int)

    response_type = {
        'obj': ObjectResponse,
        'container': ContainerResponse,
        'account': AccountResponse,
    }

    def _get_response(self, type_):
        self.stats[type_] += 1
        class_ = self.response_type[type_]
        return class_(self.statuses.next())

    def get_response(self, environ):
        (version, account, container, obj) = split_path(
            environ['PATH_INFO'], 2, 4, True)
        if obj:
            resp = self._get_response('obj')
        elif container:
            resp = self._get_response('container')
        else:
            resp = self._get_response('account')
        resp.account = account
        resp.container = container
        resp.obj = obj
        return resp


class FakeApp(object):

    recheck_container_existence = 30
    recheck_account_existence = 30

    def __init__(self, response_factory=None, statuses=None):
        self.responses = response_factory or \
            DynamicResponseFactory(*statuses or [])
        self.sources = []

    def __call__(self, environ, start_response):
        self.sources.append(environ.get('swift.source'))
        response = self.responses.get_response(environ)
        reason = RESPONSE_REASONS[response.status_int][0]
        start_response('%d %s' % (response.status_int, reason),
                       [(k, v) for k, v in response.headers.items()])
        # It's a bit strnage, but the get_info cache stuff relies on the
        # app setting some keys in the environment as it makes requests
        # (in particular GETorHEAD_base) - so our fake does the same
        _set_info_cache(self, environ, response.account,
                        response.container, response)
        if response.obj:
            _set_object_info_cache(self, environ, response.account,
                                   response.container, response.obj,
                                   response)
        return iter(response.body)


class FakeCache(FakeMemcache):
    def __init__(self, stub=None, **pre_cached):
        super(FakeCache, self).__init__()
        if pre_cached:
            self.store.update(pre_cached)
        self.stub = stub

    def get(self, key):
        return self.stub or self.store.get(key)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestFuncs(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
                                            account_ring=FakeRing(),
                                            container_ring=FakeRing())

    def test_GETorHEAD_base(self):
        base = Controller(self.app)
        req = Request.blank('/v1/a/c/o/with/slashes')
        ring = FakeRing()
        nodes = list(ring.get_part_nodes(0)) + list(ring.get_more_nodes(0))
        with patch('swift.proxy.controllers.base.'
                   'http_connect', fake_http_connect(200)):
            resp = base.GETorHEAD_base(req, 'object', iter(nodes), 'part',
                                       '/a/c/o/with/slashes')
        self.assertTrue('swift.object/a/c/o/with/slashes' in resp.environ)
        self.assertEqual(
            resp.environ['swift.object/a/c/o/with/slashes']['status'], 200)
        req = Request.blank('/v1/a/c/o')
        with patch('swift.proxy.controllers.base.'
                   'http_connect', fake_http_connect(200)):
            resp = base.GETorHEAD_base(req, 'object', iter(nodes), 'part',
                                       '/a/c/o')
        self.assertTrue('swift.object/a/c/o' in resp.environ)
        self.assertEqual(resp.environ['swift.object/a/c/o']['status'], 200)
        req = Request.blank('/v1/a/c')
        with patch('swift.proxy.controllers.base.'
                   'http_connect', fake_http_connect(200)):
            resp = base.GETorHEAD_base(req, 'container', iter(nodes), 'part',
                                       '/a/c')
        self.assertTrue('swift.container/a/c' in resp.environ)
        self.assertEqual(resp.environ['swift.container/a/c']['status'], 200)

        req = Request.blank('/v1/a')
        with patch('swift.proxy.controllers.base.'
                   'http_connect', fake_http_connect(200)):
            resp = base.GETorHEAD_base(req, 'account', iter(nodes), 'part',
                                       '/a')
        self.assertTrue('swift.account/a' in resp.environ)
        self.assertEqual(resp.environ['swift.account/a']['status'], 200)

    def test_get_info(self):
        app = FakeApp()
        # Do a non cached call to account
        env = {}
        info_a = get_info(app, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 200)
        self.assertEquals(info_a['bytes'], 6666)
        self.assertEquals(info_a['total_object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env.get('swift.account/a'), info_a)
        # Make sure the app was called
        self.assertEqual(app.responses.stats['account'], 1)

        # Do an env cached call to account
        info_a = get_info(app, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 200)
        self.assertEquals(info_a['bytes'], 6666)
        self.assertEquals(info_a['total_object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env.get('swift.account/a'), info_a)
        # Make sure the app was NOT called AGAIN
        self.assertEqual(app.responses.stats['account'], 1)

        # This time do env cached call to account and non cached to container
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEquals(info_c['status'], 200)
        self.assertEquals(info_c['bytes'], 6666)
        self.assertEquals(info_c['object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env.get('swift.account/a'), info_a)
        self.assertEquals(env.get('swift.container/a/c'), info_c)
        # Make sure the app was called for container
        self.assertEqual(app.responses.stats['container'], 1)

        # This time do a non cached call to account than non cached to
        # container
        app = FakeApp()
        env = {}  # abandon previous call to env
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEquals(info_c['status'], 200)
        self.assertEquals(info_c['bytes'], 6666)
        self.assertEquals(info_c['object_count'], 1000)
        # Make sure the env cache is set
        self.assertEquals(env.get('swift.account/a'), info_a)
        self.assertEquals(env.get('swift.container/a/c'), info_c)
        # check app calls both account and container
        self.assertEqual(app.responses.stats['account'], 1)
        self.assertEqual(app.responses.stats['container'], 1)

        # This time do an env cached call to container while account is not
        # cached
        del(env['swift.account/a'])
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEquals(info_a['status'], 200)
        self.assertEquals(info_c['bytes'], 6666)
        self.assertEquals(info_c['object_count'], 1000)
        # Make sure the env cache is set and account still not cached
        self.assertEquals(env.get('swift.container/a/c'), info_c)
        # no additional calls were made
        self.assertEqual(app.responses.stats['account'], 1)
        self.assertEqual(app.responses.stats['container'], 1)

        # Do a non cached call to account not found with ret_not_found
        app = FakeApp(statuses=(404,))
        env = {}
        info_a = get_info(app, env, 'a', ret_not_found=True)
        # Check that you got proper info
        self.assertEquals(info_a['status'], 404)
        self.assertEquals(info_a['bytes'], None)
        self.assertEquals(info_a['total_object_count'], None)
        # Make sure the env cache is set
        self.assertEquals(env.get('swift.account/a'), info_a)
        # and account was called
        self.assertEqual(app.responses.stats['account'], 1)

        # Do a cached call to account not found with ret_not_found
        info_a = get_info(app, env, 'a', ret_not_found=True)
        # Check that you got proper info
        self.assertEquals(info_a['status'], 404)
        self.assertEquals(info_a['bytes'], None)
        self.assertEquals(info_a['total_object_count'], None)
        # Make sure the env cache is set
        self.assertEquals(env.get('swift.account/a'), info_a)
        # add account was NOT called AGAIN
        self.assertEqual(app.responses.stats['account'], 1)

        # Do a non cached call to account not found without ret_not_found
        app = FakeApp(statuses=(404,))
        env = {}
        info_a = get_info(app, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a, None)
        self.assertEquals(env['swift.account/a']['status'], 404)
        # and account was called
        self.assertEqual(app.responses.stats['account'], 1)

        # Do a cached call to account not found without ret_not_found
        info_a = get_info(None, env, 'a')
        # Check that you got proper info
        self.assertEquals(info_a, None)
        self.assertEquals(env['swift.account/a']['status'], 404)
        # add account was NOT called AGAIN
        self.assertEqual(app.responses.stats['account'], 1)

    def test_get_container_info_swift_source(self):
        app = FakeApp()
        req = Request.blank("/v1/a/c", environ={'swift.cache': FakeCache()})
        get_container_info(req.environ, app, swift_source='MC')
        self.assertEqual(app.sources, ['GET_INFO', 'MC'])

    def test_get_object_info_swift_source(self):
        app = FakeApp()
        req = Request.blank("/v1/a/c/o",
                            environ={'swift.cache': FakeCache()})
        get_object_info(req.environ, app, swift_source='LU')
        self.assertEqual(app.sources, ['LU'])

    def test_get_container_info_no_cache(self):
        req = Request.blank("/v1/AUTH_account/cont",
                            environ={'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, FakeApp())
        self.assertEquals(resp['storage_policy'], '0')
        self.assertEquals(resp['bytes'], 6666)
        self.assertEquals(resp['object_count'], 1000)

    def test_get_container_info_no_account(self):
        responses = DynamicResponseFactory(404, 200)
        app = FakeApp(responses)
        req = Request.blank("/v1/AUTH_does_not_exist/cont")
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 0)

    def test_get_container_info_no_auto_account(self):
        responses = DynamicResponseFactory(404, 200)
        app = FakeApp(responses)
        req = Request.blank("/v1/.system_account/cont")
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 200)
        self.assertEquals(info['bytes'], 6666)
        self.assertEquals(info['object_count'], 1000)

    def test_get_container_info_cache(self):
        cache_stub = {
            'status': 404, 'bytes': 3333, 'object_count': 10,
            # simplejson sometimes hands back strings, sometimes unicodes
            'versions': u"\u1F4A9"}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cache_stub)})
        resp = get_container_info(req.environ, FakeApp())
        self.assertEquals(resp['storage_policy'], '0')
        self.assertEquals(resp['bytes'], 3333)
        self.assertEquals(resp['object_count'], 10)
        self.assertEquals(resp['status'], 404)
        self.assertEquals(resp['versions'], "\xe1\xbd\x8a\x39")

    def test_get_container_info_env(self):
        cache_key = get_container_memcache_key("account", "cont")
        env_key = 'swift.%s' % cache_key
        req = Request.blank("/v1/account/cont",
                            environ={env_key: {'bytes': 3867},
                                     'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3867)

    def test_get_account_info_swift_source(self):
        app = FakeApp()
        req = Request.blank("/v1/a", environ={'swift.cache': FakeCache()})
        get_account_info(req.environ, app, swift_source='MC')
        self.assertEqual(app.sources, ['MC'])

    def test_get_account_info_no_cache(self):
        app = FakeApp()
        req = Request.blank("/v1/AUTH_account",
                            environ={'swift.cache': FakeCache({})})
        resp = get_account_info(req.environ, app)
        self.assertEquals(resp['bytes'], 6666)
        self.assertEquals(resp['total_object_count'], 1000)

    def test_get_account_info_cache(self):
        # The original test that we prefer to preserve
        cached = {'status': 404,
                  'bytes': 3333,
                  'total_object_count': 10}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        resp = get_account_info(req.environ, FakeApp())
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
        resp = get_account_info(req.environ, FakeApp())
        self.assertEquals(resp['status'], 404)
        self.assertEquals(resp['bytes'], '3333')
        self.assertEquals(resp['container_count'], 234)
        self.assertEquals(resp['meta'], {})
        self.assertEquals(resp['total_object_count'], '10')

    def test_get_account_info_env(self):
        cache_key = get_account_memcache_key("account")
        env_key = 'swift.%s' % cache_key
        req = Request.blank("/v1/account",
                            environ={env_key: {'bytes': 3867},
                                     'swift.cache': FakeCache({})})
        resp = get_account_info(req.environ, 'xxx')
        self.assertEquals(resp['bytes'], 3867)

    def test_get_object_info_env(self):
        cached = {'status': 200,
                  'length': 3333,
                  'type': 'application/json',
                  'meta': {}}
        env_key = get_object_env_key("account", "cont", "obj")
        req = Request.blank("/v1/account/cont/obj",
                            environ={env_key: cached,
                                     'swift.cache': FakeCache({})})
        resp = get_object_info(req.environ, 'xxx')
        self.assertEquals(resp['length'], 3333)
        self.assertEquals(resp['type'], 'application/json')

    def test_get_object_info_no_env(self):
        app = FakeApp()
        req = Request.blank("/v1/account/cont/obj",
                            environ={'swift.cache': FakeCache({})})
        resp = get_object_info(req.environ, app)
        self.assertEqual(app.responses.stats['account'], 0)
        self.assertEqual(app.responses.stats['container'], 0)
        self.assertEqual(app.responses.stats['obj'], 1)
        self.assertEquals(resp['length'], 5555)
        self.assertEquals(resp['type'], 'text/plain')

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

    def test_headers_to_container_info_sys_meta(self):
        prefix = get_sys_meta_prefix('container')
        headers = {'%sWhatevs' % prefix: 14,
                   '%ssomethingelse' % prefix: 0}
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEquals(len(resp['sysmeta']), 2)
        self.assertEquals(resp['sysmeta']['whatevs'], 14)
        self.assertEquals(resp['sysmeta']['somethingelse'], 0)

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

    def test_headers_to_account_info_sys_meta(self):
        prefix = get_sys_meta_prefix('account')
        headers = {'%sWhatevs' % prefix: 14,
                   '%ssomethingelse' % prefix: 0}
        resp = headers_to_account_info(headers.items(), 200)
        self.assertEquals(len(resp['sysmeta']), 2)
        self.assertEquals(resp['sysmeta']['whatevs'], 14)
        self.assertEquals(resp['sysmeta']['somethingelse'], 0)

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

    def test_headers_to_object_info_missing(self):
        resp = headers_to_object_info({}, 404)
        self.assertEquals(resp['status'], 404)
        self.assertEquals(resp['length'], None)
        self.assertEquals(resp['etag'], None)

    def test_headers_to_object_info_meta(self):
        headers = {'X-Object-Meta-Whatevs': 14,
                   'x-object-meta-somethingelse': 0}
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEquals(len(resp['meta']), 2)
        self.assertEquals(resp['meta']['whatevs'], 14)
        self.assertEquals(resp['meta']['somethingelse'], 0)

    def test_headers_to_object_info_sys_meta(self):
        prefix = get_sys_meta_prefix('object')
        headers = {'%sWhatevs' % prefix: 14,
                   '%ssomethingelse' % prefix: 0}
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEquals(len(resp['sysmeta']), 2)
        self.assertEquals(resp['sysmeta']['whatevs'], 14)
        self.assertEquals(resp['sysmeta']['somethingelse'], 0)

    def test_headers_to_object_info_values(self):
        headers = {
            'content-length': '1024',
            'content-type': 'application/json',
        }
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEquals(resp['length'], '1024')
        self.assertEquals(resp['type'], 'application/json')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEquals(
            resp,
            headers_to_object_info(headers.items(), 200))

    def test_base_have_quorum(self):
        base = Controller(self.app)
        # just throw a bunch of test cases at it
        self.assertEqual(base.have_quorum([201, 404], 3), False)
        self.assertEqual(base.have_quorum([201, 201], 4), False)
        self.assertEqual(base.have_quorum([201, 201, 404, 404], 4), False)
        self.assertEqual(base.have_quorum([201, 503, 503, 201], 4), False)
        self.assertEqual(base.have_quorum([201, 201], 3), True)
        self.assertEqual(base.have_quorum([404, 404], 3), True)
        self.assertEqual(base.have_quorum([201, 201], 2), True)
        self.assertEqual(base.have_quorum([404, 404], 2), True)
        self.assertEqual(base.have_quorum([201, 404, 201, 201], 4), True)

    def test_best_response_overrides(self):
        base = Controller(self.app)
        responses = [
            (302, 'Found', '', 'The resource has moved temporarily.'),
            (100, 'Continue', '', ''),
            (404, 'Not Found', '', 'Custom body'),
        ]
        server_type = "Base DELETE"
        req = Request.blank('/v1/a/c/o', method='DELETE')
        statuses, reasons, headers, bodies = zip(*responses)

        # First test that you can't make a quorum with only overridden
        # responses
        overrides = {302: 204, 100: 204}
        resp = base.best_response(req, statuses, reasons, bodies, server_type,
                                  headers=headers, overrides=overrides)
        self.assertEqual(resp.status, '503 Service Unavailable')

        # next make a 404 quorum and make sure the last delete (real) 404
        # status is the one returned.
        overrides = {100: 404}
        resp = base.best_response(req, statuses, reasons, bodies, server_type,
                                  headers=headers, overrides=overrides)
        self.assertEqual(resp.status, '404 Not Found')
        self.assertEqual(resp.body, 'Custom body')

    def test_range_fast_forward(self):
        req = Request.blank('/')
        handler = GetOrHeadHandler(None, req, None, None, None, None, {})
        handler.fast_forward(50)
        self.assertEquals(handler.backend_headers['Range'], 'bytes=50-')

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=23-50'})
        handler.fast_forward(20)
        self.assertEquals(handler.backend_headers['Range'], 'bytes=43-50')
        self.assertRaises(HTTPException,
                          handler.fast_forward, 80)

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=23-'})
        handler.fast_forward(20)
        self.assertEquals(handler.backend_headers['Range'], 'bytes=43-')

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=-100'})
        handler.fast_forward(20)
        self.assertEquals(handler.backend_headers['Range'], 'bytes=-80')

    def test_transfer_headers_with_sysmeta(self):
        base = Controller(self.app)
        good_hdrs = {'x-base-sysmeta-foo': 'ok',
                     'X-Base-sysmeta-Bar': 'also ok'}
        bad_hdrs = {'x-base-sysmeta-': 'too short'}
        hdrs = dict(good_hdrs)
        hdrs.update(bad_hdrs)
        dst_hdrs = HeaderKeyDict()
        base.transfer_headers(hdrs, dst_hdrs)
        self.assertEqual(HeaderKeyDict(good_hdrs), dst_hdrs)

    def test_generate_request_headers(self):
        base = Controller(self.app)
        src_headers = {'x-remove-base-meta-owner': 'x',
                       'x-base-meta-size': '151M',
                       'new-owner': 'Kun'}
        req = Request.blank('/v1/a/c/o', headers=src_headers)
        dst_headers = base.generate_request_headers(req, transfer=True)
        expected_headers = {'x-base-meta-owner': '',
                            'x-base-meta-size': '151M',
                            'connection': 'close'}
        for k, v in expected_headers.iteritems():
            self.assertTrue(k in dst_headers)
            self.assertEqual(v, dst_headers[k])
        self.assertFalse('new-owner' in dst_headers)

    def test_generate_request_headers_with_sysmeta(self):
        base = Controller(self.app)
        good_hdrs = {'x-base-sysmeta-foo': 'ok',
                     'X-Base-sysmeta-Bar': 'also ok'}
        bad_hdrs = {'x-base-sysmeta-': 'too short'}
        hdrs = dict(good_hdrs)
        hdrs.update(bad_hdrs)
        req = Request.blank('/v1/a/c/o', headers=hdrs)
        dst_headers = base.generate_request_headers(req, transfer=True)
        for k, v in good_hdrs.iteritems():
            self.assertTrue(k.lower() in dst_headers)
            self.assertEqual(v, dst_headers[k.lower()])
        for k, v in bad_hdrs.iteritems():
            self.assertFalse(k.lower() in dst_headers)

    def test_client_chunk_size(self):

        class TestSource(object):
            def __init__(self, chunks):
                self.chunks = list(chunks)

            def read(self, _read_size):
                if self.chunks:
                    return self.chunks.pop(0)
                else:
                    return ''

        source = TestSource((
            'abcd', '1234', 'abc', 'd1', '234abcd1234abcd1', '2'))
        req = Request.blank('/v1/a/c/o')
        node = {}
        handler = GetOrHeadHandler(self.app, req, None, None, None, None, {},
                                   client_chunk_size=8)

        app_iter = handler._make_app_iter(req, node, source)
        client_chunks = list(app_iter)
        self.assertEqual(client_chunks, [
            'abcd1234', 'abcd1234', 'abcd1234', 'abcd12'])

    def test_client_chunk_size_resuming(self):

        class TestSource(object):
            def __init__(self, chunks):
                self.chunks = list(chunks)

            def read(self, _read_size):
                if self.chunks:
                    chunk = self.chunks.pop(0)
                    if chunk is None:
                        raise exceptions.ChunkReadTimeout()
                    else:
                        return chunk
                else:
                    return ''

        node = {'ip': '1.2.3.4', 'port': 6000, 'device': 'sda'}

        source1 = TestSource(['abcd', '1234', 'abc', None])
        source2 = TestSource(['efgh5678'])
        req = Request.blank('/v1/a/c/o')
        handler = GetOrHeadHandler(
            self.app, req, 'Object', None, None, None, {},
            client_chunk_size=8)

        app_iter = handler._make_app_iter(req, node, source1)
        with patch.object(handler, '_get_source_and_node',
                          lambda: (source2, node)):
            client_chunks = list(app_iter)
        self.assertEqual(client_chunks, ['abcd1234', 'efgh5678'])
        self.assertEqual(handler.backend_headers['Range'], 'bytes=8-')

    def test_bytes_to_skip(self):
        # if you start at the beginning, skip nothing
        self.assertEqual(bytes_to_skip(1024, 0), 0)

        # missed the first 10 bytes, so we've got 1014 bytes of partial
        # record
        self.assertEqual(bytes_to_skip(1024, 10), 1014)

        # skipped some whole records first
        self.assertEqual(bytes_to_skip(1024, 4106), 1014)

        # landed on a record boundary
        self.assertEqual(bytes_to_skip(1024, 1024), 0)
        self.assertEqual(bytes_to_skip(1024, 2048), 0)

        # big numbers
        self.assertEqual(bytes_to_skip(2 ** 20, 2 ** 32), 0)
        self.assertEqual(bytes_to_skip(2 ** 20, 2 ** 32 + 1), 2 ** 20 - 1)
        self.assertEqual(bytes_to_skip(2 ** 20, 2 ** 32 + 2 ** 19), 2 ** 19)

        # odd numbers
        self.assertEqual(bytes_to_skip(123, 0), 0)
        self.assertEqual(bytes_to_skip(123, 23), 100)
        self.assertEqual(bytes_to_skip(123, 247), 122)

        # prime numbers
        self.assertEqual(bytes_to_skip(11, 7), 4)
        self.assertEqual(bytes_to_skip(97, 7873823), 55)
