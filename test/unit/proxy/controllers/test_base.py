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
import json
from collections import defaultdict
import unittest
import mock

import six

from swift.proxy.controllers.base import headers_to_container_info, \
    headers_to_account_info, headers_to_object_info, get_container_info, \
    get_cache_key, get_account_info, get_info, get_object_info, \
    Controller, GetOrHeadHandler, bytes_to_skip
from swift.common.swob import Request, HTTPException, RESPONSE_REASONS
from swift.common import exceptions
from swift.common.utils import split_path, ShardRange, Timestamp
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import is_success
from swift.common.storage_policy import StoragePolicy, StoragePolicyCollection
from test.unit import (
    fake_http_connect, FakeRing, FakeMemcache, PatchPolicies, FakeLogger,
    make_timestamp_iter,
    mocked_http_conn)
from swift.proxy import server as proxy_server
from swift.common.request_helpers import (
    get_sys_meta_prefix, get_object_transient_sysmeta
)

from test.unit import patch_policies


class FakeResponse(object):

    base_headers = {}

    def __init__(self, status_int=200, headers=None, body=b''):
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
        return class_(next(self.statuses))

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


class ZeroCacheAccountResponse(FakeResponse):
    base_headers = {
        'X-Backend-Recheck-Account-Existence': '0',
        'x-account-container-count': 333,
        'x-account-object-count': 1000,
        'x-account-bytes-used': 6666,
    }


class ZeroCacheContainerResponse(FakeResponse):
    base_headers = {
        'X-Backend-Recheck-Container-Existence': '0',
        'x-container-object-count': 1000,
        'x-container-bytes-used': 6666,
    }


class ZeroCacheDynamicResponseFactory(DynamicResponseFactory):
    response_type = {
        'obj': ObjectResponse,
        'container': ZeroCacheContainerResponse,
        'account': ZeroCacheAccountResponse,
    }


class FakeApp(object):

    recheck_container_existence = 30
    recheck_account_existence = 30

    def __init__(self, response_factory=None, statuses=None):
        self.responses = response_factory or \
            DynamicResponseFactory(*statuses or [])
        self.captured_envs = []

    def __call__(self, environ, start_response):
        self.captured_envs.append(environ)
        response = self.responses.get_response(environ)
        reason = RESPONSE_REASONS[response.status_int][0]
        start_response('%d %s' % (response.status_int, reason),
                       [(k, v) for k, v in response.headers.items()])
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
                                            container_ring=FakeRing(),
                                            logger=FakeLogger())

    def test_get_info_zero_recheck(self):
        mock_cache = mock.Mock()
        mock_cache.get.return_value = None
        app = FakeApp(ZeroCacheDynamicResponseFactory())
        env = {'swift.cache': mock_cache}
        info_a = get_info(app, env, 'a')
        # Check that you got proper info
        self.assertEqual(info_a['status'], 200)
        self.assertEqual(info_a['bytes'], 6666)
        self.assertEqual(info_a['total_object_count'], 1000)
        self.assertEqual(info_a['container_count'], 333)
        # Make sure the env cache is set
        exp_cached_info_a = {
            k: str(v) if k in (
                'bytes', 'container_count', 'total_object_count') else v
            for k, v in info_a.items()}
        self.assertEqual(env['swift.infocache'].get('account/a'),
                         exp_cached_info_a)
        # Make sure the app was called
        self.assertEqual(app.responses.stats['account'], 1)
        self.assertEqual(app.responses.stats['container'], 0)
        # Make sure memcache was called
        self.assertEqual(mock_cache.mock_calls, [
            mock.call.get('account/a'),
            mock.call.set('account/a', exp_cached_info_a, time=0),
        ])

        mock_cache.reset_mock()
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEqual(info_c['status'], 200)
        self.assertEqual(info_c['bytes'], 6666)
        self.assertEqual(info_c['object_count'], 1000)
        # Make sure the env cache is set
        exp_cached_info_c = {
            k: str(v) if k in (
                'bytes', 'object_count', 'storage_policy') else v
            for k, v in info_c.items()}
        self.assertEqual(env['swift.infocache'].get('account/a'),
                         exp_cached_info_a)
        self.assertEqual(env['swift.infocache'].get('container/a/c'),
                         exp_cached_info_c)
        # Check app call for container, but no new calls for account
        self.assertEqual(app.responses.stats['account'], 1)
        self.assertEqual(app.responses.stats['container'], 1)
        # Make sure container info was cached
        self.assertEqual(mock_cache.mock_calls, [
            mock.call.get('container/a/c'),
            mock.call.set('container/a/c', exp_cached_info_c, time=0),
        ])

        # reset call counts
        app = FakeApp(ZeroCacheDynamicResponseFactory())
        env = {'swift.cache': mock_cache}
        mock_cache.reset_mock()
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEqual(info_c['status'], 200)
        self.assertEqual(info_c['bytes'], 6666)
        self.assertEqual(info_c['object_count'], 1000)
        # Make sure the env cache is set
        self.assertEqual(env['swift.infocache'].get('account/a'),
                         exp_cached_info_a)
        self.assertEqual(env['swift.infocache'].get('container/a/c'),
                         exp_cached_info_c)
        # check app calls both account and container
        self.assertEqual(app.responses.stats['account'], 1)
        self.assertEqual(app.responses.stats['container'], 1)
        # Make sure account info was cached but container was not
        self.assertEqual(mock_cache.mock_calls, [
            mock.call.get('container/a/c'),
            mock.call.get('account/a'),
            mock.call.set('account/a', exp_cached_info_a, time=0),
            mock.call.set('container/a/c', exp_cached_info_c, time=0),
        ])

    def test_get_info(self):
        app = FakeApp()
        # Do a non cached call to account
        env = {}
        info_a = get_info(app, env, 'a')
        # Check that you got proper info
        self.assertEqual(info_a['status'], 200)
        self.assertEqual(info_a['bytes'], 6666)
        self.assertEqual(info_a['total_object_count'], 1000)

        # Make sure the app was called
        self.assertEqual(app.responses.stats['account'], 1)

        # Make sure the return value matches get_account_info
        account_info = get_account_info({'PATH_INFO': '/v1/a'}, app)
        self.assertEqual(info_a, account_info)

        # Do an env cached call to account
        app.responses.stats['account'] = 0
        app.responses.stats['container'] = 0

        info_a = get_info(app, env, 'a')
        # Check that you got proper info
        self.assertEqual(info_a['status'], 200)
        self.assertEqual(info_a['bytes'], 6666)
        self.assertEqual(info_a['total_object_count'], 1000)

        # Make sure the app was NOT called AGAIN
        self.assertEqual(app.responses.stats['account'], 0)

        # This time do env cached call to account and non cached to container
        app.responses.stats['account'] = 0
        app.responses.stats['container'] = 0

        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEqual(info_c['status'], 200)
        self.assertEqual(info_c['bytes'], 6666)
        self.assertEqual(info_c['object_count'], 1000)
        # Make sure the app was called for container but not account
        self.assertEqual(app.responses.stats['account'], 0)
        self.assertEqual(app.responses.stats['container'], 1)

        # This time do a non-cached call to account then non-cached to
        # container
        app.responses.stats['account'] = 0
        app.responses.stats['container'] = 0
        app = FakeApp()
        env = {}  # abandon previous call to env
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEqual(info_c['status'], 200)
        self.assertEqual(info_c['bytes'], 6666)
        self.assertEqual(info_c['object_count'], 1000)
        # check app calls both account and container
        self.assertEqual(app.responses.stats['account'], 1)
        self.assertEqual(app.responses.stats['container'], 1)

        # This time do an env-cached call to container while account is not
        # cached
        app.responses.stats['account'] = 0
        app.responses.stats['container'] = 0
        info_c = get_info(app, env, 'a', 'c')
        # Check that you got proper info
        self.assertEqual(info_a['status'], 200)
        self.assertEqual(info_c['bytes'], 6666)
        self.assertEqual(info_c['object_count'], 1000)

        # no additional calls were made
        self.assertEqual(app.responses.stats['account'], 0)
        self.assertEqual(app.responses.stats['container'], 0)

    def test_get_container_info_swift_source(self):
        app = FakeApp()
        req = Request.blank("/v1/a/c", environ={'swift.cache': FakeCache()})
        get_container_info(req.environ, app, swift_source='MC')
        self.assertEqual([e['swift.source'] for e in app.captured_envs],
                         ['MC', 'MC'])

    def test_get_object_info_swift_source(self):
        app = FakeApp()
        req = Request.blank("/v1/a/c/o",
                            environ={'swift.cache': FakeCache()})
        get_object_info(req.environ, app, swift_source='LU')
        self.assertEqual([e['swift.source'] for e in app.captured_envs],
                         ['LU'])

    def test_get_container_info_no_cache(self):
        req = Request.blank("/v1/AUTH_account/cont",
                            environ={'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, FakeApp())
        self.assertEqual(resp['storage_policy'], 0)
        self.assertEqual(resp['bytes'], 6666)
        self.assertEqual(resp['object_count'], 1000)

    def test_get_container_info_no_account(self):
        app = FakeApp(statuses=[404, 200])
        req = Request.blank("/v1/AUTH_does_not_exist/cont")
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 0)

    def test_get_container_info_no_auto_account(self):
        app = FakeApp(statuses=[200])
        req = Request.blank("/v1/.system_account/cont")
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 200)
        self.assertEqual(info['bytes'], 6666)
        self.assertEqual(info['object_count'], 1000)

    def test_get_container_info_cache(self):
        cache_stub = {
            'status': 404, 'bytes': 3333, 'object_count': 10,
            'versions': u"\u1F4A9"}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cache_stub)})
        resp = get_container_info(req.environ, FakeApp())
        self.assertEqual(resp['storage_policy'], 0)
        self.assertEqual(resp['bytes'], 3333)
        self.assertEqual(resp['object_count'], 10)
        self.assertEqual(resp['status'], 404)
        if six.PY3:
            self.assertEqual(resp['versions'], u'\u1f4a9')
        else:
            self.assertEqual(resp['versions'], "\xe1\xbd\x8a\x39")

    def test_get_container_info_env(self):
        cache_key = get_cache_key("account", "cont")
        req = Request.blank(
            "/v1/account/cont",
            environ={'swift.infocache': {cache_key: {'bytes': 3867}},
                     'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, 'xxx')
        self.assertEqual(resp['bytes'], 3867)

    def test_get_account_info_swift_source(self):
        app = FakeApp()
        req = Request.blank("/v1/a", environ={'swift.cache': FakeCache()})
        get_account_info(req.environ, app, swift_source='MC')
        self.assertEqual([e['swift.source'] for e in app.captured_envs],
                         ['MC'])

    def test_get_account_info_swift_owner(self):
        app = FakeApp()
        req = Request.blank("/v1/a", environ={'swift.cache': FakeCache()})
        get_account_info(req.environ, app)
        self.assertEqual([e['swift_owner'] for e in app.captured_envs],
                         [True])

    def test_get_account_info_infocache(self):
        app = FakeApp()
        ic = {}
        req = Request.blank("/v1/a", environ={'swift.cache': FakeCache(),
                                              'swift.infocache': ic})
        get_account_info(req.environ, app)
        got_infocaches = [e['swift.infocache'] for e in app.captured_envs]
        self.assertEqual(1, len(got_infocaches))
        self.assertIs(ic, got_infocaches[0])

    def test_get_account_info_no_cache(self):
        app = FakeApp()
        req = Request.blank("/v1/AUTH_account",
                            environ={'swift.cache': FakeCache({})})
        resp = get_account_info(req.environ, app)
        self.assertEqual(resp['bytes'], 6666)
        self.assertEqual(resp['total_object_count'], 1000)

    def test_get_account_info_cache(self):
        # Works with fake apps that return ints in the headers
        cached = {'status': 404,
                  'bytes': 3333,
                  'total_object_count': 10}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        resp = get_account_info(req.environ, FakeApp())
        self.assertEqual(resp['bytes'], 3333)
        self.assertEqual(resp['total_object_count'], 10)
        self.assertEqual(resp['status'], 404)

        # Works with strings too, like you get when parsing HTTP headers
        # that came in through a socket from the account server
        cached = {'status': 404,
                  'bytes': '3333',
                  'container_count': '234',
                  'total_object_count': '10',
                  'meta': {}}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cached)})
        resp = get_account_info(req.environ, FakeApp())
        self.assertEqual(resp['status'], 404)
        self.assertEqual(resp['bytes'], 3333)
        self.assertEqual(resp['container_count'], 234)
        self.assertEqual(resp['meta'], {})
        self.assertEqual(resp['total_object_count'], 10)

    def test_get_account_info_env(self):
        cache_key = get_cache_key("account")
        req = Request.blank(
            "/v1/account",
            environ={'swift.infocache': {cache_key: {'bytes': 3867}},
                     'swift.cache': FakeCache({})})
        resp = get_account_info(req.environ, 'xxx')
        self.assertEqual(resp['bytes'], 3867)

    def test_get_object_info_env(self):
        cached = {'status': 200,
                  'length': 3333,
                  'type': 'application/json',
                  'meta': {}}
        cache_key = get_cache_key("account", "cont", "obj")
        req = Request.blank(
            "/v1/account/cont/obj",
            environ={'swift.infocache': {cache_key: cached},
                     'swift.cache': FakeCache({})})
        resp = get_object_info(req.environ, 'xxx')
        self.assertEqual(resp['length'], 3333)
        self.assertEqual(resp['type'], 'application/json')

    def test_get_object_info_no_env(self):
        app = FakeApp()
        req = Request.blank("/v1/account/cont/obj",
                            environ={'swift.cache': FakeCache({})})
        resp = get_object_info(req.environ, app)
        self.assertEqual(app.responses.stats['account'], 0)
        self.assertEqual(app.responses.stats['container'], 0)
        self.assertEqual(app.responses.stats['obj'], 1)
        self.assertEqual(resp['length'], 5555)
        self.assertEqual(resp['type'], 'text/plain')

    def test_options(self):
        base = Controller(self.app)
        base.account_name = 'a'
        base.container_name = 'c'
        origin = 'http://m.com'
        self.app.cors_allow_origin = [origin]
        req = Request.blank('/v1/a/c/o',
                            environ={'swift.cache': FakeCache()},
                            headers={'Origin': origin,
                                     'Access-Control-Request-Method': 'GET'})

        with mock.patch('swift.proxy.controllers.base.'
                        'http_connect', fake_http_connect(200)):
            resp = base.OPTIONS(req)
        self.assertEqual(resp.status_int, 200)

    def test_options_with_null_allow_origin(self):
        base = Controller(self.app)
        base.account_name = 'a'
        base.container_name = 'c'

        def my_container_info(*args):
            return {
                'cors': {
                    'allow_origin': '*',
                }
            }
        base.container_info = my_container_info
        req = Request.blank('/v1/a/c/o',
                            environ={'swift.cache': FakeCache()},
                            headers={'Origin': '*',
                                     'Access-Control-Request-Method': 'GET'})

        with mock.patch('swift.proxy.controllers.base.'
                        'http_connect', fake_http_connect(200)):
            resp = base.OPTIONS(req)
        self.assertEqual(resp.status_int, 200)

    def test_options_unauthorized(self):
        base = Controller(self.app)
        base.account_name = 'a'
        base.container_name = 'c'
        self.app.cors_allow_origin = ['http://NOT_IT']
        req = Request.blank('/v1/a/c/o',
                            environ={'swift.cache': FakeCache()},
                            headers={'Origin': 'http://m.com',
                                     'Access-Control-Request-Method': 'GET'})

        with mock.patch('swift.proxy.controllers.base.'
                        'http_connect', fake_http_connect(200)):
            resp = base.OPTIONS(req)
        self.assertEqual(resp.status_int, 401)

    def test_headers_to_container_info_missing(self):
        resp = headers_to_container_info({}, 404)
        self.assertEqual(resp['status'], 404)
        self.assertIsNone(resp['read_acl'])
        self.assertIsNone(resp['write_acl'])

    def test_headers_to_container_info_meta(self):
        headers = {'X-Container-Meta-Whatevs': 14,
                   'x-container-meta-somethingelse': 0}
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEqual(len(resp['meta']), 2)
        self.assertEqual(resp['meta']['whatevs'], 14)
        self.assertEqual(resp['meta']['somethingelse'], 0)

    def test_headers_to_container_info_sys_meta(self):
        prefix = get_sys_meta_prefix('container')
        headers = {'%sWhatevs' % prefix: 14,
                   '%ssomethingelse' % prefix: 0}
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEqual(len(resp['sysmeta']), 2)
        self.assertEqual(resp['sysmeta']['whatevs'], 14)
        self.assertEqual(resp['sysmeta']['somethingelse'], 0)

    def test_headers_to_container_info_values(self):
        headers = {
            'x-container-read': 'readvalue',
            'x-container-write': 'writevalue',
            'x-container-sync-key': 'keyvalue',
            'x-container-meta-access-control-allow-origin': 'here',
        }
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEqual(resp['read_acl'], 'readvalue')
        self.assertEqual(resp['write_acl'], 'writevalue')
        self.assertEqual(resp['cors']['allow_origin'], 'here')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEqual(
            resp,
            headers_to_container_info(headers.items(), 200))

    def test_container_info_without_req(self):
        base = Controller(self.app)
        base.account_name = 'a'
        base.container_name = 'c'

        container_info = \
            base.container_info(base.account_name,
                                base.container_name)
        self.assertEqual(container_info['status'], 0)

    def test_headers_to_account_info_missing(self):
        resp = headers_to_account_info({}, 404)
        self.assertEqual(resp['status'], 404)
        self.assertIsNone(resp['bytes'])
        self.assertIsNone(resp['container_count'])

    def test_headers_to_account_info_meta(self):
        headers = {'X-Account-Meta-Whatevs': 14,
                   'x-account-meta-somethingelse': 0}
        resp = headers_to_account_info(headers.items(), 200)
        self.assertEqual(len(resp['meta']), 2)
        self.assertEqual(resp['meta']['whatevs'], 14)
        self.assertEqual(resp['meta']['somethingelse'], 0)

    def test_headers_to_account_info_sys_meta(self):
        prefix = get_sys_meta_prefix('account')
        headers = {'%sWhatevs' % prefix: 14,
                   '%ssomethingelse' % prefix: 0}
        resp = headers_to_account_info(headers.items(), 200)
        self.assertEqual(len(resp['sysmeta']), 2)
        self.assertEqual(resp['sysmeta']['whatevs'], 14)
        self.assertEqual(resp['sysmeta']['somethingelse'], 0)

    def test_headers_to_account_info_values(self):
        headers = {
            'x-account-object-count': '10',
            'x-account-container-count': '20',
        }
        resp = headers_to_account_info(headers.items(), 200)
        self.assertEqual(resp['total_object_count'], '10')
        self.assertEqual(resp['container_count'], '20')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEqual(
            resp,
            headers_to_account_info(headers.items(), 200))

    def test_headers_to_account_info_storage_policies(self):
        headers = {
            'x-account-storage-policy-zero-object-count': '13',
            'x-account-storage-policy-zero-container-count': '120',
            'x-account-storage-policy-zero-bytes-used': '1002',
            'x-account-storage-policy-one-object-count': '10',
            'x-account-storage-policy-one-container-count': '20',
        }
        spc = StoragePolicyCollection([StoragePolicy(0, 'zero', True),
                                       StoragePolicy(1, 'one', False)])
        with PatchPolicies(spc):
            resp = headers_to_account_info(headers.items(), 200)
        self.assertEqual(resp['storage_policies'], {
            0: {'object_count': 13,
                'container_count': 120,
                'bytes': 1002},
            1: {'object_count': 10,
                'container_count': 20,
                'bytes': 0},
        })

    def test_headers_to_object_info_missing(self):
        resp = headers_to_object_info({}, 404)
        self.assertEqual(resp['status'], 404)
        self.assertIsNone(resp['length'])
        self.assertIsNone(resp['etag'])

    def test_headers_to_object_info_meta(self):
        headers = {'X-Object-Meta-Whatevs': 14,
                   'x-object-meta-somethingelse': 0}
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEqual(len(resp['meta']), 2)
        self.assertEqual(resp['meta']['whatevs'], 14)
        self.assertEqual(resp['meta']['somethingelse'], 0)

    def test_headers_to_object_info_sys_meta(self):
        prefix = get_sys_meta_prefix('object')
        headers = {'%sWhatevs' % prefix: 14,
                   '%ssomethingelse' % prefix: 0}
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEqual(len(resp['sysmeta']), 2)
        self.assertEqual(resp['sysmeta']['whatevs'], 14)
        self.assertEqual(resp['sysmeta']['somethingelse'], 0)

    def test_headers_to_object_info_transient_sysmeta(self):
        headers = {get_object_transient_sysmeta('Whatevs'): 14,
                   get_object_transient_sysmeta('somethingelse'): 0}
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEqual(len(resp['transient_sysmeta']), 2)
        self.assertEqual(resp['transient_sysmeta']['whatevs'], 14)
        self.assertEqual(resp['transient_sysmeta']['somethingelse'], 0)

    def test_headers_to_object_info_values(self):
        headers = {
            'content-length': '1024',
            'content-type': 'application/json',
        }
        resp = headers_to_object_info(headers.items(), 200)
        self.assertEqual(resp['length'], '1024')
        self.assertEqual(resp['type'], 'application/json')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEqual(
            resp,
            headers_to_object_info(headers.items(), 200))

    def test_base_have_quorum(self):
        base = Controller(self.app)
        # just throw a bunch of test cases at it
        self.assertFalse(base.have_quorum([201, 404], 3))
        self.assertTrue(base.have_quorum([201, 201], 4))
        self.assertFalse(base.have_quorum([201], 4))
        self.assertTrue(base.have_quorum([201, 201, 404, 404], 4))
        self.assertFalse(base.have_quorum([201, 302, 418, 503], 4))
        self.assertTrue(base.have_quorum([201, 503, 503, 201], 4))
        self.assertTrue(base.have_quorum([201, 201], 3))
        self.assertTrue(base.have_quorum([404, 404], 3))
        self.assertTrue(base.have_quorum([201, 201], 2))
        self.assertTrue(base.have_quorum([201, 404], 2))
        self.assertTrue(base.have_quorum([404, 404], 2))
        self.assertTrue(base.have_quorum([201, 404, 201, 201], 4))

    def test_best_response_overrides(self):
        base = Controller(self.app)
        responses = [
            (302, 'Found', '', b'The resource has moved temporarily.'),
            (100, 'Continue', '', b''),
            (404, 'Not Found', '', b'Custom body'),
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
        self.assertEqual(resp.body, b'Custom body')

    def test_range_fast_forward(self):
        req = Request.blank('/')
        handler = GetOrHeadHandler(None, req, None, None, None, None, {})
        handler.fast_forward(50)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=50-')

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=23-50'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=43-50')
        self.assertRaises(HTTPException,
                          handler.fast_forward, 80)
        self.assertRaises(exceptions.RangeAlreadyComplete,
                          handler.fast_forward, 8)

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=23-'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=43-')

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=-100'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=-80')
        self.assertRaises(HTTPException,
                          handler.fast_forward, 100)
        self.assertRaises(exceptions.RangeAlreadyComplete,
                          handler.fast_forward, 80)

        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=0-0'})
        self.assertRaises(exceptions.RangeAlreadyComplete,
                          handler.fast_forward, 1)

    def test_range_fast_forward_after_data_timeout(self):
        req = Request.blank('/')

        # We get a 200 and learn that it's a 1000-byte object, but receive 0
        # bytes of data, so then we get a new node, fast_forward(0), and
        # send out a new request. That new request must be for all 1000
        # bytes.
        handler = GetOrHeadHandler(None, req, None, None, None, None, {})
        handler.learn_size_from_content_range(0, 999, 1000)
        handler.fast_forward(0)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=0-999')

        # Same story as above, but a 1-byte object so we can have our byte
        # indices be 0.
        handler = GetOrHeadHandler(None, req, None, None, None, None, {})
        handler.learn_size_from_content_range(0, 0, 1)
        handler.fast_forward(0)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=0-0')

        # last 100 bytes
        handler = GetOrHeadHandler(None, req, None, None, None, None,
                                   {'Range': 'bytes=-100'})
        handler.learn_size_from_content_range(900, 999, 1000)
        handler.fast_forward(0)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=900-999')

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
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        self.assertNotIn('new-owner', dst_headers)

    def test_generate_request_headers_with_sysmeta(self):
        base = Controller(self.app)
        good_hdrs = {'x-base-sysmeta-foo': 'ok',
                     'X-Base-sysmeta-Bar': 'also ok'}
        bad_hdrs = {'x-base-sysmeta-': 'too short'}
        hdrs = dict(good_hdrs)
        hdrs.update(bad_hdrs)
        req = Request.blank('/v1/a/c/o', headers=hdrs)
        dst_headers = base.generate_request_headers(req, transfer=True)
        for k, v in good_hdrs.items():
            self.assertIn(k.lower(), dst_headers)
            self.assertEqual(v, dst_headers[k.lower()])
        for k, v in bad_hdrs.items():
            self.assertNotIn(k.lower(), dst_headers)

    def test_generate_request_headers_with_no_orig_req(self):
        base = Controller(self.app)
        src_headers = {'x-remove-base-meta-owner': 'x',
                       'x-base-meta-size': '151M',
                       'new-owner': 'Kun'}
        dst_headers = base.generate_request_headers(None,
                                                    additional=src_headers)
        expected_headers = {'x-base-meta-size': '151M',
                            'connection': 'close'}
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        self.assertEqual('', dst_headers['Referer'])

    def test_client_chunk_size(self):

        class TestSource(object):
            def __init__(self, chunks):
                self.chunks = list(chunks)
                self.status = 200

            def read(self, _read_size):
                if self.chunks:
                    return self.chunks.pop(0)
                else:
                    return b''

            def getheader(self, header):
                if header.lower() == "content-length":
                    return str(sum(len(c) for c in self.chunks))

            def getheaders(self):
                return [('content-length', self.getheader('content-length'))]

        source = TestSource((
            b'abcd', b'1234', b'abc', b'd1', b'234abcd1234abcd1', b'2'))
        req = Request.blank('/v1/a/c/o')
        node = {}
        handler = GetOrHeadHandler(self.app, req, None, None, None, None, {},
                                   client_chunk_size=8)

        app_iter = handler._make_app_iter(req, node, source)
        client_chunks = list(app_iter)
        self.assertEqual(client_chunks, [
            b'abcd1234', b'abcd1234', b'abcd1234', b'abcd12'])

    def test_client_chunk_size_resuming(self):

        class TestSource(object):
            def __init__(self, chunks):
                self.chunks = list(chunks)
                self.status = 200

            def read(self, _read_size):
                if self.chunks:
                    chunk = self.chunks.pop(0)
                    if chunk is None:
                        raise exceptions.ChunkReadTimeout()
                    else:
                        return chunk
                else:
                    return b''

            def getheader(self, header):
                # content-length for the whole object is generated dynamically
                # by summing non-None chunks initialized as source1
                if header.lower() == "content-length":
                    return str(sum(len(c) for c in self.chunks
                                   if c is not None))

            def getheaders(self):
                return [('content-length', self.getheader('content-length'))]

        node = {'ip': '1.2.3.4', 'port': 6200, 'device': 'sda'}

        source1 = TestSource([b'abcd', b'1234', None,
                              b'efgh', b'5678', b'lots', b'more', b'data'])
        # incomplete reads of client_chunk_size will be re-fetched
        source2 = TestSource([b'efgh', b'5678', b'lots', None])
        source3 = TestSource([b'lots', b'more', b'data'])
        req = Request.blank('/v1/a/c/o')
        handler = GetOrHeadHandler(
            self.app, req, 'Object', None, None, None, {},
            client_chunk_size=8)

        range_headers = []
        sources = [(source2, node), (source3, node)]

        def mock_get_source_and_node():
            range_headers.append(handler.backend_headers['Range'])
            return sources.pop(0)

        app_iter = handler._make_app_iter(req, node, source1)
        with mock.patch.object(handler, '_get_source_and_node',
                               side_effect=mock_get_source_and_node):
            client_chunks = list(app_iter)
        self.assertEqual(range_headers, ['bytes=8-27', 'bytes=16-27'])
        self.assertEqual(client_chunks, [
            b'abcd1234', b'efgh5678', b'lotsmore', b'data'])

    def test_client_chunk_size_resuming_chunked(self):

        class TestChunkedSource(object):
            def __init__(self, chunks):
                self.chunks = list(chunks)
                self.status = 200
                self.headers = {'transfer-encoding': 'chunked',
                                'content-type': 'text/plain'}

            def read(self, _read_size):
                if self.chunks:
                    chunk = self.chunks.pop(0)
                    if chunk is None:
                        raise exceptions.ChunkReadTimeout()
                    else:
                        return chunk
                else:
                    return b''

            def getheader(self, header):
                return self.headers.get(header.lower())

            def getheaders(self):
                return self.headers

        node = {'ip': '1.2.3.4', 'port': 6200, 'device': 'sda'}

        source1 = TestChunkedSource([b'abcd', b'1234', b'abc', None])
        source2 = TestChunkedSource([b'efgh5678'])
        req = Request.blank('/v1/a/c/o')
        handler = GetOrHeadHandler(
            self.app, req, 'Object', None, None, None, {},
            client_chunk_size=8)

        app_iter = handler._make_app_iter(req, node, source1)
        with mock.patch.object(handler, '_get_source_and_node',
                               lambda: (source2, node)):
            client_chunks = list(app_iter)
        self.assertEqual(client_chunks, [b'abcd1234', b'efgh5678'])

    def test_disconnected_warning(self):
        self.app.logger = mock.Mock()
        req = Request.blank('/v1/a/c/o')

        class TestSource(object):
            def __init__(self):
                self.headers = {'content-type': 'text/plain',
                                'content-length': len(self.read(-1))}
                self.status = 200

            def read(self, _read_size):
                return b'the cake is a lie'

            def getheader(self, header):
                return self.headers.get(header.lower())

            def getheaders(self):
                return self.headers

        source = TestSource()

        node = {'ip': '1.2.3.4', 'port': 6200, 'device': 'sda'}
        handler = GetOrHeadHandler(
            self.app, req, 'Object', None, None, None, {})
        app_iter = handler._make_app_iter(req, node, source)
        app_iter.close()
        self.app.logger.warning.assert_called_once_with(
            'Client disconnected on read')

        self.app.logger = mock.Mock()
        node = {'ip': '1.2.3.4', 'port': 6200, 'device': 'sda'}
        handler = GetOrHeadHandler(
            self.app, req, 'Object', None, None, None, {})
        app_iter = handler._make_app_iter(req, node, source)
        next(app_iter)
        app_iter.close()
        self.app.logger.warning.assert_not_called()

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

    def test_get_shard_ranges_for_container_get(self):
        ts_iter = make_timestamp_iter()
        shard_ranges = [dict(ShardRange(
            '.sharded_a/sr%d' % i, next(ts_iter), '%d_lower' % i,
            '%d_upper' % i, object_count=i, bytes_used=1024 * i,
            meta_timestamp=next(ts_iter)))
            for i in range(3)]
        base = Controller(self.app)
        req = Request.blank('/v1/a/c', method='GET')
        resp_headers = {'X-Backend-Record-Type': 'shard'}
        with mocked_http_conn(
            200, 200,
            body_iter=iter([b'', json.dumps(shard_ranges).encode('ascii')]),
            headers=resp_headers
        ) as fake_conn:
            actual = base._get_shard_ranges(req, 'a', 'c')

        # account info
        captured = fake_conn.requests
        self.assertEqual('HEAD', captured[0]['method'])
        self.assertEqual('a', captured[0]['path'][7:])
        # container GET
        self.assertEqual('GET', captured[1]['method'])
        self.assertEqual('a/c', captured[1]['path'][7:])
        self.assertEqual('format=json', captured[1]['qs'])
        self.assertEqual(
            'shard', captured[1]['headers'].get('X-Backend-Record-Type'))
        self.assertEqual(shard_ranges, [dict(pr) for pr in actual])
        self.assertFalse(self.app.logger.get_lines_for_level('error'))

    def test_get_shard_ranges_for_object_put(self):
        ts_iter = make_timestamp_iter()
        shard_ranges = [dict(ShardRange(
            '.sharded_a/sr%d' % i, next(ts_iter), '%d_lower' % i,
            '%d_upper' % i, object_count=i, bytes_used=1024 * i,
            meta_timestamp=next(ts_iter)))
            for i in range(3)]
        base = Controller(self.app)
        req = Request.blank('/v1/a/c/o', method='PUT')
        resp_headers = {'X-Backend-Record-Type': 'shard'}
        with mocked_http_conn(
            200, 200,
            body_iter=iter([b'',
                            json.dumps(shard_ranges[1:2]).encode('ascii')]),
            headers=resp_headers
        ) as fake_conn:
            actual = base._get_shard_ranges(req, 'a', 'c', '1_test')

        # account info
        captured = fake_conn.requests
        self.assertEqual('HEAD', captured[0]['method'])
        self.assertEqual('a', captured[0]['path'][7:])
        # container GET
        self.assertEqual('GET', captured[1]['method'])
        self.assertEqual('a/c', captured[1]['path'][7:])
        params = sorted(captured[1]['qs'].split('&'))
        self.assertEqual(
            ['format=json', 'includes=1_test'], params)
        self.assertEqual(
            'shard', captured[1]['headers'].get('X-Backend-Record-Type'))
        self.assertEqual(shard_ranges[1:2], [dict(pr) for pr in actual])
        self.assertFalse(self.app.logger.get_lines_for_level('error'))

    def _check_get_shard_ranges_bad_data(self, body):
        base = Controller(self.app)
        req = Request.blank('/v1/a/c/o', method='PUT')
        # empty response
        headers = {'X-Backend-Record-Type': 'shard'}
        with mocked_http_conn(200, 200, body_iter=iter([b'', body]),
                              headers=headers):
            actual = base._get_shard_ranges(req, 'a', 'c', '1_test')
        self.assertIsNone(actual)
        lines = self.app.logger.get_lines_for_level('error')
        return lines

    def test_get_shard_ranges_empty_body(self):
        error_lines = self._check_get_shard_ranges_bad_data(b'')
        self.assertIn('Problem with listing response', error_lines[0])
        if six.PY2:
            self.assertIn('No JSON', error_lines[0])
        else:
            self.assertIn('JSONDecodeError', error_lines[0])
        self.assertFalse(error_lines[1:])

    def test_get_shard_ranges_not_a_list(self):
        body = json.dumps({}).encode('ascii')
        error_lines = self._check_get_shard_ranges_bad_data(body)
        self.assertIn('Problem with listing response', error_lines[0])
        self.assertIn('not a list', error_lines[0])
        self.assertFalse(error_lines[1:])

    def test_get_shard_ranges_key_missing(self):
        body = json.dumps([{}]).encode('ascii')
        error_lines = self._check_get_shard_ranges_bad_data(body)
        self.assertIn('Failed to get shard ranges', error_lines[0])
        self.assertIn('KeyError', error_lines[0])
        self.assertFalse(error_lines[1:])

    def test_get_shard_ranges_invalid_shard_range(self):
        sr = ShardRange('a/c', Timestamp.now())
        bad_sr_data = dict(sr, name='bad_name')
        body = json.dumps([bad_sr_data]).encode('ascii')
        error_lines = self._check_get_shard_ranges_bad_data(body)
        self.assertIn('Failed to get shard ranges', error_lines[0])
        self.assertIn('ValueError', error_lines[0])
        self.assertFalse(error_lines[1:])

    def test_get_shard_ranges_missing_record_type(self):
        base = Controller(self.app)
        req = Request.blank('/v1/a/c/o', method='PUT')
        sr = ShardRange('a/c', Timestamp.now())
        body = json.dumps([dict(sr)]).encode('ascii')
        with mocked_http_conn(
                200, 200, body_iter=iter([b'', body])):
            actual = base._get_shard_ranges(req, 'a', 'c', '1_test')
        self.assertIsNone(actual)
        error_lines = self.app.logger.get_lines_for_level('error')
        self.assertIn('Failed to get shard ranges', error_lines[0])
        self.assertIn('unexpected record type', error_lines[0])
        self.assertIn('/a/c', error_lines[0])
        self.assertFalse(error_lines[1:])

    def test_get_shard_ranges_wrong_record_type(self):
        base = Controller(self.app)
        req = Request.blank('/v1/a/c/o', method='PUT')
        sr = ShardRange('a/c', Timestamp.now())
        body = json.dumps([dict(sr)]).encode('ascii')
        headers = {'X-Backend-Record-Type': 'object'}
        with mocked_http_conn(
                200, 200, body_iter=iter([b'', body]),
                headers=headers):
            actual = base._get_shard_ranges(req, 'a', 'c', '1_test')
        self.assertIsNone(actual)
        error_lines = self.app.logger.get_lines_for_level('error')
        self.assertIn('Failed to get shard ranges', error_lines[0])
        self.assertIn('unexpected record type', error_lines[0])
        self.assertIn('/a/c', error_lines[0])
        self.assertFalse(error_lines[1:])

    def test_get_shard_ranges_request_failed(self):
        base = Controller(self.app)
        req = Request.blank('/v1/a/c/o', method='PUT')
        with mocked_http_conn(200, 404, 404, 404):
            actual = base._get_shard_ranges(req, 'a', 'c', '1_test')
        self.assertIsNone(actual)
        self.assertFalse(self.app.logger.get_lines_for_level('error'))
        warning_lines = self.app.logger.get_lines_for_level('warning')
        self.assertIn('Failed to get container listing', warning_lines[0])
        self.assertIn('/a/c', warning_lines[0])
        self.assertFalse(warning_lines[1:])
