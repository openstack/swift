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
import operator
import os
from argparse import Namespace
import itertools
import json
from collections import defaultdict
import unittest
from unittest import mock

from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info, \
    headers_to_account_info, headers_to_object_info, get_container_info, \
    get_cache_key, get_account_info, get_info, get_object_info, \
    Controller, GetOrHeadHandler, bytes_to_skip, clear_info_cache, \
    set_info_cache, NodeIter, headers_from_container_info, \
    record_cache_op_metrics, GetterSource, get_namespaces_from_cache, \
    set_namespaces_in_cache
from swift.common.swob import Request, HTTPException, RESPONSE_REASONS, \
    bytes_to_wsgi
from swift.common import exceptions
from swift.common.utils import split_path, Timestamp, \
    GreenthreadSafeIterator, GreenAsyncPile, NamespaceBoundList
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import is_success
from swift.common.storage_policy import StoragePolicy, StoragePolicyCollection
from test.debug_logger import debug_logger
from test.unit import (
    fake_http_connect, FakeRing, FakeMemcache, PatchPolicies, patch_policies,
    FakeSource, StubResponse, CaptureIteratorFactory)
from swift.common.request_helpers import (
    get_sys_meta_prefix, get_object_transient_sysmeta
)


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
        'x-versions-location': bytes_to_wsgi(
            u'\U0001F334'.encode('utf8')),
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
    container_existence_skip_cache = 0
    recheck_account_existence = 30
    account_existence_skip_cache = 0
    logger = None

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
        # Fake a json roundtrip
        self.stub = json.loads(json.dumps(stub))

    def get(self, key, raise_on_error=False):
        return self.stub or super(FakeCache, self).get(key, raise_on_error)


class BaseTest(unittest.TestCase):

    def setUp(self):
        self.logger = debug_logger()
        self.cache = FakeCache()
        self.conf = {}
        self.account_ring = FakeRing()
        self.container_ring = FakeRing()
        self.app = proxy_server.Application(self.conf,
                                            logger=self.logger,
                                            account_ring=self.account_ring,
                                            container_ring=self.container_ring)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestFuncs(BaseTest):

    def test_get_namespaces_from_cache_disabled(self):
        cache_key = 'shard-updating-v2/a/c/'
        req = Request.blank('a/c')
        actual = get_namespaces_from_cache(req, cache_key, 0)
        self.assertEqual((None, 'disabled'), actual)

    def test_get_namespaces_from_cache_miss(self):
        cache_key = 'shard-updating-v2/a/c/'
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        actual = get_namespaces_from_cache(req, cache_key, 0)
        self.assertEqual((None, 'miss'), actual)

    def test_get_namespaces_from_cache_infocache_hit(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list1 = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])
        ns_bound_list2 = NamespaceBoundList([['', 'sr3'], ['t', 'sr4']])
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        req.environ['swift.infocache'] = {cache_key: ns_bound_list1}
        # memcache ignored if infocache hits
        self.cache.set(cache_key, ns_bound_list2.bounds)
        actual = get_namespaces_from_cache(req, cache_key, 0)
        self.assertEqual((ns_bound_list1, 'infocache_hit'), actual)

    def test_get_namespaces_from_cache_hit(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr3'], ['t', 'sr4']])
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        req.environ['swift.infocache'] = {}
        self.cache.set(cache_key, ns_bound_list.bounds)
        actual = get_namespaces_from_cache(req, 'shard-updating-v2/a/c/', 0)
        self.assertEqual((ns_bound_list, 'hit'), actual)
        self.assertEqual({cache_key: ns_bound_list},
                         req.environ['swift.infocache'])

    def test_get_namespaces_from_cache_skips(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])

        self.cache.set(cache_key, ns_bound_list.bounds)
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        with mock.patch('swift.proxy.controllers.base.random.random',
                        return_value=0.099):
            actual = get_namespaces_from_cache(req, cache_key, 0.1)
        self.assertEqual((None, 'skip'), actual)

        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        with mock.patch('swift.proxy.controllers.base.random.random',
                        return_value=0.1):
            actual = get_namespaces_from_cache(req, cache_key, 0.1)
        self.assertEqual((ns_bound_list, 'hit'), actual)

    def test_get_namespaces_from_cache_error(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])
        self.cache.set(cache_key, ns_bound_list.bounds)
        # sanity check
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        actual = get_namespaces_from_cache(req, cache_key, 0.0)
        self.assertEqual((ns_bound_list, 'hit'), actual)

        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        self.cache.error_on_get = [True]
        actual = get_namespaces_from_cache(req, cache_key, 0.0)
        self.assertEqual((None, 'error'), actual)

    def test_set_namespaces_in_cache_disabled(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])
        req = Request.blank('a/c')
        actual = set_namespaces_in_cache(req, cache_key, ns_bound_list, 123)
        self.assertEqual('disabled', actual)
        self.assertEqual({cache_key: ns_bound_list},
                         req.environ['swift.infocache'])

    def test_set_namespaces_in_cache_ok(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        actual = set_namespaces_in_cache(req, cache_key, ns_bound_list, 123)
        self.assertEqual('set', actual)
        self.assertEqual({cache_key: ns_bound_list},
                         req.environ['swift.infocache'])
        self.assertEqual(ns_bound_list.bounds, self.cache.store.get(cache_key))
        self.assertEqual(123, self.cache.times.get(cache_key))

    def test_set_namespaces_in_cache_infocache_exists(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])
        req = Request.blank('a/c')
        req.environ['swift.infocache'] = {'already': 'exists'}
        actual = set_namespaces_in_cache(req, cache_key, ns_bound_list, 123)
        self.assertEqual('disabled', actual)
        self.assertEqual({'already': 'exists', cache_key: ns_bound_list},
                         req.environ['swift.infocache'])

    def test_set_namespaces_in_cache_error(self):
        cache_key = 'shard-updating-v2/a/c/'
        ns_bound_list = NamespaceBoundList([['', 'sr1'], ['k', 'sr2']])
        req = Request.blank('a/c')
        req.environ['swift.cache'] = self.cache
        self.cache.error_on_set = [True]
        actual = set_namespaces_in_cache(req, cache_key, ns_bound_list, 123)
        self.assertEqual('set_error', actual)
        self.assertEqual(ns_bound_list,
                         req.environ['swift.infocache'].get(cache_key))

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

    def test_get_container_info_in_pipeline(self):
        final_app = FakeApp()

        def factory(app, include_pipeline_ref=True):
            def wsgi_filter(env, start_response):
                # lots of middlewares get info...
                if env['PATH_INFO'].count('/') > 2:
                    get_container_info(env, app)
                else:
                    get_account_info(env, app)
                # ...then decide to no-op based on the result
                return app(env, start_response)

            if include_pipeline_ref:
                # Note that we have to do some book-keeping in tests to mimic
                # what would be done in swift.common.wsgi.load_app
                wsgi_filter._pipeline_final_app = final_app
                wsgi_filter._pipeline_request_logging_app = final_app
            return wsgi_filter

        # build up a pipeline
        filtered_app = factory(factory(factory(final_app)))
        req = Request.blank("/v1/a/c/o", environ={'swift.cache': FakeCache()})
        req.get_response(filtered_app)
        self.assertEqual([e['PATH_INFO'] for e in final_app.captured_envs],
                         ['/v1/a', '/v1/a/c', '/v1/a/c/o'])

        # but we can't completely rely on our run_server pipeline-building
        # attaching proxy-app references; some 3rd party middlewares may
        # compose themselves as multiple filters, and only the outer-most one
        # would have the reference
        filtered_app = factory(factory(factory(final_app), False))
        del final_app.captured_envs[:]
        req = Request.blank("/v1/a/c/o", environ={'swift.cache': FakeCache()})
        req.get_response(filtered_app)
        self.assertEqual([e['PATH_INFO'] for e in final_app.captured_envs], [
            '/v1/a', '/v1/a', '/v1/a/c', '/v1/a/c', '/v1/a/c/o'])

    def test_get_account_info_uses_logging_app(self):
        def factory(app, func=None):
            calls = []

            def wsgi_filter(env, start_response):
                calls.append(env)
                if func:
                    func(env, app)
                return app(env, start_response)

            return wsgi_filter, calls

        # build up a pipeline, pretend there is a proxy_logging middleware
        final_app = FakeApp()
        logging_app, logging_app_calls = factory(final_app)
        filtered_app, filtered_app_calls = factory(logging_app,
                                                   func=get_account_info)
        # mimic what would be done in swift.common.wsgi.load_app
        for app in (filtered_app, logging_app):
            app._pipeline_final_app = final_app
            app._pipeline_request_logging_app = logging_app
        req = Request.blank("/v1/a/c/o", environ={'swift.cache': FakeCache()})
        req.get_response(filtered_app)
        self.assertEqual([e['PATH_INFO'] for e in final_app.captured_envs],
                         ['/v1/a', '/v1/a/c/o'])
        self.assertEqual([e['PATH_INFO'] for e in logging_app_calls],
                         ['/v1/a', '/v1/a/c/o'])
        self.assertEqual([e['PATH_INFO'] for e in filtered_app_calls],
                         ['/v1/a/c/o'])

    def test_get_container_info_uses_logging_app(self):
        def factory(app, func=None):
            calls = []

            def wsgi_filter(env, start_response):
                calls.append(env)
                if func:
                    func(env, app)
                return app(env, start_response)

            return wsgi_filter, calls

        # build up a pipeline, pretend there is a proxy_logging middleware
        final_app = FakeApp()
        logging_app, logging_app_calls = factory(final_app)
        filtered_app, filtered_app_calls = factory(logging_app,
                                                   func=get_container_info)
        # mimic what would be done in swift.common.wsgi.load_app
        for app in (filtered_app, logging_app):
            app._pipeline_final_app = final_app
            app._pipeline_request_logging_app = logging_app
        req = Request.blank("/v1/a/c/o", environ={'swift.cache': FakeCache()})
        req.get_response(filtered_app)
        self.assertEqual([e['PATH_INFO'] for e in final_app.captured_envs],
                         ['/v1/a', '/v1/a/c', '/v1/a/c/o'])
        self.assertEqual([e['PATH_INFO'] for e in logging_app_calls],
                         ['/v1/a', '/v1/a/c', '/v1/a/c/o'])
        self.assertEqual([e['PATH_INFO'] for e in filtered_app_calls],
                         ['/v1/a/c/o'])

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
        expected = u'\U0001F334'
        self.assertEqual(resp['versions'], expected)

    def test_get_container_info_no_account(self):
        app = FakeApp(statuses=[404, 200])
        req = Request.blank("/v1/AUTH_does_not_exist/cont")
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 0)

    def test_get_container_info_no_container_gets_cached(self):
        fake_cache = FakeCache({})
        app = FakeApp(statuses=[200, 404])
        req = Request.blank("/v1/AUTH_account/does_not_exist",
                            environ={'swift.cache': fake_cache})
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 404)
        key = get_cache_key("AUTH_account", "does_not_exist")
        self.assertIn(key, fake_cache.store)
        self.assertEqual(fake_cache.store[key]['status'], 404)

    def test_get_container_info_bad_path(self):
        fake_cache = FakeCache({})
        req = Request.blank("/non-swift/AUTH_account/does_not_exist",
                            environ={'swift.cache': fake_cache})
        info = get_container_info(req.environ, FakeApp(statuses=[400]))
        self.assertEqual(info['status'], 0)
        # *not* cached
        key = get_cache_key("AUTH_account", "does_not_exist")
        self.assertNotIn(key, fake_cache.store)
        # not even the "account" is cached
        key = get_cache_key("AUTH_account")
        self.assertNotIn(key, fake_cache.store)

        # but if for some reason the account *already was* cached...
        fake_cache.store[key] = headers_to_account_info({}, 200)
        req = Request.blank("/non-swift/AUTH_account/does_not_exist",
                            environ={'swift.cache': fake_cache})
        info = get_container_info(req.environ, FakeApp(statuses=[400]))
        self.assertEqual(info['status'], 0)
        # resp *still* not cached
        key = get_cache_key("AUTH_account", "does_not_exist")
        self.assertNotIn(key, fake_cache.store)

        # still nothing, even if the container is already cached, too
        fake_cache.store[key] = headers_to_container_info({}, 200)
        req = Request.blank("/non-swift/AUTH_account/does_not_exist",
                            environ={'swift.cache': fake_cache})
        info = get_container_info(req.environ, FakeApp(statuses=[400]))
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
            'versions': u"\U0001F4A9",
            'meta': {u'some-\N{SNOWMAN}': u'non-ascii meta \U0001F334'}}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cache_stub)})
        resp = get_container_info(req.environ, FakeApp())
        self.assertEqual([(k, type(k)) for k in resp],
                         [(k, str) for k in resp])
        self.assertEqual(resp['storage_policy'], 0)
        self.assertEqual(resp['bytes'], 3333)
        self.assertEqual(resp['object_count'], 10)
        self.assertEqual(resp['status'], 404)
        expected = u'\U0001F4A9'
        self.assertEqual(resp['versions'], expected)

        for subdict in resp.values():
            if isinstance(subdict, dict):
                self.assertEqual([(k, type(k), v, type(v))
                                  for k, v in subdict.items()],
                                 [(k, str, v, str)
                                  for k, v in subdict.items()])

    def test_get_container_info_only_lookup_cache(self):
        # no container info is cached in cache.
        req = Request.blank("/v1/AUTH_account/cont",
                            environ={'swift.cache': FakeCache({})})
        resp = get_container_info(
            req.environ, self.app, swift_source=None, cache_only=True)
        self.assertEqual(resp['storage_policy'], 0)
        self.assertEqual(resp['bytes'], 0)
        self.assertEqual(resp['object_count'], 0)
        self.assertEqual(resp['versions'], None)
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.miss'])

        # container info is cached in cache.
        self.logger.clear()
        cache_stub = {
            'status': 404, 'bytes': 3333, 'object_count': 10,
            'versions': u"\U0001F4A9",
            'meta': {u'some-\N{SNOWMAN}': u'non-ascii meta \U0001F334'}}
        req = Request.blank("/v1/account/cont",
                            environ={'swift.cache': FakeCache(cache_stub)})
        resp = get_container_info(
            req.environ, self.app, swift_source=None, cache_only=True)
        self.assertEqual([(k, type(k)) for k in resp],
                         [(k, str) for k in resp])
        self.assertEqual(resp['storage_policy'], 0)
        self.assertEqual(resp['bytes'], 3333)
        self.assertEqual(resp['object_count'], 10)
        self.assertEqual(resp['status'], 404)
        expected = u'\U0001F4A9'
        self.assertEqual(resp['versions'], expected)
        for subdict in resp.values():
            if isinstance(subdict, dict):
                self.assertEqual([(k, type(k), v, type(v))
                                  for k, v in subdict.items()],
                                 [(k, str, v, str)
                                  for k, v in subdict.items()])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.hit'])

    def test_get_cache_key(self):
        self.assertEqual(get_cache_key("account", "cont"),
                         'container/account/cont')
        self.assertEqual(get_cache_key(b"account", b"cont", b'obj'),
                         'object/account/cont/obj')
        self.assertEqual(get_cache_key(u"account", u"cont", b'obj'),
                         'object/account/cont/obj')

        # Expected result should always be native string
        expected = u'container/\N{SNOWMAN}/\U0001F334'

        self.assertEqual(get_cache_key(u"\N{SNOWMAN}", u"\U0001F334"),
                         expected)
        self.assertEqual(get_cache_key(u"\N{SNOWMAN}".encode('utf8'),
                                       u"\U0001F334".encode('utf8')),
                         expected)

        self.assertEqual(get_cache_key("account", "cont", shard="listing"),
                         'shard-listing-v2/account/cont')
        self.assertEqual(get_cache_key("account", "cont", shard="updating"),
                         'shard-updating-v2/account/cont')
        self.assertRaises(ValueError,
                          get_cache_key, "account", shard="listing")
        self.assertRaises(ValueError,
                          get_cache_key, "account", "cont", "obj",
                          shard="listing")

    def test_get_container_info_env(self):
        cache_key = get_cache_key("account", "cont")
        req = Request.blank(
            "/v1/account/cont",
            environ={'swift.infocache': {cache_key: {'bytes': 3867}},
                     'swift.cache': FakeCache({})})
        resp = get_container_info(req.environ, 'xxx')
        self.assertEqual(resp['bytes'], 3867)

    def test_info_clearing(self):
        def check_in_cache(req, cache_key):
            self.assertIn(cache_key, req.environ['swift.infocache'])
            self.assertIn(cache_key, req.environ['swift.cache'].store)

        def check_not_in_cache(req, cache_key):
            self.assertNotIn(cache_key, req.environ['swift.infocache'])
            self.assertNotIn(cache_key, req.environ['swift.cache'].store)

        app = FakeApp(statuses=[200, 200])
        acct_cache_key = get_cache_key("account")
        cont_cache_key = get_cache_key("account", "cont")
        req = Request.blank(
            "/v1/account/cont", environ={"swift.cache": FakeCache()})
        # populate caches
        info = get_container_info(req.environ, app)
        self.assertEqual(info['status'], 200)

        check_in_cache(req, acct_cache_key)
        check_in_cache(req, cont_cache_key)

        clear_info_cache(req.environ, 'account', 'cont')
        check_in_cache(req, acct_cache_key)
        check_not_in_cache(req, cont_cache_key)

        # Can also use set_info_cache interface
        set_info_cache(req.environ, 'account', None, None)
        check_not_in_cache(req, acct_cache_key)
        check_not_in_cache(req, cont_cache_key)

        # check shard cache-keys
        shard_cache_key = get_cache_key('account', 'cont', shard='listing')
        shard_data = [{'shard': 'ranges'}]
        req.environ['swift.infocache'][shard_cache_key] = shard_data
        req.environ['swift.cache'].set(shard_cache_key, shard_data, time=600)
        check_in_cache(req, shard_cache_key)
        clear_info_cache(req.environ, 'account', 'cont',
                         shard='listing')
        check_not_in_cache(req, shard_cache_key)

    def test_record_cache_op_metrics(self):
        record_cache_op_metrics(
            self.logger, 'container', 'shard_listing', 'infocache_hit')
        self.assertEqual(
            self.logger.statsd_client.get_increment_counts().get(
                'container.shard_listing.infocache.hit'),
            1)
        record_cache_op_metrics(
            self.logger, 'container', 'shard_listing', 'hit')
        self.assertEqual(
            self.logger.statsd_client.get_increment_counts().get(
                'container.shard_listing.cache.hit'),
            1)
        resp = FakeResponse(status_int=200)
        record_cache_op_metrics(
            self.logger, 'object', 'shard_updating', 'skip', resp)
        self.assertEqual(
            self.logger.statsd_client.get_increment_counts().get(
                'object.shard_updating.cache.skip.200'),
            1)
        resp = FakeResponse(status_int=503)
        record_cache_op_metrics(
            self.logger, 'object', 'shard_updating', 'disabled', resp)
        self.assertEqual(
            self.logger.statsd_client.get_increment_counts().get(
                'object.shard_updating.cache.disabled.503'),
            1)

        # test a cache miss call without response, expect no metric recorded.
        self.app.logger = mock.Mock()
        record_cache_op_metrics(
            self.logger, 'object', 'shard_updating', 'miss')
        self.app.logger.increment.assert_not_called()

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

    def test_get_account_info_bad_path(self):
        fake_cache = FakeCache({})
        req = Request.blank("/non-swift/AUTH_account",
                            environ={'swift.cache': fake_cache})
        info = get_account_info(req.environ, FakeApp(statuses=[400]))
        self.assertEqual(info['status'], 0)
        # *not* cached
        key = get_cache_key("AUTH_account")
        self.assertNotIn(key, fake_cache.store)

        # but if for some reason the account *already was* cached...
        fake_cache.store[key] = headers_to_account_info({}, 200)
        req = Request.blank("/non-swift/AUTH_account/does_not_exist",
                            environ={'swift.cache': fake_cache})
        info = get_account_info(req.environ, FakeApp(statuses=[400]))
        self.assertEqual(info['status'], 0)

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
        self.assertIsNone(resp['sync_key'])
        self.assertIsNone(resp['sync_to'])

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
            'x-container-sync-to': '//r/c/a/c',
            'x-container-meta-access-control-allow-origin': 'here',
        }
        resp = headers_to_container_info(headers.items(), 200)
        self.assertEqual(resp['read_acl'], 'readvalue')
        self.assertEqual(resp['write_acl'], 'writevalue')
        self.assertEqual(resp['sync_key'], 'keyvalue')
        self.assertEqual(resp['sync_to'], '//r/c/a/c')
        self.assertEqual(resp['cors']['allow_origin'], 'here')

        headers['x-unused-header'] = 'blahblahblah'
        self.assertEqual(
            resp,
            headers_to_container_info(headers.items(), 200))

    def test_headers_from_container_info(self):
        self.assertIsNone(headers_from_container_info(None))
        self.assertIsNone(headers_from_container_info({}))

        meta = {'fruit': 'cake'}
        sysmeta = {'green': 'land'}
        info = {
            'status': 200,
            'read_acl': 'my-read-acl',
            'write_acl': 'my-write-acl',
            'sync_to': 'my-sync-to',
            'sync_key': 'my-sync-key',
            'object_count': 99,
            'bytes': 999,
            'versions': 'my-versions',
            'storage_policy': '0',
            'cors': {
                'allow_origin': 'my-cors-origin',
                'expose_headers': 'my-cors-hdrs',
                'max_age': 'my-cors-age'},
            'created_at': '123.456_12',
            'put_timestamp': '234.567_34',
            'delete_timestamp': '345_67',
            'status_changed_at': '246.8_9',
            'meta': meta,
            'sysmeta': sysmeta,
            'sharding_state': 'unsharded'
        }

        res = headers_from_container_info(info)

        expected = {
            'X-Backend-Delete-Timestamp': '345_67',
            'X-Backend-Put-Timestamp': '234.567_34',
            'X-Backend-Sharding-State': 'unsharded',
            'X-Backend-Status-Changed-At': '246.8_9',
            'X-Backend-Storage-Policy-Index': '0',
            'X-Backend-Timestamp': '123.456_12',
            'X-Container-Bytes-Used': '999',
            'X-Container-Meta-Fruit': 'cake',
            'X-Container-Object-Count': '99',
            'X-Container-Read': 'my-read-acl',
            'X-Container-Sync-Key': 'my-sync-key',
            'X-Container-Sync-To': 'my-sync-to',
            'X-Container-Sysmeta-Green': 'land',
            'X-Container-Write': 'my-write-acl',
            'X-Put-Timestamp': '0000000234.56700',
            'X-Storage-Policy': 'zero',
            'X-Timestamp': '0000000123.45600',
            'X-Versions-Location': 'my-versions',
            'X-Container-Meta-Access-Control-Allow-Origin': 'my-cors-origin',
            'X-Container-Meta-Access-Control-Expose-Headers': 'my-cors-hdrs',
            'X-Container-Meta-Access-Control-Max-Age': 'my-cors-age',
        }

        self.assertEqual(expected, res)

        for required in (
                'created_at', 'put_timestamp', 'delete_timestamp',
                'status_changed_at', 'storage_policy', 'object_count', 'bytes',
                'sharding_state'):
            incomplete_info = dict(info)
            incomplete_info.pop(required)
            self.assertIsNone(headers_from_container_info(incomplete_info))

        for hdr, optional in (
                ('X-Container-Read', 'read_acl'),
                ('X-Container-Write', 'write_acl'),
                ('X-Container-Sync-Key', 'sync_key'),
                ('X-Container-Sync-To', 'sync_to'),
                ('X-Versions-Location', 'versions'),
                ('X-Container-Meta-Fruit', 'meta'),
                ('X-Container-Sysmeta-Green', 'sysmeta'),
        ):
            incomplete_info = dict(info)
            incomplete_info.pop(optional)
            incomplete_expected = dict(expected)
            incomplete_expected.pop(hdr)
            self.assertEqual(incomplete_expected,
                             headers_from_container_info(incomplete_info))

        for hdr, optional in (
            ('Access-Control-Allow-Origin', 'allow_origin'),
            ('Access-Control-Expose-Headers', 'expose_headers'),
            ('Access-Control-Max-Age', 'max_age'),
        ):
            incomplete_info = dict(info)
            incomplete_cors = dict(info['cors'])
            incomplete_cors.pop(optional)
            incomplete_info['cors'] = incomplete_cors
            incomplete_expected = dict(expected)
            incomplete_expected.pop('X-Container-Meta-' + hdr)
            self.assertEqual(incomplete_expected,
                             headers_from_container_info(incomplete_info))

    def test_container_info_preserves_storage_policy(self):
        base = Controller(self.app)
        base.account_name = 'a'
        base.container_name = 'c'

        fake_info = {'status': 404, 'storage_policy': 1}

        with mock.patch('swift.proxy.controllers.base.'
                        'get_container_info', return_value=fake_info):
            container_info = \
                base.container_info(base.account_name, base.container_name,
                                    Request.blank('/'))
        self.assertEqual(container_info['status'], 404)
        self.assertEqual(container_info['storage_policy'], 1)
        self.assertEqual(container_info['partition'], None)
        self.assertEqual(container_info['nodes'], None)

    def test_container_info_needs_req(self):
        base = Controller(self.app)
        base.account_name = 'a'
        base.container_name = 'c'

        with mock.patch('swift.proxy.controllers.base.'
                        'http_connect', fake_http_connect(200)):
            container_info = \
                base.container_info(base.account_name,
                                    base.container_name, Request.blank('/'))
        self.assertEqual(container_info['status'], 503)

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
                       'x-base-sysmeta-mysysmeta': 'myvalue',
                       'x-Backend-No-Timestamp-Update': 'true',
                       'X-Backend-Storage-Policy-Index': '3',
                       'x-backendoftheworld': 'ignored',
                       'new-owner': 'Kun'}
        req = Request.blank('/v1/a/c/o', headers=src_headers)
        dst_headers = base.generate_request_headers(req)
        expected_headers = {'x-backend-no-timestamp-update': 'true',
                            'x-backend-storage-policy-index': '3',
                            'x-timestamp': mock.ANY,
                            'x-trans-id': '-',
                            'Referer': 'GET http://localhost/v1/a/c/o',
                            'connection': 'close',
                            'user-agent': 'proxy-server %d' % os.getpid()}
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        for k, v in expected_headers.items():
            dst_headers.pop(k)
        self.assertFalse(dst_headers)

        # with transfer=True
        req = Request.blank('/v1/a/c/o', headers=src_headers)
        dst_headers = base.generate_request_headers(req, transfer=True)
        expected_headers.update({'x-base-meta-owner': '',
                                 'x-base-meta-size': '151M',
                                 'x-base-sysmeta-mysysmeta': 'myvalue'})
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        for k, v in expected_headers.items():
            dst_headers.pop(k)
        self.assertFalse(dst_headers)

        # with additional
        req = Request.blank('/v1/a/c/o', headers=src_headers)
        dst_headers = base.generate_request_headers(
            req, transfer=True,
            additional=src_headers)
        expected_headers.update({'x-remove-base-meta-owner': 'x',
                                 'x-backendoftheworld': 'ignored',
                                 'new-owner': 'Kun'})
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        for k, v in expected_headers.items():
            dst_headers.pop(k)
        self.assertFalse(dst_headers)

        # with additional, verify precedence
        req = Request.blank('/v1/a/c/o', headers=src_headers)
        dst_headers = base.generate_request_headers(
            req, transfer=False,
            additional={'X-Backend-Storage-Policy-Index': '2',
                        'X-Timestamp': '1234.56789'})
        expected_headers = {'x-backend-no-timestamp-update': 'true',
                            'x-backend-storage-policy-index': '2',
                            'x-timestamp': '1234.56789',
                            'x-trans-id': '-',
                            'Referer': 'GET http://localhost/v1/a/c/o',
                            'connection': 'close',
                            'user-agent': 'proxy-server %d' % os.getpid()}
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        for k, v in expected_headers.items():
            dst_headers.pop(k)
        self.assertFalse(dst_headers)

    def test_generate_request_headers_change_backend_user_agent(self):
        base = Controller(self.app)
        self.app.backend_user_agent = "swift-flux-capacitor"
        src_headers = {'x-remove-base-meta-owner': 'x',
                       'x-base-meta-size': '151M',
                       'new-owner': 'Kun'}
        req = Request.blank('/v1/a/c/o', headers=src_headers)
        dst_headers = base.generate_request_headers(req, transfer=True)
        expected_headers = {'x-base-meta-owner': '',
                            'x-base-meta-size': '151M',
                            'connection': 'close',
                            'user-agent': 'swift-flux-capacitor'}
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
                                                    additional=src_headers,
                                                    transfer=True)
        expected_headers = {'x-base-meta-size': '151M',
                            'connection': 'close'}
        for k, v in expected_headers.items():
            self.assertIn(k, dst_headers)
            self.assertEqual(v, dst_headers[k])
        self.assertEqual('', dst_headers['Referer'])

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


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestNodeIter(BaseTest):

    def test_iter_default_fake_ring(self):
        for ring in (self.account_ring, self.container_ring):
            self.assertEqual(ring.replica_count, 3.0)
            node_iter = NodeIter('db', self.app, ring, 0, self.logger,
                                 request=Request.blank(''))
            self.assertEqual(6, node_iter.nodes_left)
            self.assertEqual(3, node_iter.primaries_left)
            count = 0
            for node in node_iter:
                count += 1
            self.assertEqual(count, 3)
            self.assertEqual(0, node_iter.primaries_left)
            # default fake_ring has NO handoffs, so nodes_left is kind of a lie
            self.assertEqual(3, node_iter.nodes_left)

    def test_iter_with_handoffs(self):
        ring = FakeRing(replicas=3, max_more_nodes=20)  # handoffs available
        policy = StoragePolicy(0, 'zero', object_ring=ring)
        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=Request.blank(''))
        self.assertEqual(6, node_iter.nodes_left)
        self.assertEqual(3, node_iter.primaries_left)
        primary_indexes = set()
        handoff_indexes = []
        count = 0
        for node in node_iter:
            if 'index' in node:
                primary_indexes.add(node['index'])
            else:
                handoff_indexes.append(node['handoff_index'])
            count += 1
        self.assertEqual(count, 6)
        self.assertEqual(0, node_iter.primaries_left)
        self.assertEqual(0, node_iter.nodes_left)
        self.assertEqual({0, 1, 2}, primary_indexes)
        self.assertEqual([0, 1, 2], handoff_indexes)

    def test_multi_iteration(self):
        ring = FakeRing(replicas=8, max_more_nodes=20)
        policy = StoragePolicy(0, 'ec', object_ring=ring)

        # sanity
        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=Request.blank(''))
        self.assertEqual(16, len([n for n in node_iter]))

        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=Request.blank(''))
        self.assertEqual(16, node_iter.nodes_left)
        self.assertEqual(8, node_iter.primaries_left)
        pile = GreenAsyncPile(5)

        def eat_node(node_iter):
            return next(node_iter)

        safe_iter = GreenthreadSafeIterator(node_iter)
        for i in range(5):
            pile.spawn(eat_node, safe_iter)

        nodes = []
        for node in pile:
            nodes.append(node)

        primary_indexes = {n['index'] for n in nodes}
        self.assertEqual(5, len(primary_indexes))
        self.assertEqual(3, node_iter.primaries_left)

        # it's problematic we don't decrement nodes_left until we resume
        self.assertEqual(12, node_iter.nodes_left)
        for node in node_iter:
            nodes.append(node)
        self.assertEqual(17, len(nodes))

    def test_annotate_node_with_use_replication(self):
        ring = FakeRing(replicas=8, max_more_nodes=20)
        policy = StoragePolicy(0, 'ec', object_ring=ring)

        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=Request.blank(''))
        for node in node_iter:
            self.assertIn('use_replication', node)
            self.assertFalse(node['use_replication'])

        req = Request.blank('a/c')
        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=req)
        for node in node_iter:
            self.assertIn('use_replication', node)
            self.assertFalse(node['use_replication'])

        req = Request.blank(
            'a/c', headers={'x-backend-use-replication-network': 'False'})
        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=req)
        for node in node_iter:
            self.assertIn('use_replication', node)
            self.assertFalse(node['use_replication'])

        req = Request.blank(
            'a/c', headers={'x-backend-use-replication-network': 'yes'})
        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, request=req)
        for node in node_iter:
            self.assertIn('use_replication', node)
            self.assertTrue(node['use_replication'])

    def test_iter_does_not_mutate_supplied_nodes(self):
        ring = FakeRing(replicas=8, max_more_nodes=20)
        policy = StoragePolicy(0, 'ec', object_ring=ring)
        other_iter = ring.get_part_nodes(0)
        node_iter = NodeIter(
            'object', self.app, policy.object_ring, 0, self.logger,
            policy=policy, node_iter=iter(other_iter),
            request=Request.blank(''))
        nodes = list(node_iter)
        self.assertEqual(len(other_iter), len(nodes))
        for node in nodes:
            self.assertIn('use_replication', node)
            self.assertFalse(node['use_replication'])
        self.assertEqual(other_iter, ring.get_part_nodes(0))


class TestGetterSource(unittest.TestCase):
    def _make_source(self, headers, node):
        resp = StubResponse(200, headers=headers)
        return GetterSource(self.app, resp, node)

    def setUp(self):
        self.app = FakeApp()
        self.node = {'ip': '1.2.3.4', 'port': '999'}
        self.headers = {'X-Timestamp': '1234567.12345'}
        self.resp = StubResponse(200, headers=self.headers)

    def test_init(self):
        src = GetterSource(self.app, self.resp, self.node)
        self.assertIs(self.app, src.app)
        self.assertIs(self.resp, src.resp)
        self.assertEqual(self.node, src.node)

    def test_timestamp(self):
        # first test the no timestamp header case. Defaults to 0.
        headers = {}
        src = self._make_source(headers, self.node)
        self.assertIsInstance(src.timestamp, Timestamp)
        self.assertEqual(Timestamp(0), src.timestamp)
        # now x-timestamp
        headers = dict(self.headers)
        src = self._make_source(headers, self.node)
        self.assertIsInstance(src.timestamp, Timestamp)
        self.assertEqual(Timestamp(1234567.12345), src.timestamp)
        headers['x-put-timestamp'] = '1234567.11111'
        src = self._make_source(headers, self.node)
        self.assertIsInstance(src.timestamp, Timestamp)
        self.assertEqual(Timestamp(1234567.11111), src.timestamp)
        headers['x-backend-timestamp'] = '1234567.22222'
        src = self._make_source(headers, self.node)
        self.assertIsInstance(src.timestamp, Timestamp)
        self.assertEqual(Timestamp(1234567.22222), src.timestamp)
        headers['x-backend-data-timestamp'] = '1234567.33333'
        src = self._make_source(headers, self.node)
        self.assertIsInstance(src.timestamp, Timestamp)
        self.assertEqual(Timestamp(1234567.33333), src.timestamp)

    def test_sort(self):
        # verify sorting by timestamp
        srcs = [
            self._make_source({'X-Timestamp': '12345.12345'},
                              {'ip': '1.2.3.7', 'port': '9'}),
            self._make_source({'X-Timestamp': '12345.12346'},
                              {'ip': '1.2.3.8', 'port': '8'}),
            self._make_source({'X-Timestamp': '12345.12343',
                               'X-Put-Timestamp': '12345.12344'},
                              {'ip': '1.2.3.9', 'port': '7'}),
        ]
        actual = sorted(srcs, key=operator.attrgetter('timestamp'))
        self.assertEqual([srcs[2], srcs[0], srcs[1]], actual)

    def test_close(self):
        # verify close is robust...
        # source has no resp
        src = GetterSource(self.app, None, self.node)
        src.close()
        # resp has no swift_conn
        src = GetterSource(self.app, self.resp, self.node)
        self.assertFalse(hasattr(src.resp, 'swift_conn'))
        src.close()
        # verify close is plumbed through...
        src.resp.swift_conn = mock.MagicMock()
        src.resp.nuke_from_orbit = mock.MagicMock()
        src.close()
        src.resp.nuke_from_orbit.assert_called_once_with()


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestGetOrHeadHandler(BaseTest):
    def test_init_node_timeout(self):
        conf = {'node_timeout': 5, 'recoverable_node_timeout': 3}
        app = proxy_server.Application(conf,
                                       logger=self.logger,
                                       account_ring=self.account_ring,
                                       container_ring=self.container_ring)
        # x-newest set
        req = Request.blank('/v1/a/c/o', headers={'X-Newest': 'true'})
        node_iter = Namespace(num_primary_nodes=3)
        # app.node_timeout
        getter = GetOrHeadHandler(
            app, req, 'Object', node_iter, None, None, {})
        self.assertEqual(5, getter.node_timeout)

        # x-newest not set
        req = Request.blank('/v1/a/c/o')
        node_iter = Namespace(num_primary_nodes=3)
        # app.recoverable_node_timeout
        getter = GetOrHeadHandler(
            app, req, 'Object', node_iter, None, None, {})
        self.assertEqual(3, getter.node_timeout)

        # app.node_timeout
        getter = GetOrHeadHandler(
            app, req, 'Account', node_iter, None, None, {})
        self.assertEqual(5, getter.node_timeout)

        getter = GetOrHeadHandler(
            app, req, 'Container', node_iter, None, None, {})
        self.assertEqual(5, getter.node_timeout)

    def test_disconnected_logging(self):
        req = Request.blank('/v1/a/c/o')
        headers = {'content-type': 'text/plain'}
        source = FakeSource([], headers=headers, body=b'the cake is a lie')

        node = {'ip': '1.2.3.4', 'port': 6200, 'device': 'sda'}
        handler = GetOrHeadHandler(
            self.app, req, 'Object', Namespace(num_primary_nodes=1), None,
            'some-path', {})

        def mock_find_source():
            handler.source = GetterSource(self.app, source, node)
            return True

        factory = CaptureIteratorFactory(handler._iter_parts_from_response)
        with mock.patch.object(handler, '_find_source', mock_find_source):
            with mock.patch.object(
                    handler, '_iter_parts_from_response', factory):
                resp = handler.get_working_response()
                resp.app_iter.close()
        # verify that iter exited
        self.assertEqual({1: ['__next__', 'close', '__del__']},
                         factory.captured_calls)
        self.assertEqual(["Client disconnected on read of 'some-path'"],
                         self.logger.get_lines_for_level('info'))

        self.logger.clear()
        node = {'ip': '1.2.3.4', 'port': 6200, 'device': 'sda'}
        handler = GetOrHeadHandler(
            self.app, req, 'Object', Namespace(num_primary_nodes=1), None,
            None, {})

        factory = CaptureIteratorFactory(handler._iter_parts_from_response)
        with mock.patch.object(handler, '_find_source', mock_find_source):
            with mock.patch.object(
                    handler, '_iter_parts_from_response', factory):
                resp = handler.get_working_response()
                next(resp.app_iter)
            resp.app_iter.close()
        self.assertEqual({1: ['__next__', 'close', '__del__']},
                         factory.captured_calls)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        self.assertEqual([], self.logger.get_lines_for_level('info'))

    def test_range_fast_forward(self):
        req = Request.blank('/')
        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {})
        handler.fast_forward(50)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=50-')

        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {'Range': 'bytes=23-50'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=43-50')
        self.assertRaises(HTTPException,
                          handler.fast_forward, 80)
        self.assertRaises(exceptions.RangeAlreadyComplete,
                          handler.fast_forward, 8)

        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {'Range': 'bytes=23-'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=43-')

        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {'Range': 'bytes=-100'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=-80')
        self.assertRaises(HTTPException,
                          handler.fast_forward, 100)
        self.assertRaises(exceptions.RangeAlreadyComplete,
                          handler.fast_forward, 80)

        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {'Range': 'bytes=0-0'})
        self.assertRaises(exceptions.RangeAlreadyComplete,
                          handler.fast_forward, 1)

        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {'Range': 'bytes=23-',
             'X-Backend-Ignore-Range-If-Metadata-Present':
             'X-Static-Large-Object'})
        handler.fast_forward(20)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=43-')
        self.assertNotIn('X-Backend-Ignore-Range-If-Metadata-Present',
                         handler.backend_headers)

    def test_range_fast_forward_after_data_timeout(self):
        req = Request.blank('/')

        # We get a 200 and learn that it's a 1000-byte object, but receive 0
        # bytes of data, so then we get a new node, fast_forward(0), and
        # send out a new request. That new request must be for all 1000
        # bytes.
        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {})
        handler.learn_size_from_content_range(0, 999, 1000)
        handler.fast_forward(0)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=0-999')

        # Same story as above, but a 1-byte object so we can have our byte
        # indices be 0.
        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {})
        handler.learn_size_from_content_range(0, 0, 1)
        handler.fast_forward(0)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=0-0')

        # last 100 bytes
        handler = GetOrHeadHandler(
            self.app, req, 'test', Namespace(num_primary_nodes=3), None, None,
            {'Range': 'bytes=-100'})
        handler.learn_size_from_content_range(900, 999, 1000)
        handler.fast_forward(0)
        self.assertEqual(handler.backend_headers['Range'], 'bytes=900-999')
