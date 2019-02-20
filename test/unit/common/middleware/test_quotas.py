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

from swift.common.swob import Request, HTTPUnauthorized, HTTPOk
from swift.common.middleware import container_quotas, copy
from test.unit.common.middleware.helpers import FakeSwift


class FakeCache(object):

    def __init__(self, val):
        if 'status' not in val:
            val['status'] = 200
        self.val = val

    def get(self, *args):
        return self.val


class FakeApp(object):

    def __init__(self):
        pass

    def __call__(self, env, start_response):
        start_response('200 OK', [])
        return []


class FakeMissingApp(object):

    def __init__(self):
        pass

    def __call__(self, env, start_response):
        start_response('404 Not Found', [])
        return []


def start_response(*args):
    pass


class TestContainerQuotas(unittest.TestCase):

    def test_split_path_empty_container_path_segment(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a//something/something_else',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': {'key': 'value'}})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_not_handled(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'PUT'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_no_quotas(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeCache({}),
                     'CONTENT_LENGTH': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_exceed_bytes_quota(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '2'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_not_exceed_bytes_quota(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '100'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_exceed_counts_quota(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '1'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_not_exceed_counts_quota(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '2'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_invalid_quotas(self):
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'HTTP_X_CONTAINER_META_QUOTA_BYTES': 'abc'})
        res = req.get_response(
            container_quotas.ContainerQuotaMiddleware(FakeApp(), {}))
        self.assertEqual(res.status_int, 400)

        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'HTTP_X_CONTAINER_META_QUOTA_COUNT': 'abc'})
        res = req.get_response(
            container_quotas.ContainerQuotaMiddleware(FakeApp(), {}))
        self.assertEqual(res.status_int, 400)

    def test_valid_quotas(self):
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'HTTP_X_CONTAINER_META_QUOTA_BYTES': '123'})
        res = req.get_response(
            container_quotas.ContainerQuotaMiddleware(FakeApp(), {}))
        self.assertEqual(res.status_int, 200)

        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'HTTP_X_CONTAINER_META_QUOTA_COUNT': '123'})
        res = req.get_response(
            container_quotas.ContainerQuotaMiddleware(FakeApp(), {}))
        self.assertEqual(res.status_int, 200)

    def test_delete_quotas(self):
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'HTTP_X_CONTAINER_META_QUOTA_BYTES': None})
        res = req.get_response(
            container_quotas.ContainerQuotaMiddleware(FakeApp(), {}))
        self.assertEqual(res.status_int, 200)

    def test_missing_container(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeMissingApp(), {})
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '100'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 404)

    def test_auth_fail(self):
        app = container_quotas.ContainerQuotaMiddleware(FakeApp(), {})
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '1'},
                           'write_acl': None})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100',
                     'swift.authorize': lambda *args: HTTPUnauthorized()})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 401)


class ContainerQuotaCopyingTestCases(unittest.TestCase):

    def setUp(self):
        self.app = FakeSwift()
        self.cq_filter = container_quotas.filter_factory({})(self.app)
        self.copy_filter = copy.filter_factory({})(self.cq_filter)

    def test_exceed_bytes_quota_copy_verb(self):
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '2'}})
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')

        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache},
                            headers={'Destination': '/c/o'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_not_exceed_bytes_quota_copy_verb(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', HTTPOk, {}, 'passed')
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '100'}})
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache},
                            headers={'Destination': '/c/o'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_exceed_counts_quota_copy_verb(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk, {}, 'passed')
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '1'}})
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache},
                            headers={'Destination': '/c/o'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_exceed_counts_quota_copy_cross_account_verb(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk, {}, 'passed')
        a_c_cache = {'storage_policy': '0', 'meta': {'quota-count': '2'},
                     'status': 200, 'object_count': 1}
        a2_c_cache = {'storage_policy': '0', 'meta': {'quota-count': '1'},
                      'status': 200, 'object_count': 1}
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.infocache': {
                                         'container/a/c': a_c_cache,
                                         'container/a2/c': a2_c_cache}},
                            headers={'Destination': '/c/o',
                                     'Destination-Account': 'a2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_exceed_counts_quota_copy_cross_account_PUT_verb(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk, {}, 'passed')
        a_c_cache = {'storage_policy': '0', 'meta': {'quota-count': '2'},
                     'status': 200, 'object_count': 1}
        a2_c_cache = {'storage_policy': '0', 'meta': {'quota-count': '1'},
                      'status': 200, 'object_count': 1}
        req = Request.blank('/v1/a2/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.infocache': {
                                         'container/a/c': a_c_cache,
                                         'container/a2/c': a2_c_cache}},
                            headers={'X-Copy-From': '/c2/o2',
                                     'X-Copy-From-Account': 'a'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_exceed_bytes_quota_copy_from(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '2'}})

        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_not_exceed_bytes_quota_copy_from(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', HTTPOk, {}, 'passed')
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '100'}})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_bytes_quota_copy_from_no_src(self):
        self.app.register('GET', '/v1/a/c2/o3', HTTPOk, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', HTTPOk, {}, 'passed')
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '100'}})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o3'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_bytes_quota_copy_from_bad_src(self):
        cache = FakeCache({'bytes': 0, 'meta': {'quota-bytes': '100'}})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': 'bad_path'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 412)

    def test_exceed_counts_quota_copy_from(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '1'}})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_not_exceed_counts_quota_copy_from(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', HTTPOk, {}, 'passed')
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '2'}})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_not_exceed_counts_quota_copy_verb(self):
        self.app.register('GET', '/v1/a/c2/o2', HTTPOk,
                          {'Content-Length': '10'}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', HTTPOk, {}, 'passed')
        cache = FakeCache({'object_count': 1, 'meta': {'quota-count': '2'}})
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache},
                            headers={'Destination': '/c/o'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

if __name__ == '__main__':
    unittest.main()
