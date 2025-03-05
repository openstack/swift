# Copyright (c) 2019 OpenStack Foundation
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

import functools
import json
import os
import time
from unittest import mock
import unittest
import urllib.parse
from swift.common import swob, utils
from swift.common.middleware import versioned_writes, copy, symlink, \
    listing_formats
from swift.common.swob import Request, wsgi_quote, str_to_wsgi
from swift.common.middleware.symlink import TGT_OBJ_SYSMETA_SYMLINK_HDR, \
    ALLOW_RESERVED_NAMES, SYMLOOP_EXTEND
from swift.common.middleware.versioned_writes.object_versioning import \
    SYSMETA_VERSIONS_CONT, SYSMETA_VERSIONS_ENABLED, \
    SYSMETA_VERSIONS_SYMLINK, DELETE_MARKER_CONTENT_TYPE
from swift.common.request_helpers import get_reserved_name
from swift.common.storage_policy import StoragePolicy
from swift.common.utils import md5
from swift.proxy.controllers.base import get_cache_key
from test.unit import patch_policies, FakeMemcache, make_timestamp_iter
from test.unit.common.middleware.helpers import FakeSwift


def local_tz(func):
    '''
    Decorator to change the timezone when running a test.

    This uses the Eastern Time Zone definition from the time module's docs.
    Note that the timezone affects things like time.time() and time.mktime().
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        tz = os.environ.get('TZ', '')
        try:
            os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
            time.tzset()
            return func(*args, **kwargs)
        finally:
            os.environ['TZ'] = tz
            time.tzset()
    return wrapper


class ObjectVersioningBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        conf = {}
        self.sym = symlink.filter_factory(conf)(self.app)
        self.sym.logger = self.app.logger
        self.ov = versioned_writes.object_versioning.\
            ObjectVersioningMiddleware(self.sym, conf)
        self.ov.logger = self.app.logger
        self.cp = copy.filter_factory({})(self.ov)
        self.lf = listing_formats.ListingFilter(self.cp, {}, self.app.logger)

        self.ts = make_timestamp_iter()
        cont_cache_version_on = {'sysmeta': {
            'versions-container': self.build_container_name('c'),
            'versions-enabled': 'true'}}
        self.cache_version_on = FakeMemcache()
        self.cache_version_on.set(get_cache_key('a'), {'status': 200})
        self.cache_version_on.set(get_cache_key('a', 'c'),
                                  cont_cache_version_on)
        self.cache_version_on.set(
            get_cache_key('a', self.build_container_name('c')),
            {'status': 200})

        self.cache_version_on_but_busted = FakeMemcache()
        self.cache_version_on_but_busted.set(get_cache_key('a'),
                                             {'status': 200})
        self.cache_version_on_but_busted.set(get_cache_key('a', 'c'),
                                             cont_cache_version_on)
        self.cache_version_on_but_busted.set(
            get_cache_key('a', self.build_container_name('c')),
            {'status': 404})

        cont_cache_version_off = {'sysmeta': {
            'versions-container': self.build_container_name('c'),
            'versions-enabled': 'false'}}
        self.cache_version_off = FakeMemcache()
        self.cache_version_off.set(get_cache_key('a'), {'status': 200})
        self.cache_version_off.set(get_cache_key('a', 'c'),
                                   cont_cache_version_off)
        self.cache_version_off.set(
            get_cache_key('a', self.build_container_name('c')),
            {'status': 200})

        self.cache_version_never_on = FakeMemcache()
        self.cache_version_never_on.set(get_cache_key('a'), {'status': 200})
        self.cache_version_never_on.set(get_cache_key('a', 'c'),
                                        {'status': 200})
        self.expected_unread_requests = {}

    def tearDown(self):
        self.assertEqual(self.app.unclosed_requests, {})
        self.assertEqual(self.app.unread_requests,
                         self.expected_unread_requests)

    def call_ov(self, req):
        # authorized gets reset everytime
        self.authorized = []

        def authorize(req):
            self.authorized.append(req)

        if 'swift.authorize' not in req.environ:
            req.environ['swift.authorize'] = authorize

        req.headers.setdefault("User-Agent", "Marula Kruger")

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body_iter = self.lf(req.environ, start_response)
        with utils.closing_if_possible(body_iter):
            body = b''.join(body_iter)

        return status[0], headers[0], body

    def assertRequestEqual(self, req, other):
        self.assertEqual(req.method, other.method)
        self.assertEqual(req.path, other.path)

    def build_container_name(self, cont):
        return get_reserved_name('versions', cont)

    def build_object_name(self, obj, version):
        return get_reserved_name(obj, version)

    def build_symlink_path(self, cont, obj, version):
        cont = self.build_container_name(cont)
        obj = self.build_object_name(obj, version)
        return wsgi_quote(str_to_wsgi("%s/%s" % (cont, obj)))

    def build_versions_path(self, acc='a', cont='c', obj=None, version=None):
        cont = self.build_container_name(cont)
        if not obj:
            return str_to_wsgi("/v1/%s/%s" % (acc, cont))
        obj = self.build_object_name(obj, version)
        return str_to_wsgi("/v1/%s/%s/%s" % (acc, cont, obj))


class ObjectVersioningTestCase(ObjectVersioningBaseTestCase):

    def test_put_container(self):
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('HEAD', '/v1/a/c', swob.HTTPOk, {}, '')
        self.app.register('PUT', self.build_versions_path(), swob.HTTPOk, {},
                          'passed')
        self.app.register('PUT', '/v1/a/c', swob.HTTPAccepted, {}, 'passed')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'PUT'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '202 Accepted')

        # check for sysmeta header
        calls = self.app.calls_with_headers
        self.assertEqual(4, len(calls))
        method, path, headers = calls[3]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c', path)
        self.assertIn(SYSMETA_VERSIONS_CONT, headers)
        self.assertEqual(headers[SYSMETA_VERSIONS_CONT],
                         wsgi_quote(str_to_wsgi(
                             self.build_container_name('c'))))
        self.assertIn(SYSMETA_VERSIONS_ENABLED, headers)
        self.assertEqual(headers[SYSMETA_VERSIONS_ENABLED], 'True')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one', False)])
    def test_same_policy_as_existing_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('GET', '/v1/a/c', swob.HTTPOk, {
                          'x-backend-storage-policy-index': 1}, '')
        self.app.register('PUT', self.build_versions_path(), swob.HTTPOk, {},
                          'passed')
        self.app.register('POST', '/v1/a/c', swob.HTTPNoContent, {}, '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'POST'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')

        # check for sysmeta header
        calls = self.app.calls_with_headers
        self.assertEqual(4, len(calls))
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

        # request to create versions container
        method, path, headers = calls[2]
        self.assertEqual('PUT', method)
        self.assertEqual(self.build_versions_path(), path)
        self.assertIn('X-Storage-Policy', headers)
        self.assertEqual('one', headers['X-Storage-Policy'])

        # request to enable versioning on primary container
        method, path, headers = calls[3]
        self.assertEqual('POST', method)
        self.assertEqual('/v1/a/c', path)
        self.assertIn(SYSMETA_VERSIONS_CONT, headers)
        self.assertEqual(headers[SYSMETA_VERSIONS_CONT],
                         wsgi_quote(str_to_wsgi(
                             self.build_container_name('c'))))
        self.assertIn(SYSMETA_VERSIONS_ENABLED, headers)
        self.assertEqual(headers[SYSMETA_VERSIONS_ENABLED], 'True')

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one', False, is_deprecated=True)])
    def test_existing_container_has_deprecated_policy(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('GET', '/v1/a/c', swob.HTTPOk, {
                          'x-backend-storage-policy-index': 1}, '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'POST'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body,
                         b'Cannot enable object versioning on a container '
                         b'that uses a deprecated storage policy.')

        calls = self.app.calls_with_headers
        self.assertEqual(2, len(calls))
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one', False, is_deprecated=True)])
    def test_existing_container_has_deprecated_policy_unauthed(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('GET', '/v1/a/c', swob.HTTPOk, {
                          'x-backend-storage-policy-index': 1}, '')

        def fake_authorize(req):
            self.authorized.append(req)
            return swob.HTTPForbidden()

        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.authorize': fake_authorize})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '403 Forbidden')

        calls = self.app.calls_with_headers
        self.assertEqual(2, len(calls))
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_same_policy_as_primary_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('GET', '/v1/a/c', swob.HTTPNotFound, {}, '')
        self.app.register('PUT', self.build_versions_path(), swob.HTTPOk,
                          {}, '')
        self.app.register('PUT', '/v1/a/c', swob.HTTPOk, {}, '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true',
                                     'X-Storage-Policy': 'ec42'},
                            environ={'REQUEST_METHOD': 'PUT'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')

        # check for sysmeta header
        calls = self.app.calls_with_headers
        self.assertEqual(4, len(calls))
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

        # request to create versions container
        method, path, headers = calls[2]
        self.assertEqual('PUT', method)
        self.assertEqual(self.build_versions_path(), path)
        self.assertIn('X-Storage-Policy', headers)
        self.assertEqual('ec42', headers['X-Storage-Policy'])

        # request to enable versioning on primary container
        method, path, headers = calls[3]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c', path)
        self.assertIn(SYSMETA_VERSIONS_CONT, headers)
        self.assertEqual(headers[SYSMETA_VERSIONS_CONT],
                         wsgi_quote(str_to_wsgi(
                             self.build_container_name('c'))))
        self.assertIn(SYSMETA_VERSIONS_ENABLED, headers)
        self.assertEqual(headers[SYSMETA_VERSIONS_ENABLED], 'True')
        self.assertIn('X-Storage-Policy', headers)
        self.assertEqual('ec42', headers['X-Storage-Policy'])

    def test_enable_versioning_failed_primary_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, 'passed')
        self.app.register('GET', '/v1/a/c', swob.HTTPNotFound, {}, 'passed')
        self.app.register('PUT', self.build_versions_path(),
                          swob.HTTPOk, {}, 'passed')
        self.app.register('DELETE', self.build_versions_path(),
                          swob.HTTPNoContent, {}, '')
        self.app.register('PUT', '/v1/a/c', swob.HTTPInternalServerError,
                          {}, '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'PUT'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '500 Internal Error')

    def test_enable_versioning_failed_versions_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('GET', '/v1/a/c', swob.HTTPNotFound, {}, '')
        self.app.register('PUT', self.build_versions_path(),
                          swob.HTTPInternalServerError, {}, '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'PUT'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '500 Internal Error')

    def test_enable_versioning_existing_container(self):
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('HEAD', self.build_versions_path(),
                          swob.HTTPOk, {}, '')
        self.app.register('PUT', self.build_versions_path(),
                          swob.HTTPAccepted, {}, '')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: False},
            'passed')
        self.app.register('POST', '/v1/a/c', swob.HTTPOk, {}, 'passed')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'POST'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')

        # check for sysmeta header
        calls = self.app.calls_with_headers
        self.assertEqual(5, len(calls))
        method, path, req_headers = calls[-1]
        self.assertEqual('POST', method)
        self.assertEqual('/v1/a/c', path)
        self.assertIn(SYSMETA_VERSIONS_ENABLED, req_headers)
        self.assertEqual(req_headers[SYSMETA_VERSIONS_ENABLED],
                         'True')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_put_container_with_legacy_versioning(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {'x-container-sysmeta-versions-location': 'ver_cont'},
            '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'POST'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')

    def test_put_container_with_super_legacy_versioning(self):
        # x-versions-location was used before versioned writes
        # was pulled out to middleware
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {'x-versions-location': 'ver_cont'},
            '')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'true'},
                            environ={'REQUEST_METHOD': 'POST'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')

    def test_get_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True}, b'[]')
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_get_reserved_container_passthrough(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, 'passed')
        self.app.register('GET', '/v1/a/%s' % get_reserved_name('foo'),
                          swob.HTTPOk, {}, b'[]')
        req = Request.blank('/v1/a/%s' % get_reserved_name('foo'))
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_head_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True}, None)
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_delete_container_success(self):
        self.app.register(
            'DELETE', '/v1/a/c', swob.HTTPNoContent, {}, '')
        self.app.register(
            'DELETE', self.build_versions_path(),
            swob.HTTPNoContent, {}, '')
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True}, '')
        self.app.register(
            'HEAD', self.build_versions_path(), swob.HTTPOk,
            {'x-container-object-count': 0}, '')
        req = Request.blank(
            '/v1/a/c', environ={'REQUEST_METHOD': 'DELETE'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a'),
            ('HEAD', '/v1/a/c'),
            ('HEAD', self.build_versions_path()),
            ('HEAD', self.build_versions_path()),  # get_container_info
            ('DELETE', self.build_versions_path()),
            ('DELETE', '/v1/a/c'),
        ])

    def test_delete_container_fail_object_count(self):
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: False}, '')
        self.app.register(
            'HEAD',
            self.build_versions_path(),
            swob.HTTPOk,
            {'x-container-object-count': 1}, '')
        req = Request.blank(
            '/v1/a/c', environ={'REQUEST_METHOD': 'DELETE'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '409 Conflict')
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a'),
            ('HEAD', '/v1/a/c'),
            ('HEAD', self.build_versions_path()),
            ('HEAD', self.build_versions_path()),  # get_container_info
        ])

    def test_delete_container_fail_delete_versions_cont(self):
        # N.B.: Notice lack of a call to DELETE /v1/a/c
        # Since deleting versions container failed, swift should
        # not delete primary container
        self.app.register(
            'DELETE', self.build_versions_path(),
            swob.HTTPServerError, {}, '')
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: False}, '')
        self.app.register(
            'HEAD', self.build_versions_path(), swob.HTTPOk,
            {'x-container-object-count': 0}, '')
        req = Request.blank(
            '/v1/a/c', environ={'REQUEST_METHOD': 'DELETE'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '500 Internal Error')
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a'),
            ('HEAD', '/v1/a/c'),
            ('HEAD', self.build_versions_path()),
            ('HEAD', self.build_versions_path()),  # get_container_info
            ('DELETE', self.build_versions_path()),
        ])

    def test_get(self):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk, {
                'Content-Location': self.build_versions_path(
                    obj='o', version='9999998765.99999')},
            'body')
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertIn(('X-Object-Version-Id', '0000001234.00000'), headers)
        self.assertIn(
            ('Content-Location', '/v1/a/c/o?version-id=0000001234.00000'),
            headers)

    def test_get_symlink(self):
        self.app.register(
            'GET', '/v1/a/c/o?symlink=get', swob.HTTPOk, {
                'X-Symlink-Target': '%s/%s' % (
                    self.build_container_name('c'),
                    self.build_object_name('o', '9999998765.99999'),
                ),
                'X-Symlink-Target-Etag': 'versioned-obj-etag',
            }, '')
        req = Request.blank(
            '/v1/a/c/o?symlink=get',
            environ={'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertIn(('X-Object-Version-Id', '0000001234.00000'), headers)
        self.assertIn(
            ('X-Symlink-Target', 'c/o?version-id=0000001234.00000'),
            headers)
        self.assertEqual(body, b'')
        # N.B. HEAD req already works with existing registered GET response
        req = Request.blank(
            '/v1/a/c/o?symlink=get', method='HEAD',
            environ={'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertIn(('X-Object-Version-Id', '0000001234.00000'), headers)
        self.assertIn(
            ('X-Symlink-Target', 'c/o?version-id=0000001234.00000'),
            headers)
        self.assertEqual(body, b'')

    def test_put_object_no_versioning(self):
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        cache = FakeMemcache()
        cache.set(get_cache_key('a'), {'status': 200})
        cache.set(get_cache_key('a', 'c'), {'status': 200})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite(self, mock_time):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: 'c-unique/whatever'}, '')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = 'stuff' * 100
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=put_body,
            headers={'Content-Type': 'text/plain',
                     'ETag': md5(
                         put_body.encode('utf8'),
                         usedforsecurity=False).hexdigest(),
                     'Content-Length': len(put_body)},
            environ={'swift.cache': self.cache_version_on,
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 2)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(['OV', 'OV', 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT', self.build_versions_path(
                obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

        symlink_expected_headers = {
            SYMLOOP_EXTEND: 'true',
            ALLOW_RESERVED_NAMES: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                put_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(put_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in symlink_expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    def test_POST(self):
        self.app.register(
            'POST',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPAccepted, {}, '')
        self.app.register(
            'POST', '/v1/a/c/o', swob.HTTPTemporaryRedirect, {
                SYSMETA_VERSIONS_SYMLINK: 'true',
                'Location': self.build_versions_path(
                    obj='o', version='9999998765.99999')}, '')

        # TODO: in symlink middleware, swift.leave_relative_location
        # is added by the middleware during the response
        # adding to the client request here, need to understand how
        # to modify the response environ.
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/jibberish01',
                     'X-Object-Meta-Foo': 'bar'},
            environ={'swift.cache': self.cache_version_on,
                     'swift.leave_relative_location': 'true',
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '202 Accepted')

        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual([None, 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual(self.app.calls, [
            ('POST', '/v1/a/c/o'),
            ('POST', self.build_versions_path(
                obj='o', version='9999998765.99999')),
        ])

        expected_hdrs = {
            'content-type': 'text/jibberish01',
            'x-object-meta-foo': 'bar',
        }
        version_obj_post_headers = self.app.call_list[1].headers
        for k, v in expected_hdrs.items():
            self.assertEqual(version_obj_post_headers[k], v)

    def test_POST_mismatched_location(self):
        # This is a defensive chech, ideally a mistmached
        # versions container should never happen.
        self.app.register(
            'POST', '/v1/a/c/o', swob.HTTPTemporaryRedirect, {
                SYSMETA_VERSIONS_SYMLINK: 'true',
                'Location': self.build_versions_path(
                    cont='mismatched', obj='o', version='9999998765.99999')},
                '')

        # TODO: in symlink middleware, swift.leave_relative_location
        # is added by the middleware during the response
        # adding to the client request here, need to understand how
        # to modify the response environ.
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/jibberish01',
                     'X-Object-Meta-Foo': 'bar'},
            environ={'swift.cache': self.cache_version_on,
                     'swift.leave_relative_location': 'true',
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '307 Temporary Redirect')

        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual([None], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual(self.app.calls, [
            ('POST', '/v1/a/c/o'),
        ])

    def test_POST_regular_symlink(self):
        self.app.register(
            'POST', '/v1/a/c/o', swob.HTTPTemporaryRedirect, {
                'Location': '/v1/a/t/o'}, '')

        # TODO: in symlink middleware, swift.leave_relative_location
        # is added by the middleware during the response
        # adding to the client request here, need to understand how
        # to modify the response environ.
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/jibberish01',
                     'X-Object-Meta-Foo': 'bar'},
            environ={'swift.cache': self.cache_version_on,
                     'swift.leave_relative_location': 'true',
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '307 Temporary Redirect')

        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual([None], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual(self.app.calls, [
            ('POST', '/v1/a/c/o'),
        ])

    def test_denied_PUT_of_versioned_object(self):
        authorize_call = []
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:00:01 GMT'}, 'passed')

        def fake_authorize(req):
            # we should deny the object PUT
            authorize_call.append(req)
            return swob.HTTPForbidden()

        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT',
                     'swift.cache': self.cache_version_on,
                     'swift.authorize': fake_authorize,
                     'CONTENT_LENGTH': '0'})
        # Save off a copy, as the middleware may modify the original
        expected_req = Request(req.environ.copy())
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '403 Forbidden')
        self.assertEqual(len(authorize_call), 1)
        self.assertRequestEqual(expected_req, authorize_call[0])

        self.assertEqual(self.app.calls, [])

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite_tombstone(self, mock_time):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPNotFound, {}, None)
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = 'stuff' * 100
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=put_body,
            headers={'Content-Type': 'text/plain',
                     'ETag': md5(
                         put_body.encode('utf8'),
                         usedforsecurity=False).hexdigest(),
                     'Content-Length': len(put_body)},
            environ={'swift.cache': self.cache_version_on,
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        # authorized twice because of pre-flight check on PUT
        self.assertEqual(len(self.authorized), 2)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(['OV', 'OV', 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT', self.build_versions_path(
                obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                put_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(put_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite_object_with_DLO(self, mock_time):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT'}, 'old version')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = ''
        req = Request.blank('/v1/a/c/o', method='PUT', body=put_body,
                            headers={'Content-Type': 'text/plain',
                                     'X-Object-Manifest': 'req/manifest'},
                            environ={'swift.cache': self.cache_version_on,
                                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual(4, self.app.call_count)
        self.assertEqual(['OV', 'OV', 'OV', 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))

        self.assertEqual([
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999999939.99999')),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ], self.app.calls)

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

        self.assertNotIn('x-object-manifest', calls[1].headers)
        self.assertEqual('req/manifest',
                         calls[-2].headers['X-Object-Manifest'])

        symlink_put_headers = calls[-1].headers
        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                put_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(put_body)),
        }
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)
        self.assertNotIn('x-object-manifest', symlink_put_headers)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite_DLO_with_object(self, mock_time):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'X-Object-Manifest': 'resp/manifest',
                           'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT'},
                          'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = 'stuff' * 100
        req = Request.blank('/v1/a/c/o', method='PUT', body=put_body,
                            headers={'Content-Type': 'text/plain'},
                            environ={'swift.cache': self.cache_version_on,
                                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual(4, self.app.call_count)
        self.assertEqual(['OV', 'OV', 'OV', 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual([
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999999939.99999')),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ], self.app.calls)

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

        self.assertEqual('resp/manifest',
                         calls[1].headers['X-Object-Manifest'])
        self.assertNotIn(TGT_OBJ_SYSMETA_SYMLINK_HDR,
                         calls[1].headers)

        self.assertNotIn('x-object-manifest', calls[2].headers)
        self.assertNotIn(TGT_OBJ_SYSMETA_SYMLINK_HDR,
                         calls[2].headers)

        symlink_put_headers = calls[-1].headers
        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                put_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(put_body)),
        }
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)
        self.assertNotIn('x-object-manifest', symlink_put_headers)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite_SLO_with_object(self, mock_time):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            'X-Static-Large-Object': 'True',
            # N.B. object-sever strips swift_bytes
            'Content-Type': 'application/octet-stream',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
            '656516af0f7474b857857dd2a327f3b9; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88',
            'X-Object-Sysmeta-Slo-Etag': '71e938d37c1d06dc634dd24660255a88',
            'X-Object-Sysmeta-Slo-Size': '10485760',
            'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT',
        }, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = 'stuff' * 100
        req = Request.blank('/v1/a/c/o', method='PUT', body=put_body,
                            headers={'Content-Type': 'text/plain'},
                            environ={'swift.cache': self.cache_version_on,
                                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual(4, self.app.call_count)
        self.assertEqual(['OV', 'OV', 'OV', 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual([
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999999939.99999')),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ], self.app.calls)

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

        slo_headers = {
            'X-Static-Large-Object': 'True',
            'Content-Type': 'application/octet-stream; swift_bytes=10485760',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
            '656516af0f7474b857857dd2a327f3b9; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88',
            'X-Object-Sysmeta-Slo-Etag': '71e938d37c1d06dc634dd24660255a88',
            'X-Object-Sysmeta-Slo-Size': '10485760',
        }
        archive_put = calls[1]
        for key, value in slo_headers.items():
            self.assertEqual(archive_put.headers[key], value)

        client_put = calls[2]
        for key in slo_headers:
            if key == 'Content-Type':
                self.assertEqual('text/plain', client_put.headers[key])
            else:
                self.assertNotIn(key, client_put.headers)

        symlink_put_headers = calls[-1].headers
        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                put_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(put_body)),
        }
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)
        self.assertNotIn('x-object-manifest', symlink_put_headers)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite_object(self, mock_time):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT'}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')

        put_body = 'stuff' * 100
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=put_body,
            headers={'Content-Type': 'text/plain',
                     'ETag': md5(
                         put_body.encode('utf8'),
                         usedforsecurity=False).hexdigest(),
                     'Content-Length': len(put_body)},
            environ={'swift.cache': self.cache_version_on,
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        # authorized twice because of pre-flight check on PUT
        self.assertEqual(len(self.authorized), 2)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(['OV', 'OV', 'OV', 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999999939.99999')),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                put_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(put_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    def test_new_version_get_errors(self):
        # GET on source fails, expect client error response,
        # no PUT should happen
        self.app.register('GET', '/v1/a/c/o',
                          swob.HTTPBadRequest, {}, None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            environ={'swift.cache': self.cache_version_on,
                                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(1, self.app.call_count)

        # GET on source fails, expect server error response
        self.app.register('GET', '/v1/a/c/o',
                          swob.HTTPBadGateway, {}, None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            environ={'swift.cache': self.cache_version_on,
                                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '503 Service Unavailable')
        self.assertEqual(2, self.app.call_count)

    def test_new_version_put_errors(self):
        # PUT of version fails, expect client error response
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT'}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPUnauthorized, {}, None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            environ={'swift.cache': self.cache_version_on,
                                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '401 Unauthorized')
        self.assertEqual(2, self.app.call_count)

        # PUT of version fails, expect server error response
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPBadGateway, {}, None)
        req = Request.blank(
            '/v1/a/c/o', headers={'Content-Type': 'text/plain'},
            environ={'REQUEST_METHOD': 'PUT',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '503 Service Unavailable')
        self.assertEqual(4, self.app.call_count)

        # PUT fails because the reserved container is missing; server error
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNotFound, {}, None)
        req = Request.blank(
            '/v1/a/c/o', headers={'Content-Type': 'text/plain'},
            environ={'REQUEST_METHOD': 'PUT',
                     'swift.cache': self.cache_version_on_but_busted,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '500 Internal Error')
        self.assertIn(b'container does not exist', body)
        self.assertIn(b're-enable object versioning', body)


class ObjectVersioningTestDisabled(ObjectVersioningBaseTestCase):
    def test_get_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: 'c\x01versions',
             SYSMETA_VERSIONS_ENABLED: False}, b'[]')
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'False'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_head_container(self):
        self.app.register('GET', '/v1/a', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: 'c\x01versions',
             SYSMETA_VERSIONS_ENABLED: False}, None)
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'False'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_disable_versioning(self):
        self.app.register('POST', '/v1/a/c', swob.HTTPOk, {}, 'passed')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Enabled': 'false'},
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_overwrite_null_marker_versioning_disabled(self, mock_time):
        # During object PUT with a versioning disabled, if the most
        # recent versioned object is a DELETE marker will a *null*
        # version-id, then the DELETE marker should be removed.
        listing_body = [{
            "hash": "y",
            "last_modified": "2014-11-21T14:23:02.206740",
            "bytes": 0,
            "name": self.build_object_name('o', '0000000001.00000'),
            "content_type": "application/x-deleted;swift_versions_deleted=1"
        }, {
            "hash": "x",
            "last_modified": "2014-11-21T14:14:27.409100",
            "bytes": 3,
            "name": self.build_object_name('o', '0000000002.00000'),
            "content_type": "text/plain"
        }]
        prefix_listing_path = \
            '/v1/a/c\x01versions?prefix=o---&marker='
        self.app.register(
            'GET', prefix_listing_path, swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))
        self.app.register(
            'HEAD',
            self.build_versions_path(obj='o', version='0000000001.00000'),
            swob.HTTPNoContent,
            {'content-type': DELETE_MARKER_CONTENT_TYPE}, None)
        self.app.register(
            'DELETE',
            self.build_versions_path(obj='o', version='0000000001.00000'),
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = 'stuff' * 100
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=put_body,
            headers={'Content-Type': 'text/plain',
                     'ETag': md5(
                         put_body.encode('utf8'),
                         usedforsecurity=False).hexdigest(),
                     'Content-Length': len(put_body)},
            environ={'swift.cache': self.cache_version_off,
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')

        # authorized twice because of pre-flight check on PUT
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        # TODO self.assertEqual(['OV', None, 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))

        self.assertEqual(self.app.calls, [
            ('PUT', '/v1/a/c/o'),
        ])

        obj_put_headers = self.app.call_list[-1].headers
        self.assertNotIn(SYSMETA_VERSIONS_SYMLINK, obj_put_headers)

    def test_put_object_versioning_disabled(self):
        listing_body = [{
            "hash": "x",
            "last_modified": "2014-11-21T14:14:27.409100",
            "bytes": 3,
            "name": self.build_object_name('o', '0000000001.00000'),
            "content_type": "text/plain"
        }]
        prefix_listing_path = \
            '/v1/a/c\x01versions?prefix=o---&marker='
        self.app.register(
            'GET', prefix_listing_path, swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT',
                     'swift.cache': self.cache_version_off,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

        self.assertEqual(self.app.calls, [
            ('PUT', '/v1/a/c/o'),
        ])
        obj_put_headers = self.app.call_list[-1].headers
        self.assertNotIn(SYSMETA_VERSIONS_SYMLINK, obj_put_headers)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_PUT_with_recent_versioned_marker_versioning_disabled(self,
                                                                  mock_time):
        # During object PUT with a versioning disabled, if the most
        # recent versioned object is a DELETE marker will a non-null
        # version-id, then the DELETE marker should not be removed.
        listing_body = [{
            "hash": "y",
            "last_modified": "2014-11-21T14:23:02.206740",
            "bytes": 0,
            "name": self.build_object_name('o', '0000000001.00000'),
            "content_type": "application/x-deleted;swift_versions_deleted=1"
        }, {
            "hash": "x",
            "last_modified": "2014-11-21T14:14:27.409100",
            "bytes": 3,
            "name": self.build_object_name('o', '0000000002.00000'),
            "content_type": "text/plain"
        }]
        prefix_listing_path = \
            '/v1/a/c\x01versions?prefix=o---&marker='
        self.app.register(
            'GET', prefix_listing_path, swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))
        self.app.register(
            'HEAD',
            self.build_versions_path(obj='o', version='0000000001.00000'),
            swob.HTTPNoContent,
            {'content-type': DELETE_MARKER_CONTENT_TYPE}, None)
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        put_body = 'stuff' * 100
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=put_body,
            headers={'Content-Type': 'text/plain',
                     'ETag': md5(
                         put_body.encode('utf8'),
                         usedforsecurity=False).hexdigest(),
                     'Content-Length': len(put_body)},
            environ={'swift.cache': self.cache_version_off,
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')

        # authorized twice because of pre-flight check on PUT
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        # TODO self.assertEqual(['OV', None, 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))

        self.assertEqual(self.app.calls, [
            ('PUT', '/v1/a/c/o'),
        ])

        obj_put_headers = self.app.call_list[-1].headers
        self.assertNotIn(SYSMETA_VERSIONS_SYMLINK, obj_put_headers)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_delete_object_with_versioning_disabled(self, mock_time):
        # When versioning is disabled, swift will simply issue the
        # original request to the versioned container
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, 'passed')
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': self.cache_version_off})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_POST_symlink(self):
        self.app.register(
            'POST',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPAccepted, {}, '')
        self.app.register(
            'POST', '/v1/a/c/o', swob.HTTPTemporaryRedirect, {
                SYSMETA_VERSIONS_SYMLINK: 'true',
                'Location': self.build_versions_path(
                    obj='o', version='9999998765.99999')}, '')

        # TODO: in symlink middleware, swift.leave_relative_location
        # is added by the middleware during the response
        # adding to the client request here, need to understand how
        # to modify the response environ.
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/jibberish01',
                     'X-Object-Meta-Foo': 'bar'},
            environ={'swift.cache': self.cache_version_off,
                     'swift.leave_relative_location': 'true',
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '202 Accepted')

        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual([None, 'OV'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual(self.app.calls, [
            ('POST', '/v1/a/c/o'),
            ('POST',
             self.build_versions_path(obj='o', version='9999998765.99999')),
        ])

        expected_hdrs = {
            'content-type': 'text/jibberish01',
            'x-object-meta-foo': 'bar',
        }
        version_obj_post_headers = self.app.call_list[1].headers
        for k, v in expected_hdrs.items():
            self.assertEqual(version_obj_post_headers[k], v)

    def test_POST_unversioned_obj(self):
        self.app.register(
            'POST', '/v1/a/c/o', swob.HTTPAccepted, {}, '')

        # TODO: in symlink middleware, swift.leave_relative_location
        # is added by the middleware during the response
        # adding to the client request here, need to understand how
        # to modify the response environ.
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/jibberish01',
                     'X-Object-Meta-Foo': 'bar'},
            environ={'swift.cache': self.cache_version_off,
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '202 Accepted')

        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual([None], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        self.assertEqual(self.app.calls, [
            ('POST', '/v1/a/c/o'),
        ])

        expected_hdrs = {
            'content-type': 'text/jibberish01',
            'x-object-meta-foo': 'bar',
        }
        version_obj_post_headers = self.app.call_list[0].headers
        for k, v in expected_hdrs.items():
            self.assertEqual(version_obj_post_headers[k], v)


class ObjectVersioningTestDelete(ObjectVersioningBaseTestCase):
    def test_delete_object_with_versioning_never_enabled(self):
        # should be a straight DELETE, versioning middleware
        # does not get involved.
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, 'passed')
        cache = FakeMemcache()
        cache.set(get_cache_key('a'), {'status': 200})
        cache.set(get_cache_key('a', 'c'), {'status': 200})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': cache})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        called_method = [call.method for call in self.app.call_list]
        self.assertNotIn('PUT', called_method)
        self.assertNotIn('GET', called_method)
        self.assertEqual(1, self.app.call_count)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_put_delete_marker_no_object_success(self, mock_time):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPNotFound,
            {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNotFound, {}, None)

        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(len(self.authorized), 2)

        req.environ['REQUEST_METHOD'] = 'PUT'
        self.assertRequestEqual(req, self.authorized[0])

        calls = self.app.call_list
        self.assertEqual(['GET', 'PUT', 'DELETE'], [c.method for c in calls])
        self.assertEqual('application/x-deleted;swift_versions_deleted=1',
                         calls[1].headers.get('Content-Type'))

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_delete_marker_over_object_success(self, mock_time):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT'}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, None)

        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(b'', body)
        self.assertEqual(len(self.authorized), 2)

        req.environ['REQUEST_METHOD'] = 'PUT'
        self.assertRequestEqual(req, self.authorized[0])

        calls = self.app.call_list
        self.assertEqual(['GET', 'PUT', 'PUT', 'DELETE'],
                         [c.method for c in calls])
        self.assertEqual(
            self.build_versions_path(obj='o', version='9999999939.99999'),
            calls[1].path)
        self.assertEqual('application/x-deleted;swift_versions_deleted=1',
                         calls[2].headers.get('Content-Type'))

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_delete_marker_over_versioned_object_success(self, mock_time):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {SYSMETA_VERSIONS_SYMLINK: 'true'}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, None)

        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(b'', body)
        self.assertEqual(len(self.authorized), 2)

        req.environ['REQUEST_METHOD'] = 'PUT'
        self.assertRequestEqual(req, self.authorized[0])

        calls = self.app.call_list
        self.assertEqual(['GET', 'PUT', 'DELETE'],
                         [c.method for c in calls])
        self.assertEqual(
            self.build_versions_path(obj='o', version='9999998765.99999'),
            calls[1].path)
        self.assertEqual('application/x-deleted;swift_versions_deleted=1',
                         calls[1].headers.get('Content-Type'))

    def test_denied_DELETE_of_versioned_object(self):
        authorize_call = []

        def fake_authorize(req):
            authorize_call.append((req.method, req.path))
            return swob.HTTPForbidden()

        req = Request.blank('/v1/a/c/o', method='DELETE', body='',
                            headers={'X-If-Delete-At': 1},
                            environ={'swift.cache': self.cache_version_on,
                                     'swift.authorize': fake_authorize,
                                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '403 Forbidden')
        self.assertEqual(len(authorize_call), 1)
        self.assertEqual(('DELETE', '/v1/a/c/o'), authorize_call[0])


class ObjectVersioningTestCopy(ObjectVersioningBaseTestCase):
    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_COPY_overwrite_tombstone(self, mock_time):
        self.cache_version_on.set(get_cache_key('a', 'src_cont'),
                                  {'status': 200})
        src_body = 'stuff' * 100
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPNotFound, {}, None)
        self.app.register(
            'GET', '/v1/a/src_cont/src_obj', swob.HTTPOk, {}, src_body)
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        req = Request.blank(
            '/v1/a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'c/o'})

        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 3)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/src_cont/src_obj'),
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                src_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(src_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_COPY_overwrite_object(self, mock_time):
        self.cache_version_on.set(get_cache_key('a', 'src_cont'),
                                  {'status': 200})
        src_body = 'stuff' * 100
        self.app.register(
            'GET', '/v1/a/src_cont/src_obj', swob.HTTPOk, {}, src_body)
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:01:00 GMT'}, 'old object')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        req = Request.blank(
            '/v1/a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'c/o'})

        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 3)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/src_cont/src_obj'),
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999999939.99999')),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                src_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(src_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_COPY_overwrite_version_symlink(self, mock_time):
        self.cache_version_on.set(get_cache_key('a', 'src_cont'),
                                  {'status': 200})
        src_body = 'stuff' * 100
        self.app.register(
            'GET', '/v1/a/src_cont/src_obj', swob.HTTPOk, {}, src_body)
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: 'c-unique/whatever'}, '')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        req = Request.blank(
            '/v1/a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'c/o'})

        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 3)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/src_cont/src_obj'),
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                src_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(src_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    @mock.patch('swift.common.middleware.versioned_writes.object_versioning.'
                'time.time', return_value=1234)
    def test_copy_new_version_different_account(self, mock_time):
        self.cache_version_on.set(get_cache_key('src_acc'),
                                  {'status': 200})
        self.cache_version_on.set(get_cache_key('src_acc', 'src_cont'),
                                  {'status': 200})
        src_body = 'stuff' * 100
        self.app.register(
            'GET', '/v1/src_acc/src_cont/src_obj', swob.HTTPOk, {}, src_body)
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: 'c-unique/whatever'}, '')
        self.app.register(
            'PUT',
            self.build_versions_path(obj='o', version='9999998765.99999'),
            swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        req = Request.blank(
            '/v1/src_acc/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY',
                     'swift.cache': self.cache_version_on,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'c/o',
                     'Destination-Account': 'a'})

        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 3)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/src_acc/src_cont/src_obj'),
            ('GET', '/v1/a/c/o?symlink=get'),
            ('PUT',
             self.build_versions_path(obj='o', version='9999998765.99999')),
            ('PUT', '/v1/a/c/o'),
        ])

        expected_headers = {
            TGT_OBJ_SYSMETA_SYMLINK_HDR:
            self.build_symlink_path('c', 'o', '9999998765.99999'),
            'x-object-sysmeta-symlink-target-etag': md5(
                src_body.encode('utf8'), usedforsecurity=False).hexdigest(),
            'x-object-sysmeta-symlink-target-bytes': str(len(src_body)),
        }
        symlink_put_headers = self.app.call_list[-1].headers
        for k, v in expected_headers.items():
            self.assertEqual(symlink_put_headers[k], v)

    def test_copy_object_versioning_disabled(self):
        self.cache_version_off.set(get_cache_key('a', 'src_cont'),
                                   {'status': 200})
        listing_body = [{
            "hash": "x",
            "last_modified": "2014-11-21T14:14:27.409100",
            "bytes": 3,
            "name": self.build_object_name('o', '0000000001.00000'),
            "content_type": "text/plain"
        }]
        prefix_listing_path = \
            '/v1/a/c\x01versions?prefix=o---&marker='
        self.app.register(
            'GET', prefix_listing_path, swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))
        src_body = 'stuff' * 100
        self.app.register(
            'GET', '/v1/a/src_cont/src_obj', swob.HTTPOk, {}, src_body)
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        req = Request.blank(
            '/v1/a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY',
                     'swift.cache': self.cache_version_off,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'c/o'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 2)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/src_cont/src_obj'),
            ('PUT', '/v1/a/c/o'),
        ])
        obj_put_headers = self.app.call_list[-1].headers
        self.assertNotIn(SYSMETA_VERSIONS_SYMLINK, obj_put_headers)


class ObjectVersioningTestVersionAPI(ObjectVersioningBaseTestCase):

    def test_fail_non_versioned_container(self):
        self.app.register('HEAD', '/v1/a', swob.HTTPOk, {}, '')
        self.app.register('HEAD', '/v1/a/c', swob.HTTPOk, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b'version-aware operations require'
                         b' that the container is versioned')

    def test_PUT_version(self):
        timestamp = next(self.ts)
        version_path = '%s?symlink=get' % self.build_versions_path(
            obj='o', version=(~timestamp).normal)
        etag = md5(b'old-version-etag', usedforsecurity=False).hexdigest()
        self.app.register('HEAD', version_path, swob.HTTPNoContent, {
            'Content-Length': 10,
            'Content-Type': 'application/old-version',
            'ETag': etag,
        }, '')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=b'',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': timestamp.normal})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(self.app.calls, [
            ('HEAD', version_path),
            ('PUT', '/v1/a/c/o?version-id=%s' % timestamp.normal),
        ])
        obj_put_headers = self.app.call_list[-1].headers
        symlink_expected_headers = {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: self.build_symlink_path(
                'c', 'o', (~timestamp).normal),
            'x-object-sysmeta-symlink-target-etag': etag,
            'x-object-sysmeta-symlink-target-bytes': '10',
        }
        for k, v in symlink_expected_headers.items():
            self.assertEqual(obj_put_headers[k], v)

    def test_PUT_version_with_non_empty_body(self):
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body='foo',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '1'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')

        req = Request.blank(
            '/v1/a/c/o', method='PUT', body='foo',
            environ={'swift.cache': self.cache_version_on,
                     'HTTP_TRANSFER_ENCODING': 'chunked',
                     'CONTENT_LENGTH': None},
            params={'version-id': '1'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')

    def test_PUT_version_with_no_length_or_encoding(self):
        req = Request.blank(
            '/v1/a/c/o', method='PUT',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '1'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '411 Length Required')

    def test_PUT_version_not_found(self):
        timestamp = next(self.ts)
        version_path = '%s?symlink=get' % self.build_versions_path(
            obj='o', version=(~timestamp).normal)
        self.app.register('HEAD', version_path, swob.HTTPNotFound, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=b'',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': timestamp.normal})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '404 Not Found')
        self.assertIn(b'version does not exist', body)

    def test_PUT_version_container_not_found(self):
        timestamp = next(self.ts)
        version_path = '%s?symlink=get' % self.build_versions_path(
            obj='o', version=(~timestamp).normal)
        self.app.register('HEAD', version_path, swob.HTTPNotFound, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=b'',
            environ={'swift.cache': self.cache_version_on_but_busted},
            params={'version-id': timestamp.normal})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '500 Internal Error')
        self.assertIn(b'container does not exist', body)
        self.assertIn(b're-enable object versioning', body)

    def test_PUT_version_invalid(self):
        invalid_versions = ('null', 'something', '-10')
        for version_id in invalid_versions:
            req = Request.blank(
                '/v1/a/c/o', method='PUT',
                environ={'swift.cache': self.cache_version_on},
                params={'version-id': invalid_versions})
            status, headers, body = self.call_ov(req)
            self.assertEqual(status, '400 Bad Request')

    def test_POST_error(self):
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/plain',
                     'X-Object-Meta-foo': 'bar'},
            environ={'swift.cache': self.cache_version_on,
                     'swift.trans_id': 'fake_trans_id'},
            params={'version-id': '1'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')

    def test_GET_and_HEAD(self):
        self.app.register(
            'GET',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPOk, {}, 'foobar')
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Object-Version-Id', '0000000060.00000'),
                      headers)
        self.assertEqual(b'foobar', body)
        # HEAD with same params find same registered GET
        req = Request.blank(
            '/v1/a/c/o', method='HEAD',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Object-Version-Id', '0000000060.00000'),
                      headers)
        self.assertEqual(b'', body)

    def test_GET_404(self):
        self.app.register(
            'GET',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNotFound, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '404 Not Found')
        self.assertNotIn(('X-Object-Version-Id', '0000000060.00000'),
                         headers)

    def test_HEAD(self):
        self.app.register(
            'HEAD',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPOk, {
                'X-Object-Meta-Foo': 'bar'},
            '')
        req = Request.blank(
            '/v1/a/c/o', method='HEAD',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertIn(('X-Object-Version-Id', '0000000060.00000'),
                      headers)
        self.assertIn(('X-Object-Meta-Foo', 'bar'), headers)

    def test_GET_null_id(self):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk, {}, 'foobar')
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        # N.B. GET w/ query param found registered raw_path GET
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(1, len(self.authorized))
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(1, len(self.app.calls))
        self.assertIn(('X-Object-Version-Id', 'null'), headers)
        self.assertEqual(b'foobar', body)
        # and HEAD w/ same params finds same registered GET
        req = Request.blank(
            '/v1/a/c/o?version-id=null', method='HEAD',
            environ={'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(2, len(self.app.calls))
        self.assertIn(('X-Object-Version-Id', 'null'), headers)
        self.assertEqual(b'', body)

    def test_GET_null_id_versioned_obj(self):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk, {
                'Content-Location': self.build_versions_path(
                    obj='o', version='9999998765.99999')},
            '')
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(1, len(self.authorized))
        self.assertEqual(1, len(self.app.calls))
        self.assertNotIn(('X-Object-Version-Id', '0000001234.00000'), headers)
        # This will log a 499 but (at the moment, anyway)
        # we don't have a good way to avoid it
        self.expected_unread_requests[('GET', '/v1/a/c/o?version-id=null')] = 1

    def test_GET_null_id_404(self):
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPNotFound, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(1, len(self.authorized))
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(1, len(self.app.calls))
        self.assertNotIn(('X-Object-Version-Id', 'null'), headers)
        # and HEAD w/ same params finds same registered GET
        # we have test_HEAD_null_id, the following test is meant to illustrate
        # that FakeSwift works for HEADs even if only GETs are registered.
        req = Request.blank(
            '/v1/a/c/o', method='HEAD',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(1, len(self.authorized))
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(2, len(self.app.calls))
        self.assertNotIn(('X-Object-Version-Id', 'null'), headers)

    def test_HEAD_null_id(self):
        self.app.register(
            'HEAD', '/v1/a/c/o', swob.HTTPOk, {'X-Object-Meta-Foo': 'bar'}, '')
        req = Request.blank(
            '/v1/a/c/o', method='HEAD',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(1, len(self.authorized))
        self.assertEqual(1, len(self.app.calls))
        self.assertIn(('X-Object-Version-Id', 'null'), headers)
        self.assertIn(('X-Object-Meta-Foo', 'bar'), headers)
        # N.B. GET on explicitly registered HEAD raised KeyError
        req = Request.blank(
            '/v1/a/c/o', method='GET',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        with self.assertRaises(KeyError):
            status, headers, body = self.call_ov(req)

    def test_HEAD_delete_marker(self):
        self.app.register(
            'HEAD',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPOk, {
                'content-type':
                'application/x-deleted;swift_versions_deleted=1'},
            '')
        req = Request.blank(
            '/v1/a/c/o', method='HEAD',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)

        # a HEAD/GET of a delete-marker returns a 404
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(len(self.authorized), 1)
        self.assertIn(('X-Object-Version-Id', '0000000060.00000'),
                      headers)

    def test_DELETE_not_current_version(self):
        # This tests when version-id does not point to the
        # current version, in this case, there's no need to
        # re-link symlink
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk, {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: self.build_symlink_path(
                'c', 'o', '9999999940.99999')}, '')
        self.app.register(
            'DELETE',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNoContent, {}, 'foobar')
        req = Request.blank(
            '/v1/a/c/o', method='DELETE',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual('0000000059.00000',
                         dict(headers)['X-Object-Current-Version-Id'])
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a/c/o?symlink=get'),
            ('DELETE',
             '%s?version-id=0000000060.00000' % self.build_versions_path(
                 obj='o', version='9999999939.99999')),
        ])

        calls = self.app.call_list
        self.assertIn('X-Newest', calls[0].headers)
        self.assertEqual('True', calls[0].headers['X-Newest'])

    def test_DELETE_current_version(self):
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk, {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: self.build_symlink_path(
                'c', 'o', '9999999939.99999')}, '')
        self.app.register(
            'DELETE',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNoContent, {}, '')
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='DELETE',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual('null',
                         dict(headers)['X-Object-Current-Version-Id'])
        self.assertEqual('0000000060.00000',
                         dict(headers)['X-Object-Version-Id'])
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a/c/o?symlink=get'),
            ('DELETE', '/v1/a/c/o'),
            ('DELETE',
             self.build_versions_path(obj='o', version='9999999939.99999')),
        ])

    def test_DELETE_current_version_is_delete_marker(self):
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPNotFound, {}, '')
        self.app.register(
            'DELETE',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNoContent, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='DELETE',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual('null',
                         dict(headers)['X-Object-Current-Version-Id'])
        self.assertEqual('0000000060.00000',
                         dict(headers)['X-Object-Version-Id'])
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a/c/o?symlink=get'),
            ('DELETE',
             '%s?version-id=0000000060.00000' % self.build_versions_path(
                 obj='o', version='9999999939.99999')),
        ])

    def test_DELETE_current_obj_is_unversioned(self):
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk, {}, '')
        self.app.register(
            'DELETE',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNoContent, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='DELETE',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual('null',
                         dict(headers)['X-Object-Current-Version-Id'])
        self.assertEqual('0000000060.00000',
                         dict(headers)['X-Object-Version-Id'])
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a/c/o?symlink=get'),
            ('DELETE',
             '%s?version-id=0000000060.00000' % self.build_versions_path(
                 obj='o', version='9999999939.99999')),
        ])

    def test_DELETE_null_version(self):
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='DELETE',
            environ={'swift.cache': self.cache_version_on},
            params={'version-id': 'null'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(self.app.calls, [
            ('DELETE', '/v1/a/c/o?version-id=null'),
        ])


class ObjectVersioningVersionAPIWhileDisabled(ObjectVersioningBaseTestCase):

    def test_PUT_version_versioning_disbaled(self):
        timestamp = next(self.ts)
        version_path = '%s?symlink=get' % self.build_versions_path(
            obj='o', version=(~timestamp).normal)
        etag = md5(b'old-version-etag', usedforsecurity=False).hexdigest()
        self.app.register('HEAD', version_path, swob.HTTPNoContent, {
            'Content-Length': 10,
            'Content-Type': 'application/old-version',
            'ETag': etag,
        }, '')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {}, '')
        req = Request.blank(
            '/v1/a/c/o', method='PUT', body=b'',
            environ={'swift.cache': self.cache_version_off},
            params={'version-id': timestamp.normal})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(self.app.calls, [
            ('HEAD', version_path),
            ('PUT', '/v1/a/c/o?version-id=%s' % timestamp.normal),
        ])
        obj_put_headers = self.app.call_list[-1].headers
        symlink_expected_headers = {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: self.build_symlink_path(
                'c', 'o', (~timestamp).normal),
            'x-object-sysmeta-symlink-target-etag': etag,
            'x-object-sysmeta-symlink-target-bytes': '10',
        }
        for k, v in symlink_expected_headers.items():
            self.assertEqual(obj_put_headers[k], v)

    def test_POST_error_versioning_disabled(self):
        req = Request.blank(
            '/v1/a/c/o', method='POST',
            headers={'Content-Type': 'text/plain',
                     'X-Object-Meta-foo': 'bar'},
            environ={'swift.cache': self.cache_version_off,
                     'swift.trans_id': 'fake_trans_id'},
            params={'version-id': '1'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')

    def test_DELETE_current_version(self):
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk, {
            SYSMETA_VERSIONS_SYMLINK: 'true',
            TGT_OBJ_SYSMETA_SYMLINK_HDR: self.build_symlink_path(
                'c', 'o', '9999999939.99999')}, '')
        self.app.register(
            'DELETE',
            self.build_versions_path(obj='o', version='9999999939.99999'),
            swob.HTTPNoContent, {}, '')
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, '')

        # request with versioning disabled
        req = Request.blank(
            '/v1/a/c/o', method='DELETE',
            environ={'swift.cache': self.cache_version_off},
            params={'version-id': '0000000060.00000'})
        status, headers, body = self.call_ov(req)

        self.assertEqual(status, '204 No Content')
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/a/c/o?symlink=get'),
            ('DELETE', '/v1/a/c/o'),
            ('DELETE',
             self.build_versions_path(obj='o', version='9999999939.99999')),
        ])


class ObjectVersioningTestContainerOperations(ObjectVersioningBaseTestCase):
    def test_container_listing_translation(self):
        listing_body = [{
            'bytes': 0,
            'name': 'my-normal-obj',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=%s; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9' % self.build_symlink_path(
                'c', 'my-normal-obj', '9999999989.99999'),
            'last_modified': '2019-07-26T15:09:54.518990',
            'content_type': 'application/foo',
        }, {
            'bytes': 8,
            'name': 'my-old-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '2019-07-26T15:54:38.326800',
            'content_type': 'application/bar',
        }, {
            'bytes': 0,
            'name': 'my-slo-manifest',
            'hash': '387d1ab7d89eda2162bcf8e502667c86; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88; '
            'symlink_target=%s; '
            'symlink_target_etag=387d1ab7d89eda2162bcf8e502667c86; '
            # N.B. symlink_target_bytes is set to the slo_size
            'symlink_target_bytes=10485760' % self.build_symlink_path(
                'c', 'my-slo-manifest', '9999999979.99999'),
            'last_modified': '2019-07-26T15:00:28.499260',
            'content_type': 'application/baz',
        }, {
            'bytes': 0,
            'name': 'unexpected-symlink',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=tgt_container/tgt_obj; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9',
            'last_modified': '2019-07-26T15:09:54.518990',
            'content_type': 'application/symlink',
        }]

        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 9,
            'name': 'my-normal-obj',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '2019-07-26T15:09:54.518990',
            'content_type': 'application/foo',
            'symlink_path':
                '/v1/a/c/my-normal-obj?version-id=0000000010.00000',
            'version_symlink': True,
        }, {
            'bytes': 8,
            'name': 'my-old-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '2019-07-26T15:54:38.326800',
            'content_type': 'application/bar',
        }, {
            'bytes': 10485760,
            'name': 'my-slo-manifest',
            # since we don't have slo middleware in test pipeline, we expect
            # slo_etag to stay in the hash key
            'hash': '387d1ab7d89eda2162bcf8e502667c86; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88',
            'last_modified': '2019-07-26T15:00:28.499260',
            'content_type': 'application/baz',
            'symlink_path':
                '/v1/a/c/my-slo-manifest?version-id=0000000020.00000',
            'version_symlink': True,
        }, {
            'bytes': 0,
            'name': 'unexpected-symlink',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'last_modified': '2019-07-26T15:09:54.518990',
            'symlink_bytes': 9,
            'symlink_path': '/v1/a/tgt_container/tgt_obj',
            'symlink_etag': 'e55cedc11adb39c404b7365f7d6291fa',
            'content_type': 'application/symlink',
        }]
        self.assertEqual(expected, json.loads(body))

    def test_listing_translation_utf8(self):
        listing_body = [{
            'bytes': 0,
            'name': u'\N{SNOWMAN}-obj',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=%s; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9' % self.build_symlink_path(
                u'\N{COMET}-container', u'\N{CLOUD}-target',
                '9999999989.99999'),
            'last_modified': '2019-07-26T15:09:54.518990',
            'content_type': 'application/snowman',
        }]
        self.app.register(
            'GET', '/v1/a/\xe2\x98\x83-test', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: wsgi_quote(
                str_to_wsgi(self.build_container_name(
                    u'\N{COMET}-container'))),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/\xe2\x98\x83-test',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 9,
            'name': u'\N{SNOWMAN}-obj',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '2019-07-26T15:09:54.518990',
            'symlink_path':
                '/v1/a/%E2%98%83-test/%E2%98%81-target?'
                'version-id=0000000010.00000',
            'content_type': 'application/snowman',
            'version_symlink': True,
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 0,
            'name': 'obj',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=%s; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9' %
            self.build_symlink_path('c', 'obj', '9999999979.99999'),
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }]

        versions_listing_body = [{
            'bytes': 9,
            'name': self.build_object_name('obj', '9999999979.99999'),
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }, {
            'bytes': 8,
            'name': self.build_object_name('obj', '9999999989.99999'),
            'hash': 'ebdd8d46ecb4a07f6c433d67eb35d5f2',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
        }]
        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'my-other-object',
            'version_id': 'null',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
            'is_latest': True,
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': '0000000020.00000',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }, {
            'bytes': 8,
            'name': 'obj',
            'version_id': '0000000010.00000',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb35d5f2',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
            'is_latest': False,
        }]
        self.assertEqual(expected, json.loads(body))

        # Can be explicitly JSON
        req = Request.blank(
            '/v1/a/c?versions&format=json',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')

        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on,
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')

        # But everything else is unacceptable
        req = Request.blank(
            '/v1/a/c?versions&format=plain',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on,
                     'HTTP_ACCEPT': 'text/plain'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

        req = Request.blank(
            '/v1/a/c?versions&format=xml',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on,
                     'HTTP_ACCEPT': 'text/xml'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on,
                     'HTTP_ACCEPT': 'application/xml'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

        req = Request.blank(
            '/v1/a/c?versions&format=asdf',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on,
                     'HTTP_ACCEPT': 'foo/bar'})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '406 Not Acceptable')

    def test_list_versions_marker_missing_marker(self):

        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True}, '{}')

        req = Request.blank(
            '/v1/a/c?versions&version_marker=1',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b'version_marker param requires marker')

        req = Request.blank(
            '/v1/a/c?versions&marker=obj&version_marker=id',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b'invalid version_marker param')

    def test_list_versions_marker(self):
        listing_body = [{
            'bytes': 8,
            'name': 'non-versioned-obj',
            'hash': 'etag',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 0,
            'name': 'obj',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=%s; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9' %
            self.build_symlink_path('c', 'obj', '9999999969.99999'),
            'last_modified': '1970-01-01T00:00:30.000000',
            'content_type': 'text/plain',
        }]

        versions_listing_body = [{
            'bytes': 9,
            'name': self.build_object_name('obj', '9999999969.99999'),
            'hash': 'etagv3',
            'last_modified': '1970-01-01T00:00:30.000000',
            'content_type': 'text/plain',
        }, {
            'bytes': 10,
            'name': self.build_object_name('obj', '9999999979.99999'),
            'hash': 'etagv2',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }, {
            'bytes': 8,
            'name': self.build_object_name('obj', '9999999989.99999'),
            'hash': 'etagv1',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
        }]

        expected = [{
            'bytes': 8,
            'name': 'non-versioned-obj',
            'hash': 'etag',
            'version_id': 'null',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': '0000000030.00000',
            'hash': 'etagv3',
            'last_modified': '1970-01-01T00:00:30.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }, {
            'bytes': 10,
            'name': 'obj',
            'version_id': '0000000020.00000',
            'hash': 'etagv2',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': False,
        }, {
            'bytes': 8,
            'name': 'obj',
            'version_id': '0000000010.00000',
            'hash': 'etagv1',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
            'is_latest': False,
        }]

        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body[1:]).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions&marker=obj',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(expected[1:], json.loads(body))

        # version_marker
        self.app.register(
            'GET',
            '%s?marker=%s' % (
                self.build_versions_path(),
                self.build_object_name('obj', '9999999989.99999')),
            swob.HTTPOk, {},
            json.dumps(versions_listing_body[2:]).encode('utf8'))
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body[1:]).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions&marker=obj&version_marker=0000000010.00000',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(expected[3:], json.loads(body))

    def test_list_versions_invalid_delimiter(self):

        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True}, '{}')

        req = Request.blank(
            '/v1/a/c?versions&delimiter=1',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b'invalid delimiter param')

    def test_list_versions_delete_markers(self):
        listing_body = []
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        versions_listing_body = [{
            'name': self.build_object_name('obj', '9999999979.99999'),
            'bytes': 0,
            'hash': utils.MD5_OF_EMPTY_STRING,
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': DELETE_MARKER_CONTENT_TYPE,
        }, {
            'name': self.build_object_name('obj', '9999999989.99999'),
            'bytes': 0,
            'hash': utils.MD5_OF_EMPTY_STRING,
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': DELETE_MARKER_CONTENT_TYPE,
        }]
        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))
        req = Request.blank('/v1/a/c?versions', method='GET',
                            environ={'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        expected = [{
            'name': 'obj',
            'bytes': 0,
            'version_id': '0000000020.00000',
            'hash': utils.MD5_OF_EMPTY_STRING,
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': DELETE_MARKER_CONTENT_TYPE,
            'is_latest': True,
        }, {
            'name': 'obj',
            'bytes': 0,
            'version_id': '0000000010.00000',
            'hash': utils.MD5_OF_EMPTY_STRING,
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': DELETE_MARKER_CONTENT_TYPE,
            'is_latest': False,
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions_unversioned(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            # How did this get here??? Who knows -- maybe another container
            # replica *does* know about versioning being enabled
            'bytes': 0,
            'name': 'obj',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=%s; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9' %
            self.build_symlink_path('c', 'obj', '9999999979.99999'),
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }]

        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPNotFound, {}, None)
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_off})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertNotIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'my-other-object',
            'version_id': 'null',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
            'is_latest': True,
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': '0000000020.00000',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions_delimiter(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 0,
            'name': 'obj',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=%s; '
            'symlink_target_etag=e55cedc11adb39c404b7365f7d6291fa; '
            'symlink_target_bytes=9' %
            self.build_symlink_path('c', 'obj', '9999999979.99999'),
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }, {
            'subdir': 'subdir/'
        }]

        versions_listing_body = [{
            'bytes': 9,
            'name': self.build_object_name('obj', '9999999979.99999'),
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }, {
            'bytes': 8,
            'name': self.build_object_name('obj', '9999999989.99999'),
            'hash': 'ebdd8d46ecb4a07f6c433d67eb35d5f2',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
        }, {
            'subdir': get_reserved_name('subdir/')
        }]

        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions&delimiter=/',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'my-other-object',
            'version_id': 'null',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
            'is_latest': True,
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': '0000000020.00000',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }, {
            'bytes': 8,
            'name': 'obj',
            'version_id': '0000000010.00000',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb35d5f2',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
            'is_latest': False,
        }, {
            'subdir': 'subdir/'
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions_empty_primary(self):
        versions_listing_body = [{
            'bytes': 8,
            'name': self.build_object_name('obj', '9999999979.99999'),
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }, {
            'bytes': 8,
            'name': self.build_object_name('obj', '9999999989.99999'),
            'hash': 'ebdd8d46ecb4a07f6c433d67eb35d5f2',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
        }]
        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            '{}')
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'obj',
            'version_id': '0000000020.00000',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': False,
        }, {
            'bytes': 8,
            'name': 'obj',
            'version_id': '0000000010.00000',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb35d5f2',
            'last_modified': '1970-01-01T00:00:10.000000',
            'content_type': 'text/plain',
            'is_latest': False,
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions_error_versions_container(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 9,
            'name': 'obj',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }]

        self.app.register(
            'GET', self.build_versions_path(),
            swob.HTTPInternalServerError, {}, '')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '500 Internal Error')

    def test_list_versions_empty_versions_container(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 9,
            'name': 'obj',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }]

        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPOk, {}, '{}')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'my-other-object',
            'version_id': 'null',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
            'is_latest': True,
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': 'null',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions_404_versions_container(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 9,
            'name': 'obj',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }]

        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPNotFound, {}, '')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'my-other-object',
            'version_id': 'null',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
            'is_latest': True,
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': 'null',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_versions_never_enabled(self):
        listing_body = [{
            'bytes': 8,
            'name': 'my-other-object',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
        }, {
            'bytes': 9,
            'name': 'obj',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
        }]

        self.app.register(
            'GET', self.build_versions_path(), swob.HTTPNotFound, {}, '')
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_never_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertNotIn('x-versions-enabled', [h.lower() for h, _ in headers])
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        expected = [{
            'bytes': 8,
            'name': 'my-other-object',
            'version_id': 'null',
            'hash': 'ebdd8d46ecb4a07f6c433d67eb05d5f3',
            'last_modified': '1970-01-01T00:00:05.000000',
            'content_type': 'application/bar',
            'is_latest': True,
        }, {
            'bytes': 9,
            'name': 'obj',
            'version_id': 'null',
            'hash': 'e55cedc11adb39c404b7365f7d6291fa',
            'last_modified': '1970-01-01T00:00:20.000000',
            'content_type': 'text/plain',
            'is_latest': True,
        }]
        self.assertEqual(expected, json.loads(body))
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c?format=json'),
            ('HEAD', self.build_versions_path()),
        ])

        # if it's in cache, we won't even get the HEAD
        self.app.clear_calls()
        self.cache_version_never_on.set(
            get_cache_key('a', self.build_container_name('c')),
            {'status': 404})
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': self.cache_version_never_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertNotIn('x-versions-enabled', [h.lower() for h, _ in headers])
        self.assertEqual(expected, json.loads(body))
        self.assertEqual(self.app.calls, [('GET', '/v1/a/c?format=json')])

    def test_bytes_count(self):
        self.app.register(
            'HEAD', self.build_versions_path(), swob.HTTPOk,
            {'X-Container-Bytes-Used': '17',
             'X-Container-Object-Count': '3'}, '')
        self.app.register(
            'HEAD', '/v1/a/c', swob.HTTPOk,
            {SYSMETA_VERSIONS_CONT: self.build_container_name('c'),
             SYSMETA_VERSIONS_ENABLED: True,
             'X-Container-Bytes-Used': '8',
             'X-Container-Object-Count': '1'}, '')
        req = Request.blank(
            '/v1/a/c?versions',
            environ={'REQUEST_METHOD': 'HEAD',
                     'swift.cache': self.cache_version_on})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Versions-Enabled', 'True'), headers)
        self.assertIn(('X-Container-Bytes-Used', '25'), headers)
        self.assertIn(('X-Container-Object-Count', '1'), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])


class ObjectVersioningTestAccountOperations(ObjectVersioningBaseTestCase):

    def test_list_containers(self):
        listing_body = [{
            'bytes': 10,
            'count': 2,
            'name': 'regular-cont',
            'last_modified': '1970-01-01T00:00:05.000000',
        }, {
            'bytes': 0,
            'count': 3,
            'name': 'versioned-cont',
            'last_modified': '1970-01-01T00:00:20.000000',
        }]

        versions_listing_body = [{
            'bytes': 24,
            'count': 3,
            'name': self.build_container_name('versioned-cont'),
            'last_modified': '1970-01-01T00:00:20.000000',
        }]

        cache = FakeMemcache()

        self.app.register(
            'GET', '/v1/a', swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))

        params = {
            'format': 'json',
            'prefix': str_to_wsgi(get_reserved_name('versions')),
        }
        path = '/v1/a?%s' % urllib.parse.urlencode(params)

        self.app.register(
            'GET', path, swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))

        req = Request.blank(
            '/v1/a',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': cache})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        expected = [{
            'bytes': 10,
            'count': 2,
            'name': 'regular-cont',
            'last_modified': '1970-01-01T00:00:05.000000',
        }, {
            'bytes': 24,
            'count': 3,
            'name': 'versioned-cont',
            'last_modified': '1970-01-01T00:00:20.000000',
        }]
        self.assertEqual(expected, json.loads(body))

        req.query_string = 'limit=1'
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(1, len(json.loads(body)))

        req.query_string = 'limit=foo'
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(2, len(json.loads(body)))

        req.query_string = 'limit=100000000000000000000000'
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '412 Precondition Failed')

    def test_list_containers_prefix(self):
        listing_body = [{
            'bytes': 0,
            'count': 1,
            'name': 'versioned-cont',
            'last_modified': '1970-01-01T00:00:05.000000',
        }]

        versions_listing_body = [{
            'bytes': 24,
            'count': 3,
            'name': self.build_container_name('versioned-cont'),
            'last_modified': '1970-01-01T00:00:20.000000',
        }]

        cache = FakeMemcache()

        path = '/v1/a?%s' % urllib.parse.urlencode({
            'format': 'json', 'prefix': 'versioned-'})

        self.app.register(
            'GET', path, swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))

        path = '/v1/a?%s' % urllib.parse.urlencode({
            'format': 'json', 'prefix': str_to_wsgi(
                self.build_container_name('versioned-'))})

        self.app.register(
            'GET', path, swob.HTTPOk, {},
            json.dumps(versions_listing_body).encode('utf8'))

        req = Request.blank(
            '/v1/a?prefix=versioned-',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': cache})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        expected = [{
            'bytes': 24,
            'count': 1,
            'name': 'versioned-cont',
            'last_modified': '1970-01-01T00:00:05.000000',
        }]
        self.assertEqual(expected, json.loads(body))

    def test_list_orphan_hidden_containers(self):

        listing_body = [{
            'bytes': 10,
            'count': 2,
            'name': 'alpha',
            'last_modified': '1970-01-01T00:00:05.000000',
        }, {
            'bytes': 6,
            'count': 3,
            'name': 'bravo',
            'last_modified': '1970-01-01T00:00:20.000000',
        }, {
            'bytes': 0,
            'count': 5,
            'name': 'charlie',
            'last_modified': '1970-01-01T00:00:30.000000',
        }, {
            'bytes': 0,
            'count': 8,
            'name': 'zulu',
            'last_modified': '1970-01-01T00:00:40.000000',
        }]

        versions_listing_body1 = [{
            'bytes': 24,
            'count': 8,
            'name': self.build_container_name('bravo'),
            'last_modified': '1970-01-01T00:00:20.000000',
        }, {
            'bytes': 123,
            'count': 23,
            'name': self.build_container_name('charlie'),
            'last_modified': '1970-01-01T00:00:30.000000',
        }, {
            'bytes': 13,
            'count': 30,
            'name': self.build_container_name('kilo'),
            'last_modified': '1970-01-01T00:00:35.000000',
        }, {
            'bytes': 83,
            'count': 13,
            'name': self.build_container_name('zulu'),
            'last_modified': '1970-01-01T00:00:40.000000',
        }]

        cache = FakeMemcache()

        self.app.register(
            'GET', '/v1/a', swob.HTTPOk, {},
            json.dumps(listing_body).encode('utf8'))

        params = {
            'format': 'json',
            'prefix': str_to_wsgi(get_reserved_name('versions')),
        }
        path = '/v1/a?%s' % urllib.parse.urlencode(params)

        self.app.register(
            'GET', path, swob.HTTPOk, {},
            json.dumps(versions_listing_body1).encode('utf8'))

        req = Request.blank(
            '/v1/a',
            environ={'REQUEST_METHOD': 'GET',
                     'swift.cache': cache})
        status, headers, body = self.call_ov(req)
        self.assertEqual(status, '200 OK')
        expected = [{
            'bytes': 10,
            'count': 2,
            'name': 'alpha',
            'last_modified': '1970-01-01T00:00:05.000000',
        }, {
            'bytes': 30,
            'count': 3,
            'name': 'bravo',
            'last_modified': '1970-01-01T00:00:20.000000',
        }, {
            'bytes': 123,
            'count': 5,
            'name': 'charlie',
            'last_modified': '1970-01-01T00:00:30.000000',
        }, {
            'bytes': 13,
            'count': 0,
            'name': 'kilo',
            'last_modified': '1970-01-01T00:00:35.000000',
        }, {
            'bytes': 83,
            'count': 8,
            'name': 'zulu',
            'last_modified': '1970-01-01T00:00:40.000000',
        }]
        self.assertEqual(expected, json.loads(body))


if __name__ == '__main__':
    unittest.main()
