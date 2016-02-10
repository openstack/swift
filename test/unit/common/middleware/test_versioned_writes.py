# Copyright (c) 2013 OpenStack Foundation
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
import os
import time
import unittest
from swift.common import swob
from swift.common.middleware import versioned_writes
from swift.common.swob import Request
from test.unit.common.middleware.helpers import FakeSwift


class FakeCache(object):

    def __init__(self, val):
        if 'status' not in val:
            val['status'] = 200
        self.val = val

    def get(self, *args):
        return self.val


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


class VersionedWritesTestCase(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        conf = {'allow_versioned_writes': 'true'}
        self.vw = versioned_writes.filter_factory(conf)(self.app)

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

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

        body_iter = app(req.environ, start_response)
        body = ''
        caught_exc = None
        try:
            for chunk in body_iter:
                body += chunk
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return status[0], headers[0], body, caught_exc
        else:
            return status[0], headers[0], body

    def call_vw(self, req, **kwargs):
        return self.call_app(req, app=self.vw, **kwargs)

    def assertRequestEqual(self, req, other):
        self.assertEqual(req.method, other.method)
        self.assertEqual(req.path, other.path)

    def test_put_container(self):
        self.app.register('PUT', '/v1/a/c', swob.HTTPOk, {}, 'passed')
        req = Request.blank('/v1/a/c',
                            headers={'X-Versions-Location': 'ver_cont'},
                            environ={'REQUEST_METHOD': 'PUT'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')

        # check for sysmeta header
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c', path)
        self.assertTrue('x-container-sysmeta-versions-location' in req_headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_container_allow_versioned_writes_false(self):
        self.vw.conf = {'allow_versioned_writes': 'false'}

        # PUT/POST container must fail as 412 when allow_versioned_writes
        # set to false
        for method in ('PUT', 'POST'):
            req = Request.blank('/v1/a/c',
                                headers={'X-Versions-Location': 'ver_cont'},
                                environ={'REQUEST_METHOD': method})
            status, headers, body = self.call_vw(req)
            self.assertEqual(status, "412 Precondition Failed")

        # GET/HEAD performs as normal
        self.app.register('GET', '/v1/a/c', swob.HTTPOk, {}, 'passed')
        self.app.register('HEAD', '/v1/a/c', swob.HTTPOk, {}, 'passed')

        for method in ('GET', 'HEAD'):
            req = Request.blank('/v1/a/c',
                                headers={'X-Versions-Location': 'ver_cont'},
                                environ={'REQUEST_METHOD': method})
            status, headers, body = self.call_vw(req)
            self.assertEqual(status, '200 OK')

    def test_remove_versions_location(self):
        self.app.register('POST', '/v1/a/c', swob.HTTPOk, {}, 'passed')
        req = Request.blank('/v1/a/c',
                            headers={'X-Remove-Versions-Location': 'x'},
                            environ={'REQUEST_METHOD': 'POST'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')

        # check for sysmeta header
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('POST', method)
        self.assertEqual('/v1/a/c', path)
        self.assertTrue('x-container-sysmeta-versions-location' in req_headers)
        self.assertTrue('x-versions-location' in req_headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_remove_add_versions_precedence(self):
        self.app.register(
            'POST', '/v1/a/c', swob.HTTPOk,
            {'x-container-sysmeta-versions-location': 'ver_cont'},
            'passed')
        req = Request.blank('/v1/a/c',
                            headers={'X-Remove-Versions-Location': 'x',
                                     'X-Versions-Location': 'ver_cont'},
                            environ={'REQUEST_METHOD': 'POST'})

        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertTrue(('X-Versions-Location', 'ver_cont') in headers)

        # check for sysmeta header
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('POST', method)
        self.assertEqual('/v1/a/c', path)
        self.assertTrue('x-container-sysmeta-versions-location' in req_headers)
        self.assertTrue('x-remove-versions-location' not in req_headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_get_container(self):
        self.app.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {'x-container-sysmeta-versions-location': 'ver_cont'}, None)
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertTrue(('X-Versions-Location', 'ver_cont') in headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_get_head(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, None)
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk, {}, None)
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_put_object_no_versioning(self):
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')

        cache = FakeCache({})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_put_first_object_success(self):
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c/o', swob.HTTPNotFound, {}, None)

        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_PUT_versioning_with_nonzero_default_policy(self):
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c/o', swob.HTTPNotFound, {}, None)

        cache = FakeCache({'versions': 'ver_cont', 'storage_policy': '2'})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')

        # check for 'X-Backend-Storage-Policy-Index' in HEAD request
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('HEAD', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue('X-Backend-Storage-Policy-Index' in req_headers)
        self.assertEqual('2',
                         req_headers.get('X-Backend-Storage-Policy-Index'))
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_put_object_no_versioning_with_container_config_true(self):
        # set False to versions_write obsously and expect no COPY occurred
        self.vw.conf = {'allow_versioned_writes': 'false'}
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPCreated, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Wed, 19 Nov 2014 18:19:02 GMT'}, 'passed')
        cache = FakeCache({'versions': 'ver_cont'})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        called_method = [method for (method, path, hdrs) in self.app._calls]
        self.assertTrue('COPY' not in called_method)

    def test_delete_object_no_versioning_with_container_config_true(self):
        # set False to versions_write obviously and expect no GET versioning
        # container and COPY called (just delete object as normal)
        self.vw.conf = {'allow_versioned_writes': 'false'}
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, 'passed')
        cache = FakeCache({'versions': 'ver_cont'})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE', 'swift.cache': cache})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '204 No Content')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        called_method = \
            [method for (method, path, rheaders) in self.app._calls]
        self.assertTrue('COPY' not in called_method)
        self.assertTrue('GET' not in called_method)

    def test_copy_object_no_versioning_with_container_config_true(self):
        # set False to versions_write obviously and expect no extra
        # COPY called (just copy object as normal)
        self.vw.conf = {'allow_versioned_writes': 'false'}
        self.app.register(
            'COPY', '/v1/a/c/o', swob.HTTPCreated, {}, None)
        cache = FakeCache({'versions': 'ver_cont'})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'COPY', 'swift.cache': cache})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        called_method = \
            [method for (method, path, rheaders) in self.app._calls]
        self.assertTrue('COPY' in called_method)
        self.assertEqual(called_method.count('COPY'), 1)

    def test_new_version_success(self):
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Wed, 19 Nov 2014 18:19:02 GMT'}, 'passed')
        self.app.register(
            'COPY', '/v1/a/c/o', swob.HTTPCreated, {}, None)
        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    @local_tz
    def test_new_version_sysmeta_precedence(self):
        self.app.register(
            'PUT', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/c/o', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:00:00 GMT'}, 'passed')
        self.app.register(
            'COPY', '/v1/a/c/o', swob.HTTPCreated, {}, None)

        # fill cache with two different values for versions location
        # new middleware should use sysmeta first
        cache = FakeCache({'versions': 'old_ver_cont',
                          'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

        # check that sysmeta header was used
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('COPY', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertEqual('ver_cont/001o/0000000000.00000',
                         req_headers['Destination'])

    def test_copy_first_version(self):
        self.app.register(
            'COPY', '/v1/a/src_cont/src_obj', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/tgt_cont/tgt_obj', swob.HTTPNotFound, {}, None)
        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'tgt_cont/tgt_obj'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_copy_new_version(self):
        self.app.register(
            'COPY', '/v1/a/src_cont/src_obj', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/a/tgt_cont/tgt_obj', swob.HTTPOk,
            {'last-modified': 'Wed, 19 Nov 2014 18:19:02 GMT'}, 'passed')
        self.app.register(
            'COPY', '/v1/a/tgt_cont/tgt_obj', swob.HTTPCreated, {}, None)
        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'tgt_cont/tgt_obj'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_copy_new_version_different_account(self):
        self.app.register(
            'COPY', '/v1/src_a/src_cont/src_obj', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'HEAD', '/v1/tgt_a/tgt_cont/tgt_obj', swob.HTTPOk,
            {'last-modified': 'Wed, 19 Nov 2014 18:19:02 GMT'}, 'passed')
        self.app.register(
            'COPY', '/v1/tgt_a/tgt_cont/tgt_obj', swob.HTTPCreated, {}, None)
        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/src_a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'tgt_cont/tgt_obj',
                     'Destination-Account': 'tgt_a'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_copy_new_version_bogus_account(self):
        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/src_a/src_cont/src_obj',
            environ={'REQUEST_METHOD': 'COPY', 'swift.cache': cache,
                     'CONTENT_LENGTH': '100'},
            headers={'Destination': 'tgt_cont/tgt_obj',
                     'Destination-Account': '/im/on/a/boat'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '412 Precondition Failed')

    def test_delete_first_object_success(self):
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'GET',
            '/v1/a/ver_cont?format=json&prefix=001o/&reverse=on&marker=',
            swob.HTTPNotFound, {}, None)

        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE', 'swift.cache': cache,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_delete_latest_version_success(self):
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'GET',
            '/v1/a/ver_cont?format=json&prefix=001o/&reverse=on&marker=',
            swob.HTTPOk, {},
            '[{"hash": "y", '
            '"last_modified": "2014-11-21T14:23:02.206740", '
            '"bytes": 3, '
            '"name": "001o/2", '
            '"content_type": "text/plain"}, '
            '{"hash": "x", '
            '"last_modified": "2014-11-21T14:14:27.409100", '
            '"bytes": 3, '
            '"name": "001o/1", '
            '"content_type": "text/plain"}]')
        self.app.register(
            'COPY', '/v1/a/ver_cont/001o/2', swob.HTTPCreated,
            {}, None)
        self.app.register(
            'DELETE', '/v1/a/ver_cont/001o/2', swob.HTTPOk,
            {}, None)

        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            headers={'X-If-Delete-At': 1},
            environ={'REQUEST_METHOD': 'DELETE', 'swift.cache': cache,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

        # check that X-If-Delete-At was removed from DELETE request
        calls = self.app.calls_with_headers
        method, path, req_headers = calls.pop()
        self.assertEqual('DELETE', method)
        self.assertTrue(path.startswith('/v1/a/ver_cont/001o/2'))
        self.assertFalse('x-if-delete-at' in req_headers or
                         'X-If-Delete-At' in req_headers)

    def test_DELETE_on_expired_versioned_object(self):
        self.app.register(
            'GET',
            '/v1/a/ver_cont?format=json&prefix=001o/&reverse=on&marker=',
            swob.HTTPOk, {},
            '[{"hash": "y", '
            '"last_modified": "2014-11-21T14:23:02.206740", '
            '"bytes": 3, '
            '"name": "001o/2", '
            '"content_type": "text/plain"}, '
            '{"hash": "x", '
            '"last_modified": "2014-11-21T14:14:27.409100", '
            '"bytes": 3, '
            '"name": "001o/1", '
            '"content_type": "text/plain"}]')

        # expired object
        self.app.register(
            'COPY', '/v1/a/ver_cont/001o/2', swob.HTTPNotFound,
            {}, None)
        self.app.register(
            'COPY', '/v1/a/ver_cont/001o/1', swob.HTTPCreated,
            {}, None)
        self.app.register(
            'DELETE', '/v1/a/ver_cont/001o/1', swob.HTTPOk,
            {}, None)

        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE', 'swift.cache': cache,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_denied_DELETE_of_versioned_object(self):
        authorize_call = []
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'GET',
            '/v1/a/ver_cont?format=json&prefix=001o/&reverse=on&marker=',
            swob.HTTPOk, {},
            '[{"hash": "y", '
            '"last_modified": "2014-11-21T14:23:02.206740", '
            '"bytes": 3, '
            '"name": "001o/2", '
            '"content_type": "text/plain"}, '
            '{"hash": "x", '
            '"last_modified": "2014-11-21T14:14:27.409100", '
            '"bytes": 3, '
            '"name": "001o/1", '
            '"content_type": "text/plain"}]')
        self.app.register(
            'DELETE', '/v1/a/c/o', swob.HTTPForbidden,
            {}, None)

        def fake_authorize(req):
            authorize_call.append(req)
            return swob.HTTPForbidden()

        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE', 'swift.cache': cache,
                     'swift.authorize': fake_authorize,
                     'CONTENT_LENGTH': '0'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '403 Forbidden')
        self.assertEqual(len(authorize_call), 1)
        self.assertRequestEqual(req, authorize_call[0])
