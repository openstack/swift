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
import os
from tempfile import mkdtemp
from urllib import quote
import shutil
from swift.common.storage_policy import StoragePolicy
from swift.common.swob import Request
from swift.common.utils import mkdirs, split_path
from swift.common.wsgi import monkey_patch_mimetools, WSGIContext
from swift.obj import server as object_server
from swift.proxy import server as proxy
import swift.proxy.controllers
from test.unit import FakeMemcache, debug_logger, FakeRing, \
    fake_http_connect, patch_policies


class FakeServerConnection(WSGIContext):
    '''Fakes an HTTPConnection to a server instance.'''
    def __init__(self, app):
        super(FakeServerConnection, self).__init__(app)
        self.data = ''

    def getheaders(self):
        return self._response_headers

    def read(self, amt=None):
        try:
            result = self.resp_iter.next()
            return result
        except StopIteration:
            return ''

    def getheader(self, name, default=None):
        result = self._response_header_value(name)
        return result if result else default

    def getresponse(self):
        environ = {'REQUEST_METHOD': self.method}
        req = Request.blank(self.path, environ, headers=self.req_headers,
                            body=self.data)
        self.data = ''
        self.resp = self._app_call(req.environ)
        self.resp_iter = iter(self.resp)
        if self._response_headers is None:
            self._response_headers = []
        status_parts = self._response_status.split(' ', 1)
        self.status = int(status_parts[0])
        self.reason = status_parts[1] if len(status_parts) == 2 else ''
        return self

    def getexpect(self):
        class ContinueResponse(object):
            status = 100
        return ContinueResponse()

    def send(self, data):
        self.data += data

    def close(self):
        pass

    def __call__(self, ipaddr, port, device, partition, method, path,
                 headers=None, query_string=None):
        self.path = quote('/' + device + '/' + str(partition) + path)
        self.method = method
        self.req_headers = headers
        return self


def get_http_connect(account_func, container_func, object_func):
    '''Returns a http_connect function that delegates to
    entity-specific http_connect methods based on request path.
    '''
    def http_connect(ipaddr, port, device, partition, method, path,
                     headers=None, query_string=None):
        a, c, o = split_path(path, 1, 3, True)
        if o:
            func = object_func
        elif c:
            func = container_func
        else:
            func = account_func
        resp = func(ipaddr, port, device, partition, method, path,
                    headers=headers, query_string=query_string)
        return resp

    return http_connect


@patch_policies([StoragePolicy(0, 'zero', True,
                               object_ring=FakeRing(replicas=1))])
class TestObjectSysmeta(unittest.TestCase):
    '''Tests object sysmeta is correctly handled by combination
    of proxy server and object server.
    '''
    def _assertStatus(self, resp, expected):
        self.assertEqual(resp.status_int, expected,
                         'Expected %d, got %s'
                         % (expected, resp.status))

    def _assertInHeaders(self, resp, expected):
        for key, val in expected.iteritems():
            self.assertTrue(key in resp.headers,
                            'Header %s missing from %s' % (key, resp.headers))
            self.assertEqual(val, resp.headers[key],
                             'Expected header %s:%s, got %s:%s'
                             % (key, val, key, resp.headers[key]))

    def _assertNotInHeaders(self, resp, unexpected):
        for key, val in unexpected.iteritems():
            self.assertFalse(key in resp.headers,
                             'Header %s not expected in %s'
                             % (key, resp.headers))

    def setUp(self):
        self.app = proxy.Application(None, FakeMemcache(),
                                     logger=debug_logger('proxy-ut'),
                                     account_ring=FakeRing(replicas=1),
                                     container_ring=FakeRing(replicas=1))
        monkey_patch_mimetools()
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(self.tmpdir,
                                    'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sda', 'tmp'))
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.obj_ctlr = object_server.ObjectController(
            conf, logger=debug_logger('obj-ut'))

        http_connect = get_http_connect(fake_http_connect(200),
                                        fake_http_connect(200),
                                        FakeServerConnection(self.obj_ctlr))

        self.orig_base_http_connect = swift.proxy.controllers.base.http_connect
        self.orig_obj_http_connect = swift.proxy.controllers.obj.http_connect
        swift.proxy.controllers.base.http_connect = http_connect
        swift.proxy.controllers.obj.http_connect = http_connect

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        swift.proxy.controllers.base.http_connect = self.orig_base_http_connect
        swift.proxy.controllers.obj.http_connect = self.orig_obj_http_connect

    original_sysmeta_headers_1 = {'x-object-sysmeta-test0': 'val0',
                                  'x-object-sysmeta-test1': 'val1'}
    original_sysmeta_headers_2 = {'x-object-sysmeta-test2': 'val2'}
    changed_sysmeta_headers = {'x-object-sysmeta-test0': '',
                               'x-object-sysmeta-test1': 'val1 changed'}
    new_sysmeta_headers = {'x-object-sysmeta-test3': 'val3'}
    original_meta_headers_1 = {'x-object-meta-test0': 'meta0',
                               'x-object-meta-test1': 'meta1'}
    original_meta_headers_2 = {'x-object-meta-test2': 'meta2'}
    changed_meta_headers = {'x-object-meta-test0': '',
                            'x-object-meta-test1': 'meta1 changed'}
    new_meta_headers = {'x-object-meta-test3': 'meta3'}
    bad_headers = {'x-account-sysmeta-test1': 'bad1'}

    def test_PUT_sysmeta_then_GET(self):
        path = '/v1/a/c/o'

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.original_sysmeta_headers_1)
        hdrs.update(self.original_meta_headers_1)
        hdrs.update(self.bad_headers)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        req = Request.blank(path, environ={})
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.original_sysmeta_headers_1)
        self._assertInHeaders(resp, self.original_meta_headers_1)
        self._assertNotInHeaders(resp, self.bad_headers)

    def test_PUT_sysmeta_then_HEAD(self):
        path = '/v1/a/c/o'

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.original_sysmeta_headers_1)
        hdrs.update(self.original_meta_headers_1)
        hdrs.update(self.bad_headers)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        env = {'REQUEST_METHOD': 'HEAD'}
        req = Request.blank(path, environ=env)
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.original_sysmeta_headers_1)
        self._assertInHeaders(resp, self.original_meta_headers_1)
        self._assertNotInHeaders(resp, self.bad_headers)

    def test_sysmeta_replaced_by_PUT(self):
        path = '/v1/a/c/o'

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.original_sysmeta_headers_1)
        hdrs.update(self.original_sysmeta_headers_2)
        hdrs.update(self.original_meta_headers_1)
        hdrs.update(self.original_meta_headers_2)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.changed_sysmeta_headers)
        hdrs.update(self.new_sysmeta_headers)
        hdrs.update(self.changed_meta_headers)
        hdrs.update(self.new_meta_headers)
        hdrs.update(self.bad_headers)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        req = Request.blank(path, environ={})
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.changed_sysmeta_headers)
        self._assertInHeaders(resp, self.new_sysmeta_headers)
        self._assertNotInHeaders(resp, self.original_sysmeta_headers_2)
        self._assertInHeaders(resp, self.changed_meta_headers)
        self._assertInHeaders(resp, self.new_meta_headers)
        self._assertNotInHeaders(resp, self.original_meta_headers_2)

    def _test_sysmeta_not_updated_by_POST(self):
        # check sysmeta is not changed by a POST but user meta is replaced
        path = '/v1/a/c/o'

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.original_sysmeta_headers_1)
        hdrs.update(self.original_meta_headers_1)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        env = {'REQUEST_METHOD': 'POST'}
        hdrs = dict(self.changed_sysmeta_headers)
        hdrs.update(self.new_sysmeta_headers)
        hdrs.update(self.changed_meta_headers)
        hdrs.update(self.new_meta_headers)
        hdrs.update(self.bad_headers)
        req = Request.blank(path, environ=env, headers=hdrs)
        resp = req.get_response(self.app)
        self._assertStatus(resp, 202)

        req = Request.blank(path, environ={})
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.original_sysmeta_headers_1)
        self._assertNotInHeaders(resp, self.new_sysmeta_headers)
        self._assertInHeaders(resp, self.changed_meta_headers)
        self._assertInHeaders(resp, self.new_meta_headers)
        self._assertNotInHeaders(resp, self.bad_headers)

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.changed_sysmeta_headers)
        hdrs.update(self.new_sysmeta_headers)
        hdrs.update(self.bad_headers)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        req = Request.blank(path, environ={})
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.changed_sysmeta_headers)
        self._assertInHeaders(resp, self.new_sysmeta_headers)
        self._assertNotInHeaders(resp, self.original_sysmeta_headers_2)

    def test_sysmeta_not_updated_by_POST(self):
        self.app.object_post_as_copy = False
        self._test_sysmeta_not_updated_by_POST()

    def test_sysmeta_not_updated_by_POST_as_copy(self):
        self.app.object_post_as_copy = True
        self._test_sysmeta_not_updated_by_POST()

    def test_sysmeta_updated_by_COPY(self):
        # check sysmeta is updated by a COPY in same way as user meta
        path = '/v1/a/c/o'
        dest = '/c/o2'
        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.original_sysmeta_headers_1)
        hdrs.update(self.original_sysmeta_headers_2)
        hdrs.update(self.original_meta_headers_1)
        hdrs.update(self.original_meta_headers_2)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        env = {'REQUEST_METHOD': 'COPY'}
        hdrs = dict(self.changed_sysmeta_headers)
        hdrs.update(self.new_sysmeta_headers)
        hdrs.update(self.changed_meta_headers)
        hdrs.update(self.new_meta_headers)
        hdrs.update(self.bad_headers)
        hdrs.update({'Destination': dest})
        req = Request.blank(path, environ=env, headers=hdrs)
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)
        self._assertInHeaders(resp, self.changed_sysmeta_headers)
        self._assertInHeaders(resp, self.new_sysmeta_headers)
        self._assertInHeaders(resp, self.original_sysmeta_headers_2)
        self._assertInHeaders(resp, self.changed_meta_headers)
        self._assertInHeaders(resp, self.new_meta_headers)
        self._assertInHeaders(resp, self.original_meta_headers_2)
        self._assertNotInHeaders(resp, self.bad_headers)

        req = Request.blank('/v1/a/c/o2', environ={})
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.changed_sysmeta_headers)
        self._assertInHeaders(resp, self.new_sysmeta_headers)
        self._assertInHeaders(resp, self.original_sysmeta_headers_2)
        self._assertInHeaders(resp, self.changed_meta_headers)
        self._assertInHeaders(resp, self.new_meta_headers)
        self._assertInHeaders(resp, self.original_meta_headers_2)
        self._assertNotInHeaders(resp, self.bad_headers)

    def test_sysmeta_updated_by_COPY_from(self):
        # check sysmeta is updated by a COPY in same way as user meta
        path = '/v1/a/c/o'
        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.original_sysmeta_headers_1)
        hdrs.update(self.original_sysmeta_headers_2)
        hdrs.update(self.original_meta_headers_1)
        hdrs.update(self.original_meta_headers_2)
        req = Request.blank(path, environ=env, headers=hdrs, body='x')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)

        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = dict(self.changed_sysmeta_headers)
        hdrs.update(self.new_sysmeta_headers)
        hdrs.update(self.changed_meta_headers)
        hdrs.update(self.new_meta_headers)
        hdrs.update(self.bad_headers)
        hdrs.update({'X-Copy-From': '/c/o'})
        req = Request.blank('/v1/a/c/o2', environ=env, headers=hdrs, body='')
        resp = req.get_response(self.app)
        self._assertStatus(resp, 201)
        self._assertInHeaders(resp, self.changed_sysmeta_headers)
        self._assertInHeaders(resp, self.new_sysmeta_headers)
        self._assertInHeaders(resp, self.original_sysmeta_headers_2)
        self._assertInHeaders(resp, self.changed_meta_headers)
        self._assertInHeaders(resp, self.new_meta_headers)
        self._assertInHeaders(resp, self.original_meta_headers_2)
        self._assertNotInHeaders(resp, self.bad_headers)

        req = Request.blank('/v1/a/c/o2', environ={})
        resp = req.get_response(self.app)
        self._assertStatus(resp, 200)
        self._assertInHeaders(resp, self.changed_sysmeta_headers)
        self._assertInHeaders(resp, self.new_sysmeta_headers)
        self._assertInHeaders(resp, self.original_sysmeta_headers_2)
        self._assertInHeaders(resp, self.changed_meta_headers)
        self._assertInHeaders(resp, self.new_meta_headers)
        self._assertInHeaders(resp, self.original_meta_headers_2)
        self._assertNotInHeaders(resp, self.bad_headers)
