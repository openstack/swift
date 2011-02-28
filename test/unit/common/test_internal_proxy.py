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

# TODO: Tests

import unittest
import webob
import tempfile

from swift.common import internal_proxy

class DumbBaseApplicationFactory(object):

    def __init__(self, status_codes, body=''):
        self.status_codes = status_codes[:]
        self.body = body

    def __call__(self, *a, **kw):
        app = DumbBaseApplication(*a, **kw)
        app.status_codes = self.status_codes
        app.body = self.body
        return app

class DumbBaseApplication(object):

    def __init__(self, *a, **kw):
        self.status_codes = []
        self.call_count = 0
        self.body = ''

    def handle_request(self, req):
        self.call_count += 1
        resp = webob.Response(request=req, body=self.body,
                              conditional_response=True)
        resp.status_int = self.status_codes.pop(0)
        return resp

    def update_request(self, req):
        return req


class TestInternalProxy(unittest.TestCase):

    def test_webob_request_copy(self):
        req = webob.Request.blank('/')
        req2 = internal_proxy.webob_request_copy(req)
        self.assertEquals(req, req2)

    def test_handle_request(self):
        status_codes = [200]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes)
        p = internal_proxy.InternalProxy()
        req = webob.Request.blank('/')
        orig_req = internal_proxy.webob_request_copy(req)
        resp = p._handle_request(req)
        self.assertEquals(str(req), str(orig_req), '%s != %s' % (req, orig_req))

    def test_get_object(self):
        status_codes = [200]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes)
        p = internal_proxy.InternalProxy()
        code, body = p.get_object('a', 'c', 'o')
        body = ''.join(body)
        self.assertEquals(code, 200)
        self.assertEquals(body, '')

    def test_create_container(self):
        status_codes = [200]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes)
        p = internal_proxy.InternalProxy()
        resp = p.create_container('a', 'c')
        self.assertTrue(resp)

    def test_handle_request_with_retries(self):
        status_codes = [500, 200]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes)
        p = internal_proxy.InternalProxy(retries=3)
        req = webob.Request.blank('/')
        orig_req = internal_proxy.webob_request_copy(req)
        resp = p._handle_request(req)
        self.assertEquals(str(req), str(orig_req), '%s != %s' % (req, orig_req))
        self.assertEquals(p.upload_app.call_count, 2)

    def test_get_container_list_empty(self):
        status_codes = [200]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes, body='[]')
        p = internal_proxy.InternalProxy()
        resp = p.get_container_list('a', 'c')
        self.assertEquals(resp, [])

    def test_get_container_list_no_body(self):
        status_codes = [204]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes, body='')
        p = internal_proxy.InternalProxy()
        resp = p.get_container_list('a', 'c')
        self.assertEquals(resp, [])

    def test_get_container_list_full(self):
        status_codes = [204]
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes, body='')
        p = internal_proxy.InternalProxy()
        resp = p.get_container_list('a', 'c', marker='a', end_marker='b',
                                    limit=100, prefix='/', delimiter='.')
        self.assertEquals(resp, [])

    def test_upload_file(self):
        status_codes = [200, 200]  # contianer HEAD + object PUT
        internal_proxy.BaseApplication = DumbBaseApplicationFactory(
                                            status_codes)
        p = internal_proxy.InternalProxy()
        with tempfile.NamedTemporaryFile() as file_obj:
            resp = p.upload_file(file_obj, 'a', 'c', 'o')
        self.assertTrue(resp)


if __name__ == '__main__':
    unittest.main()
