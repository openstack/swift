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

from swift.common.swob import Request, Response
from swift.common.middleware import gatekeeper


class FakeApp(object):
    def __init__(self, headers={}):
        self.headers = headers
        self.req = None

    def __call__(self, env, start_response):
        self.req = Request(env)
        return Response(request=self.req, body='FAKE APP',
                        headers=self.headers)(env, start_response)


class TestGatekeeper(unittest.TestCase):
    methods = ['PUT', 'POST', 'GET', 'DELETE', 'HEAD', 'COPY', 'OPTIONS']

    allowed_headers = {'xx-account-sysmeta-foo': 'value',
                       'xx-container-sysmeta-foo': 'value',
                       'xx-object-sysmeta-foo': 'value',
                       'x-account-meta-foo': 'value',
                       'x-container-meta-foo': 'value',
                       'x-object-meta-foo': 'value',
                       'x-timestamp-foo': 'value'}

    sysmeta_headers = {'x-account-sysmeta-': 'value',
                       'x-container-sysmeta-': 'value',
                       'x-object-sysmeta-': 'value',
                       'x-account-sysmeta-foo': 'value',
                       'x-container-sysmeta-foo': 'value',
                       'x-object-sysmeta-foo': 'value',
                       'X-Account-Sysmeta-BAR': 'value',
                       'X-Container-Sysmeta-BAR': 'value',
                       'X-Object-Sysmeta-BAR': 'value'}

    forbidden_headers_out = dict(sysmeta_headers)
    forbidden_headers_in = dict(sysmeta_headers)

    def _assertHeadersEqual(self, expected, actual):
        for key in expected:
            self.assertTrue(key.lower() in actual,
                            '%s missing from %s' % (key, actual))

    def _assertHeadersAbsent(self, unexpected, actual):
        for key in unexpected:
            self.assertTrue(key.lower() not in actual,
                            '%s is in %s' % (key, actual))

    def get_app(self, app, global_conf, **local_conf):
        factory = gatekeeper.filter_factory(global_conf, **local_conf)
        return factory(app)

    def test_ok_header(self):
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers=self.allowed_headers)
        fake_app = FakeApp()
        app = self.get_app(fake_app, {})
        resp = req.get_response(app)
        self.assertEquals('200 OK', resp.status)
        self.assertEquals(resp.body, 'FAKE APP')
        self._assertHeadersEqual(self.allowed_headers, fake_app.req.headers)

    def _test_reserved_header_removed_inbound(self, method):
        headers = dict(self.forbidden_headers_in)
        headers.update(self.allowed_headers)
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method},
                            headers=headers)
        fake_app = FakeApp()
        app = self.get_app(fake_app, {})
        resp = req.get_response(app)
        self.assertEquals('200 OK', resp.status)
        self._assertHeadersEqual(self.allowed_headers, fake_app.req.headers)
        self._assertHeadersAbsent(self.forbidden_headers_in,
                                  fake_app.req.headers)

    def test_reserved_header_removed_inbound(self):
        for method in self.methods:
            self._test_reserved_header_removed_inbound(method)

    def _test_reserved_header_removed_outbound(self, method):
        headers = dict(self.forbidden_headers_out)
        headers.update(self.allowed_headers)
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method})
        fake_app = FakeApp(headers=headers)
        app = self.get_app(fake_app, {})
        resp = req.get_response(app)
        self.assertEquals('200 OK', resp.status)
        self._assertHeadersEqual(self.allowed_headers, resp.headers)
        self._assertHeadersAbsent(self.forbidden_headers_out, resp.headers)

    def test_reserved_header_removed_outbound(self):
        for method in self.methods:
            self._test_reserved_header_removed_outbound(method)


if __name__ == '__main__':
    unittest.main()
