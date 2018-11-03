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

import json
import unittest
import time
from mock import Mock

from swift.proxy.controllers import InfoController
from swift.proxy.server import Application as ProxyApp
from swift.common import utils
from swift.common.swob import Request, HTTPException


class TestInfoController(unittest.TestCase):

    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def get_controller(self, expose_info=None, disallowed_sections=None,
                       admin_key=None):
        disallowed_sections = disallowed_sections or []

        app = Mock(spec=ProxyApp)
        return InfoController(app, None, expose_info,
                              disallowed_sections, admin_key)

    def start_response(self, status, headers):
        self.got_statuses.append(status)
        for h in headers:
            self.got_headers.append({h[0]: h[1]})

    def test_disabled_info(self):
        controller = self.get_controller(expose_info=False)

        req = Request.blank(
            '/info', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('403 Forbidden', str(resp))

    def test_get_info(self):
        controller = self.get_controller(expose_info=True)
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        req = Request.blank(
            '/info', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))
        info = json.loads(resp.body)
        self.assertNotIn('admin', info)
        self.assertIn('foo', info)
        self.assertIn('bar', info['foo'])
        self.assertEqual(info['foo']['bar'], 'baz')

    def test_options_info(self):
        controller = self.get_controller(expose_info=True)

        req = Request.blank(
            '/info', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.OPTIONS(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))
        self.assertIn('Allow', resp.headers)

    def test_get_info_cors(self):
        controller = self.get_controller(expose_info=True)
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        req = Request.blank(
            '/info', environ={'REQUEST_METHOD': 'GET'},
            headers={'Origin': 'http://example.com'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))
        info = json.loads(resp.body)
        self.assertNotIn('admin', info)
        self.assertIn('foo', info)
        self.assertIn('bar', info['foo'])
        self.assertEqual(info['foo']['bar'], 'baz')
        self.assertIn('Access-Control-Allow-Origin', resp.headers)
        self.assertIn('Access-Control-Expose-Headers', resp.headers)

    def test_head_info(self):
        controller = self.get_controller(expose_info=True)
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        req = Request.blank(
            '/info', environ={'REQUEST_METHOD': 'HEAD'})
        resp = controller.HEAD(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))

    def test_disallow_info(self):
        controller = self.get_controller(expose_info=True,
                                         disallowed_sections=['foo2'])
        utils._swift_info = {'foo': {'bar': 'baz'},
                             'foo2': {'bar2': 'baz2'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        req = Request.blank(
            '/info', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))
        info = json.loads(resp.body)
        self.assertIn('foo', info)
        self.assertIn('bar', info['foo'])
        self.assertEqual(info['foo']['bar'], 'baz')
        self.assertNotIn('foo2', info)

    def test_disabled_admin_info(self):
        controller = self.get_controller(expose_info=True, admin_key='')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('GET', '/info', expires, '')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('403 Forbidden', str(resp))

    def test_get_admin_info(self):
        controller = self.get_controller(expose_info=True,
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('GET', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))
        info = json.loads(resp.body)
        self.assertIn('admin', info)
        self.assertIn('qux', info['admin'])
        self.assertIn('quux', info['admin']['qux'])
        self.assertEqual(info['admin']['qux']['quux'], 'corge')

    def test_head_admin_info(self):
        controller = self.get_controller(expose_info=True,
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('GET', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'HEAD'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('HEAD', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'HEAD'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))

    def test_get_admin_info_invalid_method(self):
        controller = self.get_controller(expose_info=True,
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('HEAD', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('401 Unauthorized', str(resp))

    def test_get_admin_info_invalid_expires(self):
        controller = self.get_controller(expose_info=True,
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = 1
        sig = utils.get_hmac('GET', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('401 Unauthorized', str(resp))

        expires = 'abc'
        sig = utils.get_hmac('GET', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('401 Unauthorized', str(resp))

    def test_get_admin_info_invalid_path(self):
        controller = self.get_controller(expose_info=True,
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('GET', '/foo', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('401 Unauthorized', str(resp))

    def test_get_admin_info_invalid_key(self):
        controller = self.get_controller(expose_info=True,
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('GET', '/foo', expires, 'invalid-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('401 Unauthorized', str(resp))

    def test_admin_disallow_info(self):
        controller = self.get_controller(expose_info=True,
                                         disallowed_sections=['foo2'],
                                         admin_key='secret-admin-key')
        utils._swift_info = {'foo': {'bar': 'baz'},
                             'foo2': {'bar2': 'baz2'}}
        utils._swift_admin_info = {'qux': {'quux': 'corge'}}

        expires = int(time.time() + 86400)
        sig = utils.get_hmac('GET', '/info', expires, 'secret-admin-key')
        path = '/info?swiftinfo_sig={sig}&swiftinfo_expires={expires}'.format(
            sig=sig, expires=expires)
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'GET'})
        resp = controller.GET(req)
        self.assertIsInstance(resp, HTTPException)
        self.assertEqual('200 OK', str(resp))
        info = json.loads(resp.body)
        self.assertNotIn('foo2', info)
        self.assertIn('admin', info)
        self.assertIn('disallowed_sections', info['admin'])
        self.assertIn('foo2', info['admin']['disallowed_sections'])
        self.assertIn('qux', info['admin'])
        self.assertIn('quux', info['admin']['qux'])
        self.assertEqual(info['admin']['qux']['quux'], 'corge')


if __name__ == '__main__':
    unittest.main()
