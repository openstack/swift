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

import os
import shutil
import tempfile
import unittest

from swift.common.swob import Request, Response
from swift.common.middleware import healthcheck


class FakeApp(object):
    def __call__(self, env, start_response):
        req = Request(env)
        return Response(request=req, body=b'FAKE APP')(
            env, start_response)


class TestHealthCheck(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.disable_path = os.path.join(self.tempdir, 'dont-taze-me-bro')
        self.got_statuses = []

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def get_app(self, app, global_conf, **local_conf):
        factory = healthcheck.filter_factory(global_conf, **local_conf)
        return factory(app)

    def start_response(self, status, headers):
        self.got_statuses.append(status)

    def test_healthcheck(self):
        req = Request.blank('/healthcheck', environ={'REQUEST_METHOD': 'GET'})
        app = self.get_app(FakeApp(), {})
        resp = app(req.environ, self.start_response)
        self.assertEqual(['200 OK'], self.got_statuses)
        self.assertEqual(resp, [b'OK'])

    def test_healthcheck_pass(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        app = self.get_app(FakeApp(), {})
        resp = app(req.environ, self.start_response)
        self.assertEqual(['200 OK'], self.got_statuses)
        self.assertEqual(resp, [b'FAKE APP'])

    def test_healthcheck_pass_not_disabled(self):
        req = Request.blank('/healthcheck', environ={'REQUEST_METHOD': 'GET'})
        app = self.get_app(FakeApp(), {}, disable_path=self.disable_path)
        resp = app(req.environ, self.start_response)
        self.assertEqual(['200 OK'], self.got_statuses)
        self.assertEqual(resp, [b'OK'])

    def test_healthcheck_pass_disabled(self):
        open(self.disable_path, 'w')
        req = Request.blank('/healthcheck', environ={'REQUEST_METHOD': 'GET'})
        app = self.get_app(FakeApp(), {}, disable_path=self.disable_path)
        resp = app(req.environ, self.start_response)
        self.assertEqual(['503 Service Unavailable'], self.got_statuses)
        self.assertEqual(resp, [b'DISABLED BY FILE'])


if __name__ == '__main__':
    unittest.main()
