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

import unittest

from webob import Request

from swift.common.middleware import healthcheck

class FakeApp(object):
    def __call__(self, env, start_response):
        return "FAKE APP"

def start_response(*args):
    pass

class TestHealthCheck(unittest.TestCase):

    def setUp(self):
        self.app = healthcheck.HealthCheckMiddleware(FakeApp())

    def test_healthcheck(self):
        req = Request.blank('/healthcheck', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, ['OK'])

    def test_healtcheck_pass(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

if __name__ == '__main__':
    unittest.main()
