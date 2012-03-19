# Copyright (c) 2010-2012 OpenStack, LLC.
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

from webob import Request, Response

from swift.common.middleware import catch_errors
from swift.common.utils import get_logger

class FakeApp(object):
    def __init__(self, error=False):
        self.error = error

    def __call__(self, env, start_response):
        if 'swift.trans_id' not in env:
            raise Exception('Trans id should always be in env')
        if self.error:
            raise Exception('An error occurred')
        return ["FAKE APP"]

def start_response(*args):
    pass

class TestCatchErrors(unittest.TestCase):

    def setUp(self):
        self.logger = get_logger({})
        self.logger.txn_id = None

    def test_catcherrors_passthrough(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        self.assertEquals(resp, ['FAKE APP'])

    def test_catcherrors(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(True), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        self.assertEquals(resp, ['An error occurred'])

    def test_trans_id_header_pass(self):
        self.assertEquals(self.logger.txn_id, None)
        def start_response(status, headers):
            self.assert_('x-trans-id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, start_response)
        self.assertEquals(len(self.logger.txn_id), 34) # 32 hex + 'tx'

    def test_trans_id_header_fail(self):
        self.assertEquals(self.logger.txn_id, None)
        def start_response(status, headers):
            self.assert_('x-trans-id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(FakeApp(True), {})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, start_response)
        self.assertEquals(len(self.logger.txn_id), 34)

if __name__ == '__main__':
    unittest.main()
