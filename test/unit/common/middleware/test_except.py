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

from swift.common.swob import Request
from swift.common.middleware import catch_errors
from swift.common.utils import get_logger


class StrangeException(BaseException):
    pass


class FakeApp(object):

    def __init__(self, error=False, body_iter=None):
        self.error = error
        self.body_iter = body_iter

    def __call__(self, env, start_response):
        if 'swift.trans_id' not in env:
            raise Exception('Trans id should always be in env')
        if self.error:
            if self.error == 'strange':
                raise StrangeException('whoa')
            raise Exception('An error occurred')
        if self.body_iter is None:
            return ["FAKE APP"]
        else:
            return self.body_iter


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
        self.assertEquals(list(resp), ['FAKE APP'])

    def test_catcherrors(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(True), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        self.assertEquals(list(resp), ['An error occurred'])

    def test_trans_id_header_pass(self):
        self.assertEquals(self.logger.txn_id, None)

        def start_response(status, headers, exc_info=None):
            self.assert_('X-Trans-Id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, start_response)
        self.assertEquals(len(self.logger.txn_id), 34)  # 32 hex + 'tx'

    def test_trans_id_header_fail(self):
        self.assertEquals(self.logger.txn_id, None)

        def start_response(status, headers, exc_info=None):
            self.assert_('X-Trans-Id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(FakeApp(True), {})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, start_response)
        self.assertEquals(len(self.logger.txn_id), 34)

    def test_error_in_iterator(self):
        app = catch_errors.CatchErrorMiddleware(
            FakeApp(body_iter=(int(x) for x in 'abcd')), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        self.assertEquals(list(resp), ['An error occurred'])

    def test_trans_id_header_suffix(self):
        self.assertEquals(self.logger.txn_id, None)

        def start_response(status, headers, exc_info=None):
            self.assert_('X-Trans-Id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(
            FakeApp(), {'trans_id_suffix': '-stuff'})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, start_response)
        self.assertTrue(self.logger.txn_id.endswith('-stuff'))

    def test_trans_id_header_extra(self):
        self.assertEquals(self.logger.txn_id, None)

        def start_response(status, headers, exc_info=None):
            self.assert_('X-Trans-Id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(
            FakeApp(), {'trans_id_suffix': '-fromconf'})
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Trans-Id-Extra': 'fromuser'})
        app(req.environ, start_response)
        self.assertTrue(self.logger.txn_id.endswith('-fromconf-fromuser'))

    def test_trans_id_header_extra_length_limit(self):
        self.assertEquals(self.logger.txn_id, None)

        def start_response(status, headers, exc_info=None):
            self.assert_('X-Trans-Id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(
            FakeApp(), {'trans_id_suffix': '-fromconf'})
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Trans-Id-Extra': 'a' * 1000})
        app(req.environ, start_response)
        self.assertTrue(self.logger.txn_id.endswith(
            '-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))

    def test_trans_id_header_extra_quoted(self):
        self.assertEquals(self.logger.txn_id, None)

        def start_response(status, headers, exc_info=None):
            self.assert_('X-Trans-Id' in (x[0] for x in headers))
        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Trans-Id-Extra': 'xan than"gum'})
        app(req.environ, start_response)
        self.assertTrue(self.logger.txn_id.endswith('-xan%20than%22gum'))

    def test_catcherrors_with_unexpected_error(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(error='strange'), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        self.assertEquals(list(resp), ['An error occurred'])


if __name__ == '__main__':
    unittest.main()
