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
            return [b"FAKE APP"]
        else:
            return self.body_iter


class TestCatchErrors(unittest.TestCase):

    def setUp(self):
        self.logger = get_logger({})
        self.logger.txn_id = None

    def start_response(self, status, headers, *args):
        request_ids = ('X-Trans-Id', 'X-Openstack-Request-Id')
        hdict = dict(headers)
        for key in request_ids:
            self.assertIn(key, hdict)
        for key1, key2 in zip(request_ids, request_ids[1:]):
            self.assertEqual(hdict[key1], hdict[key2])

    def test_catcherrors_passthrough(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, self.start_response)
        self.assertEqual(list(resp), [b'FAKE APP'])

    def test_catcherrors(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(True), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, self.start_response)
        self.assertEqual(list(resp), [b'An error occurred'])

    def test_trans_id_header_pass(self):
        self.assertIsNone(self.logger.txn_id)

        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, self.start_response)
        self.assertEqual(len(self.logger.txn_id), 34)  # 32 hex + 'tx'

    def test_trans_id_header_fail(self):
        self.assertIsNone(self.logger.txn_id)

        app = catch_errors.CatchErrorMiddleware(FakeApp(True), {})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, self.start_response)
        self.assertEqual(len(self.logger.txn_id), 34)

    def test_error_in_iterator(self):
        app = catch_errors.CatchErrorMiddleware(
            FakeApp(body_iter=(int(x) for x in 'abcd')), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, self.start_response)
        self.assertEqual(list(resp), [b'An error occurred'])

    def test_trans_id_header_suffix(self):
        self.assertIsNone(self.logger.txn_id)

        app = catch_errors.CatchErrorMiddleware(
            FakeApp(), {'trans_id_suffix': '-stuff'})
        req = Request.blank('/v1/a/c/o')
        app(req.environ, self.start_response)
        self.assertTrue(self.logger.txn_id.endswith('-stuff'))

    def test_trans_id_header_extra(self):
        self.assertIsNone(self.logger.txn_id)

        app = catch_errors.CatchErrorMiddleware(
            FakeApp(), {'trans_id_suffix': '-fromconf'})
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Trans-Id-Extra': 'fromuser'})
        app(req.environ, self.start_response)
        self.assertTrue(self.logger.txn_id.endswith('-fromconf-fromuser'))

    def test_trans_id_header_extra_length_limit(self):
        self.assertIsNone(self.logger.txn_id)

        app = catch_errors.CatchErrorMiddleware(
            FakeApp(), {'trans_id_suffix': '-fromconf'})
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Trans-Id-Extra': 'a' * 1000})
        app(req.environ, self.start_response)
        self.assertTrue(self.logger.txn_id.endswith(
            '-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))

    def test_trans_id_header_extra_quoted(self):
        self.assertIsNone(self.logger.txn_id)

        app = catch_errors.CatchErrorMiddleware(FakeApp(), {})
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Trans-Id-Extra': 'xan than"gum'})
        app(req.environ, self.start_response)
        self.assertTrue(self.logger.txn_id.endswith('-xan%20than%22gum'))

    def test_catcherrors_with_unexpected_error(self):
        app = catch_errors.CatchErrorMiddleware(FakeApp(error='strange'), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, self.start_response)
        self.assertEqual(list(resp), [b'An error occurred'])

    def test_HEAD_with_content_length(self):
        def cannot_count_app(env, sr):
            sr("200 OK", [("Content-Length", "10")])
            return [b""]

        app = catch_errors.CatchErrorMiddleware(cannot_count_app, {})
        list(app({'REQUEST_METHOD': 'HEAD'}, self.start_response))

    def test_short_response_body(self):

        def cannot_count_app(env, sr):
            sr("200 OK", [("Content-Length", "2000")])
            return [b"our staff tailor is Euripedes Imenedes"]

        app = catch_errors.CatchErrorMiddleware(cannot_count_app, {})

        with self.assertRaises(catch_errors.BadResponseLength):
            list(app({'REQUEST_METHOD': 'GET'}, self.start_response))

    def test_long_response_body(self):
        def cannot_count_app(env, sr):
            sr("200 OK", [("Content-Length", "10")])
            return [b"our optometric firm is C.F. Eye Care"]

        app = catch_errors.CatchErrorMiddleware(cannot_count_app, {})

        with self.assertRaises(catch_errors.BadResponseLength):
            list(app({'REQUEST_METHOD': 'GET'}, self.start_response))

    def test_bogus_content_length(self):

        def bogus_cl_app(env, sr):
            sr("200 OK", [("Content-Length", "25 cm")])
            return [b"our British cutlery specialist is Sir Irving Spoon"]

        app = catch_errors.CatchErrorMiddleware(bogus_cl_app, {})
        list(app({'REQUEST_METHOD': 'GET'}, self.start_response))

    def test_no_content_length(self):

        def no_cl_app(env, sr):
            sr("200 OK", [("Content-Type", "application/names")])
            return [b"our staff statistician is Marge Inovera"]

        app = catch_errors.CatchErrorMiddleware(no_cl_app, {})
        list(app({'REQUEST_METHOD': 'GET'}, self.start_response))

    def test_multiple_content_lengths(self):

        def poly_cl_app(env, sr):
            sr("200 OK", [("Content-Length", "30"),
                          ("Content-Length", "40")])
            return [b"The head of our personal trainers is Jim Shortz"]

        app = catch_errors.CatchErrorMiddleware(poly_cl_app, {})
        list(app({'REQUEST_METHOD': 'GET'}, self.start_response))


if __name__ == '__main__':
    unittest.main()
