# Copyright (c) 2016-2017 OpenStack Foundation
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

from swift.common.middleware import copy, proxy_logging
from swift.common.swob import Request, HTTPOk
from swift.common.utils import close_if_possible
from swift.common.wsgi import make_subrequest
from test.debug_logger import debug_logger
from test.unit.common.middleware.helpers import FakeSwift


SUB_GET_PATH = '/v1/a/c/sub_get'
SUB_PUT_POST_PATH = '/v1/a/c/sub_put'


class FakeFilter(object):
    def __init__(self, app, conf, register):
        self.body = ['FAKE MIDDLEWARE']
        self.conf = conf
        self.app = app
        self.register = register
        self.logger = None

    def __call__(self, env, start_response):
        path = SUB_PUT_POST_PATH
        if env['REQUEST_METHOD'] == 'GET':
            path = SUB_GET_PATH

        # Make a subrequest that will be logged
        hdrs = {'content-type': 'text/plain'}
        sub_req = make_subrequest(env, path=path,
                                  method=self.conf['subrequest_type'],
                                  headers=hdrs,
                                  agent='FakeApp',
                                  swift_source='FA')
        self.register(self.conf['subrequest_type'],
                      path, HTTPOk, headers=hdrs)

        resp = sub_req.get_response(self.app)
        close_if_possible(resp.app_iter)

        return self.app(env, start_response)


class FakeApp(object):
    def __init__(self, conf):
        self.fake_logger = debug_logger()
        self.fake_swift = self.app = FakeSwift()
        self.register = self.fake_swift.register
        for filter in reversed([
                proxy_logging.filter_factory,
                copy.filter_factory,
                lambda conf: lambda app: FakeFilter(app, conf, self.register),
                proxy_logging.filter_factory]):
            self.app = filter(conf)(self.app)
            self.app.logger = self.fake_logger
            if hasattr(self.app, 'access_logger'):
                self.app.access_logger = self.fake_logger

        if conf['subrequest_type'] == 'GET':
            self.register(conf['subrequest_type'], SUB_GET_PATH, HTTPOk, {})
        else:
            self.register(conf['subrequest_type'],
                          SUB_PUT_POST_PATH, HTTPOk, {})

    @property
    def __call__(self):
        return self.app.__call__


class TestSubRequestLogging(unittest.TestCase):
    path = '/v1/a/c/o'

    def _test_subrequest_logged(self, subrequest_type):
        # Test that subrequests made downstream from Copy PUT will be logged
        # with the request type of the subrequest as opposed to the GET/PUT.

        app = FakeApp({'subrequest_type': subrequest_type})

        hdrs = {'content-type': 'text/plain', 'X-Copy-From': 'test/obj'}
        req = Request.blank(self.path, method='PUT', headers=hdrs)

        app.register('PUT', self.path, HTTPOk, headers=hdrs)
        app.register('GET', '/v1/a/test/obj', HTTPOk, headers=hdrs)

        req.get_response(app)
        info_log_lines = app.fake_logger.get_lines_for_level('info')
        self.assertEqual(len(info_log_lines), 4)
        subreq_get = '%s %s' % (subrequest_type, SUB_GET_PATH)
        subreq_put = '%s %s' % (subrequest_type, SUB_PUT_POST_PATH)
        origput = 'PUT %s' % self.path
        copyget = 'GET %s' % '/v1/a/test/obj'
        # expect GET subreq, copy GET, PUT subreq, orig PUT
        self.assertTrue(subreq_get in info_log_lines[0])
        self.assertTrue(copyget in info_log_lines[1])
        self.assertTrue(subreq_put in info_log_lines[2])
        self.assertTrue(origput in info_log_lines[3])

    def test_subrequest_logged_x_copy_from(self):
        self._test_subrequest_logged('HEAD')
        self._test_subrequest_logged('GET')
        self._test_subrequest_logged('POST')
        self._test_subrequest_logged('PUT')
        self._test_subrequest_logged('DELETE')

    def _test_subrequest_logged_POST(self, subrequest_type):
        app = FakeApp({'subrequest_type': subrequest_type})

        hdrs = {'content-type': 'text/plain'}
        req = Request.blank(self.path, method='POST', headers=hdrs)

        app.register('POST', self.path, HTTPOk, headers=hdrs)
        expect_lines = 2

        req.get_response(app)
        info_log_lines = app.fake_logger.get_lines_for_level('info')
        self.assertEqual(len(info_log_lines), expect_lines)
        self.assertTrue('Copying object' not in info_log_lines[0])

        subreq_put_post = '%s %s' % (subrequest_type, SUB_PUT_POST_PATH)
        origpost = 'POST %s' % self.path

        # fast post expect POST subreq, original POST
        self.assertTrue(subreq_put_post in info_log_lines[0])
        self.assertTrue(origpost in info_log_lines[1])

    def test_subrequest_logged_with_POST(self):
        self._test_subrequest_logged_POST('HEAD')
        self._test_subrequest_logged_POST('GET')
        self._test_subrequest_logged_POST('POST')
        self._test_subrequest_logged_POST('PUT')
        self._test_subrequest_logged_POST('DELETE')


if __name__ == '__main__':
    unittest.main()
