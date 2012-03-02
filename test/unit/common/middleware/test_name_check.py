# Copyright (c) 2012 OpenStack, LLC.
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

'''
Unit tests for Name_check filter

Created on February 29, 2012

@author: eamonn-otoole
'''

import unittest
from webob import Request, Response
from swift.common.middleware import name_check

MAX_LENGTH = 255
FORBIDDEN_CHARS = '\'\"<>`'


class FakeApp(object):

    def __call__(self, env, start_response):
        return Response(body="OK")(env, start_response)


class TestNameCheckMiddleware(unittest.TestCase):

    def setUp(self):
        self.conf = {'maximum_length': MAX_LENGTH, 'forbidden_chars':
                     FORBIDDEN_CHARS}
        self.test_check = name_check.filter_factory(self.conf)(FakeApp())

    def test_valid_length_and_character(self):
        path = '/V1.0/' + 'c' * (MAX_LENGTH - 6)
        resp = Request.blank(path, environ={'REQUEST_METHOD': 'PUT'}
                             ).get_response(self.test_check)
        self.assertEquals(resp.body, 'OK')

    def test_invalid_character(self):
        for c in self.conf['forbidden_chars']:
            path = '/V1.0/1234' + c + '5'
            resp = Request.blank(path, environ={'REQUEST_METHOD': 'PUT'}
                             ).get_response(self.test_check)
            self.assertEquals(resp.body,
                    ("Object/Container name contains forbidden chars from %s"
                    % self.conf['forbidden_chars']))
            self.assertEquals(resp.status_int, 400)

    def test_invalid_length(self):
        path = '/V1.0/' + 'c' * (MAX_LENGTH - 5)
        resp = Request.blank(path, environ={'REQUEST_METHOD': 'PUT'}
                             ).get_response(self.test_check)
        self.assertEquals(resp.body,
                    ("Object/Container name longer than the allowed maximum %s"
                    % self.conf['maximum_length']))
        self.assertEquals(resp.status_int, 400)


if __name__ == '__main__':
    unittest.main()
