# Copyright (c) 2013 OpenStack Foundation.
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

from swift.common.middleware import crossdomain


class FakeApp(object):

    def __call__(self, env, start_response):
        return b"FAKE APP"


def start_response(*args):
    pass


class TestCrossDomain(unittest.TestCase):

    def setUp(self):
        self.app = crossdomain.CrossDomainMiddleware(FakeApp(), {})

    # GET of /crossdomain.xml (default)
    def test_crossdomain_default(self):
        expectedResponse = b'<?xml version="1.0"?>\n' \
            b'<!DOCTYPE cross-domain-policy SYSTEM ' \
            b'"http://www.adobe.com/xml/dtds/cross-domain-policy.dtd" >\n' \
            b'<cross-domain-policy>\n' \
            b'<allow-access-from domain="*" secure="false" />\n' \
            b'</cross-domain-policy>'

        req = Request.blank('/crossdomain.xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [expectedResponse])

    # GET of /crossdomain.xml (custom)
    def test_crossdomain_custom(self):
        conf = {'cross_domain_policy': '<dummy 1>\n<dummy 2>'}
        self.app = crossdomain.CrossDomainMiddleware(FakeApp(), conf)
        expectedResponse = b'<?xml version="1.0"?>\n' \
            b'<!DOCTYPE cross-domain-policy SYSTEM ' \
            b'"http://www.adobe.com/xml/dtds/cross-domain-policy.dtd" >\n' \
            b'<cross-domain-policy>\n' \
            b'<dummy 1>\n' \
            b'<dummy 2>\n' \
            b'</cross-domain-policy>'

        req = Request.blank('/crossdomain.xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [expectedResponse])

    # GET to a different resource should be passed on
    def test_crossdomain_pass(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, b'FAKE APP')

    # Only GET is allowed on the /crossdomain.xml resource
    def test_crossdomain_get_only(self):
        for method in ['HEAD', 'PUT', 'POST', 'COPY', 'OPTIONS']:
            req = Request.blank('/crossdomain.xml',
                                environ={'REQUEST_METHOD': method})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, b'FAKE APP')


if __name__ == '__main__':
    unittest.main()
