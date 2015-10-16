#!/usr/bin/python -u
# Copyright (c) 2010-2013 OpenStack Foundation
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

import re
import unittest

from six.moves import http_client
from six.moves.urllib.parse import urlparse
from swiftclient import get_auth
from test.probe.common import ReplProbeTest


class TestAccountGetFakeResponsesMatch(ReplProbeTest):

    def setUp(self):
        super(TestAccountGetFakeResponsesMatch, self).setUp()
        self.url, self.token = get_auth(
            'http://127.0.0.1:8080/auth/v1.0', 'admin:admin', 'admin')

    def _account_path(self, account):
        _, _, path, _, _, _ = urlparse(self.url)

        basepath, _ = path.rsplit('/', 1)
        return basepath + '/' + account

    def _get(self, *a, **kw):
        kw['method'] = 'GET'
        return self._account_request(*a, **kw)

    def _account_request(self, account, method, headers=None):
        if headers is None:
            headers = {}
        headers['X-Auth-Token'] = self.token

        scheme, netloc, path, _, _, _ = urlparse(self.url)
        host, port = netloc.split(':')
        port = int(port)

        conn = http_client.HTTPConnection(host, port)
        conn.request(method, self._account_path(account), headers=headers)
        resp = conn.getresponse()
        if resp.status // 100 != 2:
            raise Exception("Unexpected status %s\n%s" %
                            (resp.status, resp.read()))

        response_headers = dict(resp.getheaders())
        response_body = resp.read()
        resp.close()
        return response_headers, response_body

    def test_main(self):
        # Two accounts: "real" and "fake". The fake one doesn't have any .db
        # files on disk; the real one does. The real one is empty.
        #
        # Make sure the important response fields match.

        real_acct = "AUTH_real"
        fake_acct = "AUTH_fake"

        self._account_request(real_acct, 'POST',
                              {'X-Account-Meta-Bert': 'Ernie'})

        # text
        real_headers, real_body = self._get(real_acct)
        fake_headers, fake_body = self._get(fake_acct)

        self.assertEqual(real_body, fake_body)
        self.assertEqual(real_headers['content-type'],
                         fake_headers['content-type'])

        # json
        real_headers, real_body = self._get(
            real_acct, headers={'Accept': 'application/json'})
        fake_headers, fake_body = self._get(
            fake_acct, headers={'Accept': 'application/json'})

        self.assertEqual(real_body, fake_body)
        self.assertEqual(real_headers['content-type'],
                         fake_headers['content-type'])

        # xml
        real_headers, real_body = self._get(
            real_acct, headers={'Accept': 'application/xml'})
        fake_headers, fake_body = self._get(
            fake_acct, headers={'Accept': 'application/xml'})

        # the account name is in the XML response
        real_body = re.sub('AUTH_\w{4}', 'AUTH_someaccount', real_body)
        fake_body = re.sub('AUTH_\w{4}', 'AUTH_someaccount', fake_body)

        self.assertEqual(real_body, fake_body)
        self.assertEqual(real_headers['content-type'],
                         fake_headers['content-type'])


if __name__ == '__main__':
    unittest.main()
