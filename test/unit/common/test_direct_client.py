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

# TODO: Tests

import unittest
import os

from swift.common import direct_client


class TestDirectClient(unittest.TestCase):

    def test_quote(self):
        res = direct_client.quote('123')
        assert res == '123'
        res = direct_client.quote('1&2&/3')
        assert res == '1%262%26/3'
        res = direct_client.quote('1&2&3', safe='&')
        assert res == '1&2&3'

    def test_gen_headers(self):
        hdrs = direct_client.gen_headers()
        assert 'user-agent' in hdrs
        assert hdrs['user-agent'] == 'direct-client %s' % os.getpid()
        assert len(hdrs.keys()) == 1

        hdrs = direct_client.gen_headers(add_ts=True)
        assert 'user-agent' in hdrs
        assert 'x-timestamp' in hdrs
        assert len(hdrs.keys()) == 2

        hdrs = direct_client.gen_headers(hdrs_in={'foo-bar': '47'})
        assert 'user-agent' in hdrs
        assert 'foo-bar' in hdrs
        assert hdrs['foo-bar'] == '47'
        assert len(hdrs.keys()) == 2

        hdrs = direct_client.gen_headers(hdrs_in={'user-agent': '47'})
        assert 'user-agent' in hdrs
        assert hdrs['user-agent'] == 'direct-client %s' % os.getpid()
        assert len(hdrs.keys()) == 1


if __name__ == '__main__':
    unittest.main()
