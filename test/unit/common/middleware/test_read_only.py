# Copyright (c) 2010-2015 OpenStack Foundation
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

from unittest import mock
import unittest

from swift.common.middleware import read_only
from swift.common.swob import Request
from test.debug_logger import debug_logger


class FakeApp(object):
    def __call__(self, env, start_response):
        start_response('200 OK', [])
        return [b'Some Content']


def start_response(*args):
    pass


read_methods = 'GET HEAD'.split()
write_methods = 'COPY DELETE POST PUT'.split()
ro_resp = [b'Writes are disabled for this account.']


class TestReadOnly(unittest.TestCase):
    def test_global_read_only_off(self):
        conf = {
            'read_only': 'false',
        }

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={}):
            for method in read_methods + write_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(resp, [b'Some Content'])

    def test_global_read_only_on(self):
        conf = {
            'read_only': 'true',
        }

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={}):
            for method in read_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(resp, [b'Some Content'])

            for method in write_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(ro_resp, resp)

    def test_account_read_only_on(self):
        conf = {}

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={'sysmeta': {'read-only': 'true'}}):
            for method in read_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(resp, [b'Some Content'])

            for method in write_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(ro_resp, resp)

    def test_account_read_only_off(self):
        conf = {}

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={'sysmeta': {'read-only': 'false'}}):
            for method in read_methods + write_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(resp, [b'Some Content'])

    def test_global_read_only_on_account_off(self):
        conf = {
            'read_only': 'true',
        }

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={'sysmeta': {'read-only': 'false'}}):
            for method in read_methods + write_methods:
                req = Request.blank('/v1/a')
                req.method = method
                resp = ro(req.environ, start_response)
                self.assertEqual(resp, [b'Some Content'])

    def test_global_read_only_on_allow_deletes(self):
        conf = {
            'read_only': 'true',
            'allow_deletes': 'true',
        }

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={}):
            req = Request.blank('/v1/a')
            req.method = "DELETE"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])

    def test_account_read_only_on_allow_deletes(self):
        conf = {
            'allow_deletes': 'true',
        }

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch('swift.common.middleware.read_only.get_info',
                        return_value={'sysmeta': {'read-only': 'on'}}):
            req = Request.blank('/v1/a')
            req.method = "DELETE"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])

    def test_global_read_only_on_destination_account_off_on_copy(self):
        conf = {
            'read_only': 'true',
        }

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        def get_fake_read_only(*args, **kwargs):
            if 'b' in args:
                return {'sysmeta': {'read-only': 'false'}}
            return {}

        with mock.patch('swift.common.middleware.read_only.get_info',
                        get_fake_read_only):
            headers = {'Destination-Account': 'b'}
            req = Request.blank('/v1/a', headers=headers)
            req.method = "COPY"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])

    def test_global_read_only_off_destination_account_on_on_copy(self):
        conf = {}

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        def get_fake_read_only(*args, **kwargs):
            if 'b' in args:
                return {'sysmeta': {'read-only': 'true'}}
            return {}

        with mock.patch('swift.common.middleware.read_only.get_info',
                        get_fake_read_only):
            headers = {'Destination-Account': 'b'}
            req = Request.blank('/v1/a', headers=headers)
            req.method = "COPY"
            resp = ro(req.environ, start_response)
            self.assertEqual(ro_resp, resp)

    def test_global_read_only_off_src_acct_on_dest_acct_off_on_copy(self):
        conf = {}

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        def fake_account_read_only(self, req, account):
            if account == 'a':
                return 'on'
            return ''

        with mock.patch(
                'swift.common.middleware.read_only.ReadOnlyMiddleware.' +
                'account_read_only',
                fake_account_read_only):
            headers = {'Destination-Account': 'b'}
            req = Request.blank('/v1/a', headers=headers)
            req.method = "COPY"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])

    def test_global_read_only_off_src_acct_on_dest_acct_on_on_copy(self):
        conf = {}

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        with mock.patch(
                'swift.common.middleware.read_only.ReadOnlyMiddleware.' +
                'account_read_only',
                return_value='true'):
            headers = {'Destination-Account': 'b'}
            req = Request.blank('/v1/a', headers=headers)
            req.method = "COPY"
            resp = ro(req.environ, start_response)
            self.assertEqual(ro_resp, resp)

    def test_global_read_only_non_swift_path(self):
        conf = {}

        ro = read_only.filter_factory(conf)(FakeApp())
        ro.logger = debug_logger()

        def fake_account_read_only(self, req, account):
            return 'on'

        with mock.patch(
                'swift.common.middleware.read_only.ReadOnlyMiddleware.' +
                'account_read_only',
                fake_account_read_only):
            req = Request.blank('/auth/v3.14')
            req.method = "POST"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])

            req = Request.blank('/v1')
            req.method = "PUT"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])

            req = Request.blank('/v1.0/')
            req.method = "DELETE"
            resp = ro(req.environ, start_response)
            self.assertEqual(resp, [b'Some Content'])


if __name__ == '__main__':
    unittest.main()
