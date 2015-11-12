# Copyright (c) 2015 OpenStack Foundation
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
import base64

import unittest

from swift.common.middleware import keymaster
from swift.common import swob
Request = swob.Request

from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts


def capture_start_response():
    calls = []

    def start_response(*args):
        calls.append(args)
    return start_response, calls


class TestKeymaster(unittest.TestCase):

    def setUp(self):
        super(TestKeymaster, self).setUp()
        self.swift = FakeSwift()

    def test_object_path(self):
        self.verify_keys_for_path(
            '/v1/a/c/o', expected_keys=('object', 'container'),
            key_id=base64.b64encode('/a/c/o'))

    def test_container_path(self):
        self.verify_keys_for_path(
            '/v1/a/c', expected_keys=('container',))

    def verify_keys_for_path(self, path, expected_keys, key_id=None):
        put_keys = None
        app = keymaster.KeyMaster(self.swift,
                                  {'encryption_root_secret': 'secret'})
        for method, resp_class, status in (
                ('PUT', swob.HTTPCreated, '201'),
                ('POST', swob.HTTPAccepted, '202'),
                ('GET', swob.HTTPOk, '200'),
                ('HEAD', swob.HTTPNoContent, '204')):
            resp_headers = {}
            if key_id is not None:
                resp_headers.update({'X-Object-Sysmeta-Crypto-Id': key_id})
            self.swift.register(method, path, resp_class, resp_headers, '')
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertTrue(calls[0][0].startswith(status))
            self.assertNotIn('swift.crypto.override', req.environ)
            self.assertIn('swift.crypto.fetch_crypto_keys', req.environ,
                          'fetch_crypto_keys not set in env')
            keys = req.environ.get('swift.crypto.fetch_crypto_keys')()
            self.assertListEqual(sorted(expected_keys), sorted(keys.keys()),
                                 '%s %s got keys %r, but expected %r'
                                 % (method, path, keys.keys(), expected_keys))
            if put_keys is not None:
                # check all key sets were consistent for this path
                self.assertDictEqual(put_keys, keys)
            else:
                put_keys = keys

    def test_object_with_different_key_id(self):
        # object was put using different path; stored key_id should be used
        # to generate keys, not the GET path
        path = '/v1/a/c/o'
        key_id = base64.b64encode('/a/c/o')
        resp_headers = {'X-Object-Sysmeta-Crypto-Id': key_id}
        # first get keys when path matches key_id
        method = 'HEAD'
        self.swift.register(method, path, swob.HTTPOk, resp_headers, '')
        app = keymaster.KeyMaster(self.swift,
                                  {'encryption_root_secret': 'secret'})
        req = Request.blank(path, environ={'REQUEST_METHOD': method})
        start_response, calls = capture_start_response()
        app(req.environ, start_response)
        self.assertEqual(1, len(calls))
        self.assertEqual('200 OK', calls[0][0])
        self.assertIn('swift.crypto.fetch_crypto_keys', req.environ)
        expected_keys = req.environ.get('swift.crypto.fetch_crypto_keys')()

        # now change path but verify that keys match key_id, not path
        path = '/v1/a/got/relocated'
        for method in ('HEAD', 'GET'):
            self.swift.register(method, path, swob.HTTPOk, resp_headers, '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('200 OK', calls[0][0])
            self.assertIn('swift.crypto.fetch_crypto_keys', req.environ)
            actual_keys = req.environ.get('swift.crypto.fetch_crypto_keys')()
            self.assertDictEqual(expected_keys, actual_keys)

    def test_object_with_no_key_id(self):
        # object was not put using keymaster so has no key id, that's ok
        for method in ('HEAD', 'GET'):
            path = '/v1/a/c/o'
            self.swift.register(method, path, swob.HTTPOk, {}, '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('200 OK', calls[0][0])
            self.assertIn('swift.crypto.override', req.environ)
            self.assertNotIn('swift.crypto.fetch_crypto_keys', req.environ)

    def test_object_with_no_key_id_but_crypto_meta(self):
        # object should have a key id if it has any
        # x-object-sysmeta-crypto-meta- header
        path = '/v1/a/c/o'
        for method in ('HEAD', 'GET'):
            self.swift.register(method, path, swob.HTTPOk,
                                {'x-object-sysmeta-crypto-meta-foo': 'gotcha'},
                                '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            # TODO change to expect 422 once FakeFooters is removed.
            # error_if_need_keys is currently disabled in keymaster
            # because of how FakeFooters works. So 422's will not currently be
            # returned when keys are 'missing' and crypto-meta is found.
            self.assertEqual('200 OK', calls[0][0])
            # self.assertEqual('422 Unprocessable Entity', calls[0][0])

        # but "crypto-meta" in other headers is ok
        path = '/v1/a/c/o'
        for method in ('HEAD', 'GET'):
            self.swift.register(method, path, swob.HTTPOk,
                                {'x-object-sysmeta-foo-crypto-meta': 'ok',
                                 'x-object-sysmeta-foo-crypto-metabolic': 'ok',
                                 'x-object-meta-crypto-meta': 'no probs',
                                 'crypto-meta': 'pas de problem'},
                                '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('200 OK', calls[0][0])

    def test_filter(self):
        factory = keymaster.filter_factory(
            {'encryption_root_secret': 'secret'})
        self.assertTrue(callable(factory))
        self.assertTrue(callable(factory(self.swift)))

    def test_app_exception(self):
        app = keymaster.KeyMaster(
            FakeAppThatExcepts(), {'encryption_root_secret': 'secret'})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        start_response, _ = capture_start_response()
        self.assertRaises(Exception, app, req.environ, start_response)

    def test_key_loaded(self):
        app = keymaster.KeyMaster(self.swift,
                                  {'encryption_root_secret': 'secret'})
        self.assertEqual(app.root_secret, 'secret')

    def test_no_root_secret_error(self):
        with self.assertRaises(ValueError) as err:
            keymaster.KeyMaster(self.swift, {})

        self.assertEqual(err.exception.message,
                         'encryption_root_secret not set in proxy-server.conf')


if __name__ == '__main__':
    unittest.main()
