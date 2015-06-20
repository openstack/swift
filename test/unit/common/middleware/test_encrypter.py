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


import unittest
import mock

from swift.common.middleware import encrypter
from swift.common.swob import Request, HTTPException, HTTPCreated
from swift.common.utils import FileLikeIter
from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys, \
    md5hex, FakeCrypto, fake_encrypt
from test.unit.common.middleware.helpers import FakeSwift
from test.unit.common.middleware.test_proxy_logging import FakeAppThatExcepts


@mock.patch('swift.common.middleware.encrypter.Crypto', FakeCrypto)
class TestEncrypter(unittest.TestCase):

    def test_basic_put_req(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, fake_encrypt(body))

    def test_backend_response_etag_is_replaced(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated,
                     {'Etag': 'ciphertextEtag'})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

    def test_multiseg_no_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, fake_encrypt(body))

    def test_multiseg_good_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': md5hex(body)}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, fake_encrypt(body))

    def test_multiseg_bad_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': 'badclientetag'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '422 Unprocessable Entity')

    def test_missing_key_callback(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body, 'swift.crypto.fetch_crypto_keys not in env')

    def test_error_in_key_callback(self):
        def raise_exc():
            raise Exception('Testing')

        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': raise_exc}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body, 'swift.crypto.fetch_crypto_keys had exception: Testing')

    def test_encryption_override(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.override': True}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        # verify object is NOT encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, body)

    def test_filter(self):
        factory = encrypter.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory({}), encrypter.Encrypter)

    def test_app_exception(self):
        app = encrypter.Encrypter(
            FakeAppThatExcepts(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(catcher.exception.body,
                         FakeAppThatExcepts.get_error_msg())


if __name__ == '__main__':
    unittest.main()
