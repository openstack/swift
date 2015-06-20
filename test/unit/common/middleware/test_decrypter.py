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
import base64
import json

from swift.common.middleware import decrypter
from swift.common.swob import Request, HTTPException, HTTPInternalServerError, \
    HTTPOk
from test.unit.common.middleware.crypto_helpers import FakeCrypto, md5hex, \
    fake_encrypt, fetch_crypto_keys
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts


class FakeContextThrows(object):

    @staticmethod
    def get_error_msg():
        return 'Testing context update exception'

    def update(self, chunk):
        raise HTTPInternalServerError(self.get_error_msg())


class FakeCryptoThrows(FakeCrypto):

    def create_encryption_ctxt(self, key, iv):
        return FakeContextThrows()

    def create_decryption_ctxt(self, key, iv, offset):
        return FakeContextThrows()


def get_crypto_meta():
    fc = FakeCrypto()
    return {'iv': 'someIV', 'cipher': fc.get_cipher()}


def get_crypto_meta_header(crypto_meta=None):
    if crypto_meta is None:
        crypto_meta = get_crypto_meta()
    return json.dumps({key: (base64.b64encode(value).decode()
                       if key == 'iv' else value)
                       for key, value in crypto_meta.items()})


def get_content_type():
    return 'text/plain'


@mock.patch('swift.common.middleware.decrypter.Crypto', FakeCrypto)
class TestDecrypter(unittest.TestCase):

    def test_basic_get_req(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = fake_encrypt(body)
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

    def test_get_req_hdr_decrypt_throws(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = fake_encrypt(body)
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        with mock.patch('swift.common.middleware.decrypter.Crypto',
                        FakeCryptoThrows):
            with self.assertRaises(HTTPException) as catcher:
                resp = req.get_response(decrypter.Decrypter(app, {}))
                resp.body
        self.assertEqual(catcher.exception.body,
                         FakeContextThrows.get_error_msg())

    def test_basic_head_req(self):
        env = {'REQUEST_METHOD': 'HEAD',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        app = FakeSwift()
        app.register('HEAD', '/v1/a/c/o', HTTPOk, headers={})
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.status, '200 OK')

    def test_multiseg_get_obj(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        enc_body = [fake_encrypt(chunk) for chunk in chunks]
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': sum(map(len, enc_body)),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

    def test_multiseg_get_range_obj(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        req.headers['Content-Range'] = 'bytes 3-10/17'
        chunks = ['0123', '45678', '9abcdef']
        body = ''.join(chunks)
        enc_body = [fake_encrypt(chunk) for chunk in chunks]
        enc_body = [enc_body[0][3:], enc_body[1], enc_body[2][:2]]
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': sum(map(len, enc_body)),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.body, '3456789a')
        self.assertEqual(resp.status, '200 OK')
        # TODO - how do we validate the range body if etag is for whole? Is
        # the test actually faking the correct Etag in response?
        self.assertEqual(resp.headers['Etag'], md5hex(body))

    def test_etag_no_match_on_get(self):
        self.skipTest('Etag verification not yet implemented')
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = fake_encrypt(body)
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex('not the body'),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')

    def test_missing_key_callback(self):
        # Do not provide keys, and do not set override flag
        env = {'REQUEST_METHOD': 'GET'}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = fake_encrypt(body)
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex('not the body'),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body, 'swift.crypto.fetch_crypto_keys not in env')

    def test_error_in_key_callback(self):
        def raise_exc():
            raise Exception('Testing')

        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': raise_exc}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = fake_encrypt(body)
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body, 'swift.crypto.fetch_crypto_keys had exception: Testing')

    def test_cipher_mismatch(self):
        # Cipher does nto match
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = fake_encrypt(body)
        app = FakeSwift()
        crypto_meta = get_crypto_meta()
        crypto_meta['cipher'] = 'unknown_cipher'
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex('not the body'),
                'X-Object-Sysmeta-Crypto-Meta':
                    get_crypto_meta_header(crypto_meta=crypto_meta)}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body,
            'Encrypted with cipher %s, but can only decrypt with cipher %s'
            % ('unknown_cipher', FakeCrypto().get_cipher()))

    def test_decryption_override(self):
        # This covers the case of an old un-encrypted object
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'swift.crypto.override': True}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        app = FakeSwift()
        hdrs = {'Etag': md5hex(body),
                'content-type': 'text/plain',
                'content-length': len(body)}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

    def test_filter_factory(self):
        factory = decrypter.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory(None), decrypter.Decrypter)

    def test_app_exception(self):
        app = decrypter.Decrypter(
            FakeAppThatExcepts(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(catcher.exception.body,
                         FakeAppThatExcepts.get_error_msg())


if __name__ == '__main__':
    unittest.main()
