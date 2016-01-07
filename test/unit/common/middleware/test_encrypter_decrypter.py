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
from swift.common.middleware.crypto import Crypto

from swift.common.middleware import encrypter, decrypter
from swift.common.swob import Request, HTTPCreated

from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys, \
    md5hex
from test.unit.common.middleware.helpers import FakeSwift


class TestEncrypterDecrypter(unittest.TestCase):
    """
    Unit tests to verify round-trip encryption followed by decryption.
    These tests serve to complement the separate unit tests for encrypter and
    decrypter, which test each in isolation.
    However, the real Crypto implementation is used.
    """

    def test_basic_put_get_req(self):
        # Setup pass the PUT request through the encrypter.
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        req.get_response(encrypter.Encrypter(app, {}))

        # Verify that at least the request body was indeed encrypted.
        # Otherwise, checking that input matches output after decryption is
        # not sufficient if encryption and decryption were both just the
        # identity function! (i.e. if they did nothing)
        encrypt_get_req = Request.blank('/v1/a/c/o',
                                        environ={'REQUEST_METHOD': 'GET'})
        encrypt_get_resp = encrypt_get_req.get_response(app)

        exp_enc_body = self.encrypt_value(
            body, fetch_crypto_keys()['object'],
            encrypt_get_resp.headers['X-Object-Sysmeta-Crypto-Meta'])
        self.assertEqual(exp_enc_body, encrypt_get_resp.body)

        decrypt_env = {'REQUEST_METHOD': 'GET',
                       'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        decrypt_req = Request.blank('/v1/a/c/o', environ=decrypt_env)
        decrypt_resp = decrypt_req.get_response(
            decrypter.Decrypter(app, {}))

        self.assertEqual(body, decrypt_resp.body)
        self.assertEqual('200 OK', decrypt_resp.status)
        self.assertEqual('text/plain', decrypt_resp.headers['Content-Type'])
        self.assertEqual(md5hex(body), decrypt_resp.headers['Etag'])
        self.assertEqual('encrypt me',
                         decrypt_resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         decrypt_resp.headers['x-object-sysmeta-test'])

    def encrypt_value(self, value, key, crypto_meta_json):
        crypto_meta = decrypter._load_crypto_meta(crypto_meta_json)
        enc_ctxt = Crypto({}).create_encryption_ctxt(
            key, crypto_meta.get('iv'))
        return enc_ctxt.update(value)

if __name__ == '__main__':
    unittest.main()
