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
import json
import unittest
import urllib

from swift.common.middleware import encrypter, decrypter
from swift.common.swob import Request, HTTPCreated, HTTPAccepted
from swift.common.crypto_utils import CRYPTO_KEY_CALLBACK

from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys, \
    md5hex, encrypt
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
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
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

        crypto_meta = json.loads(urllib.unquote_plus(
            encrypt_get_resp.headers['X-Object-Sysmeta-Crypto-Meta']))
        crypto_meta['iv'] = base64.b64decode(crypto_meta['iv'])
        exp_enc_body = encrypt(
            body, fetch_crypto_keys()['object'], crypto_meta['iv'])
        self.assertEqual(exp_enc_body, encrypt_get_resp.body)
        self.assertNotEqual(body, encrypt_get_resp.body)  # sanity check

        decrypt_env = {'REQUEST_METHOD': 'GET',
                       CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
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

        # do a POST update to verify updated metadata is encrypted
        env = {'REQUEST_METHOD': 'POST',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'x-object-meta-test': 'encrypt me is updated'}
        req = Request.blank('/v1/a/c/o', environ=env, headers=hdrs)
        app.register('POST', '/v1/a/c/o', HTTPAccepted, {})
        req.get_response(encrypter.Encrypter(app, {}))

        # verify the metadata header was indeed encrypted by doing a GET
        # direct to the app
        encrypt_get_resp = encrypt_get_req.get_response(app)
        crypto_meta = json.loads(urllib.unquote_plus(
            encrypt_get_resp.headers[
                'X-Object-Transient-Sysmeta-Crypto-Meta-Test']))
        crypto_meta['iv'] = base64.b64decode(crypto_meta['iv'])
        exp_header_value = base64.b64encode(encrypt(
            'encrypt me is updated', fetch_crypto_keys()['object'],
            crypto_meta['iv']))
        self.assertEqual(exp_header_value,
                         encrypt_get_resp.headers['x-object-meta-test'])

        # do a GET to verify the updated metadata is decrypted
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp_dec = req.get_response(
            decrypter.Decrypter(app, {}))
        self.assertEqual('encrypt me is updated',
                         resp_dec.headers['x-object-meta-test'])


if __name__ == '__main__':
    unittest.main()
