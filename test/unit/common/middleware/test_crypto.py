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
import mock
import unittest
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from swift.common.exceptions import EncryptionException
from swift.common.middleware.crypto import Crypto


class TestCrypto(unittest.TestCase):

    def setUp(self):
        self.crypto = Crypto({})

    def test_create_encryption_context(self):
        value = 'encrypt me' * 100  # more than one cipher block
        key = os.urandom(32)
        iv = os.urandom(16)
        ctxt = self.crypto.create_encryption_ctxt(key, iv)
        self.assertEqual(iv, ctxt.iv)
        self.assertEqual(0, ctxt.offset)
        expected = Cipher(
            algorithms.AES(key), modes.CTR(iv),
            backend=default_backend()).encryptor().update(value)
        self.assertEqual(expected, ctxt.update(value))

        for bad_iv in ('a little too long', 'too short'):
            self.assertRaises(
                ValueError, self.crypto.create_encryption_ctxt, key, bad_iv)

        for bad_key in ('objKey', 'a' * 31, 'a' * 33, 'a' * 16, 'a' * 24):
            self.assertRaises(
                ValueError, self.crypto.create_encryption_ctxt, bad_key, iv)

    def test_create_decryption_context(self):
        # TODO: add tests here for non-zero offset
        value = 'decrypt me' * 100  # more than one cipher block
        key = os.urandom(32)
        iv = os.urandom(16)
        ctxt = self.crypto.create_decryption_ctxt(key, iv, 0)
        self.assertEqual(iv, ctxt.iv)
        self.assertEqual(0, ctxt.offset)
        expected = Cipher(
            algorithms.AES(key), modes.CTR(iv),
            backend=default_backend()).decryptor().update(value)
        self.assertEqual(expected, ctxt.update(value))

        for bad_iv in ('a little too long', 'too short'):
            self.assertRaises(
                ValueError, self.crypto.create_decryption_ctxt, key, bad_iv, 0)

        for bad_key in ('objKey', 'a' * 31, 'a' * 33, 'a' * 16, 'a' * 24):
            self.assertRaises(
                ValueError, self.crypto.create_decryption_ctxt, bad_key, iv, 0)

        self.assertRaises(
            ValueError, self.crypto.create_decryption_ctxt, key, iv, -1)

    def test_enc_dec_small_chunks(self):
        self.enc_dec_chunks(['encrypt me', 'because I', 'am sensitive'])

    def test_enc_dec_large_chunks(self):
        self.enc_dec_chunks([os.urandom(65536), os.urandom(65536)])

    def enc_dec_chunks(self, chunks):
        key = 'objL7wjV6L79Sfs4y7dy41273l0k6Wki'
        iv = self.crypto.create_iv()
        enc_ctxt = self.crypto.create_encryption_ctxt(key, iv)
        enc_val = [enc_ctxt.update(chunk) for chunk in chunks]
        self.assertTrue(''.join(enc_val) != chunks)
        dec_ctxt = self.crypto.create_decryption_ctxt(key, iv, 0)
        dec_val = [dec_ctxt.update(chunk) for chunk in enc_val]
        self.assertEqual(''.join(chunks), ''.join(dec_val),
                         'Expected value {%s} but got {%s}' %
                         (''.join(chunks), ''.join(dec_val)))

    def test_decrypt_range(self):
        chunks = ['012345', '6789', 'abcdef']
        key = 'objL7wjV6L79Sfs4y7dy41273l0k6Wki'
        iv = self.crypto.create_iv()
        enc_ctxt = self.crypto.create_encryption_ctxt(key, iv)
        enc_val = [enc_ctxt.update(chunk) for chunk in chunks]
        self.assertTrue(''.join(enc_val) != chunks)

        # Simulate a ranged GET from byte 4 to 12 : '456789abc'
        dec_ctxt = self.crypto.create_decryption_ctxt(key, iv, 4)
        ranged_chunks = [enc_val[0][4:], enc_val[1], enc_val[2][:3]]
        dec_val = [dec_ctxt.update(chunk) for chunk in ranged_chunks]
        self.assertEqual('456789abc', ''.join(dec_val),
                         'Expected value {%s} but got {%s}' %
                         ('456789abc', ''.join(dec_val)))

    def test_check_key(self):
        for key in ('objKey', 'a' * 31, 'a' * 33, 'a' * 16, 'a' * 24):
            with self.assertRaises(ValueError) as cm:
                self.crypto.check_key(key)
            self.assertEqual("Key must be length 32 bytes",
                             cm.exception.message)

    def test_check_crypto_meta(self):
        meta = {'cipher': 'AES_CTR_256'}
        with self.assertRaises(EncryptionException) as cm:
            self.crypto.check_crypto_meta(meta)
        self.assertEqual("Bad crypto meta: Missing 'iv'",
                         cm.exception.message)

        for bad_iv in ('a little too long', 'too short'):
            meta['iv'] = bad_iv
            with self.assertRaises(EncryptionException) as cm:
                self.crypto.check_crypto_meta(meta)
            self.assertEqual("Bad crypto meta: IV must be length 16 bytes",
                             cm.exception.message)

        meta = {'iv': os.urandom(16)}
        with self.assertRaises(EncryptionException) as cm:
            self.crypto.check_crypto_meta(meta)
        self.assertEqual("Bad crypto meta: Missing 'cipher'",
                         cm.exception.message)

        meta['cipher'] = 'Mystery cipher'
        with self.assertRaises(EncryptionException) as cm:
            self.crypto.check_crypto_meta(meta)
        self.assertEqual("Bad crypto meta: Cipher must be AES_CTR_256",
                         cm.exception.message)

    def test_create_iv(self):
        self.assertEqual(16, len(self.crypto.create_iv()))

    def test_shrink_iv_base(self):
        base = 'base' * 5
        target_length = self.crypto.get_required_iv_length()
        self.assertGreater(len(base), target_length)

        shrunk = self.crypto.create_iv(iv_base=base)
        self.assertEqual(target_length, len(shrunk))

    def test_pad_iv_base(self):
        base = 'base'
        target_length = self.crypto.get_required_iv_length()
        self.assertLess(len(base), target_length)

        padded = self.crypto.create_iv(iv_base=base)
        self.assertEqual(target_length, len(padded))

    def test_good_iv_base(self):
        target_length = self.crypto.get_required_iv_length()
        base = '1' * target_length
        self.assertEqual(target_length, len(base))

        same = self.crypto.create_iv(iv_base=base)
        self.assertEqual(base, same)

    def test_get_crypto_meta(self):
        meta = self.crypto.create_crypto_meta()
        self.assertIsInstance(meta, dict)
        # this is deliberately brittle so that if new items are added then the
        # test will need to be updated
        self.assertEqual(2, len(meta))
        self.assertIn('iv', meta)
        self.assertEqual(16, len(meta['iv']))
        self.assertIn('cipher', meta)
        self.assertEqual('AES_CTR_256', meta['cipher'])
        self.crypto.check_crypto_meta(meta)  # sanity check
        meta2 = self.crypto.create_crypto_meta()
        self.assertNotEqual(meta['iv'], meta2['iv'])  # crude sanity check

    def test_create_random_key(self):
        # crude check that we get unique keys on each call
        keys = set()
        for i in range(10):
            key = self.crypto.create_random_key()
            self.assertEqual(32, len(key))
            keys.add(key)
        self.assertEqual(10, len(keys))

    def test_wrap_unwrap_key(self):
        wrapping_key = os.urandom(32)
        key_to_wrap = os.urandom(32)
        iv = os.urandom(16)
        with mock.patch('swift.common.middleware.crypto.Crypto._get_random_iv',
                        return_value=iv):
            wrapped = self.crypto.wrap_key(wrapping_key, key_to_wrap)
        cipher = Cipher(algorithms.AES(wrapping_key), modes.CTR(iv),
                        backend=default_backend())
        expected = {'key': cipher.encryptor().update(key_to_wrap),
                    'iv': iv}
        self.assertEqual(expected, wrapped)

        unwrapped = self.crypto.unwrap_key(wrapping_key, wrapped)
        self.assertEqual(key_to_wrap, unwrapped)

    def test_unwrap_bad_key(self):
        # verify that ValueError is raised if unwrapped key is invalid
        wrapping_key = os.urandom(32)
        for length in (0, 16, 24, 31, 33):
            key_to_wrap = os.urandom(length)
            wrapped = self.crypto.wrap_key(wrapping_key, key_to_wrap)
            with self.assertRaises(ValueError) as cm:
                self.crypto.unwrap_key(wrapping_key, wrapped)
            self.assertEqual(
                cm.exception.message, 'Key must be length 32 bytes')


if __name__ == '__main__':
    unittest.main()
