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
import os

from swift.common.middleware.crypto import Crypto


class TestCrypto(unittest.TestCase):

    def setUp(self):
        self.crypto = Crypto({})

    def test_basic_crypto(self):
        value = 'encrypt me'
        key = 'objL7wjV6L79Sfs4y7dy41273l0k6Wki'
        iv = self.crypto.create_iv()
        enc_ctxt = self.crypto.create_encryption_ctxt(key, iv)
        enc_val = enc_ctxt.update(value)
        self.assertTrue(enc_val != value)
        self.assertEqual(len(value), len(enc_val))
        self.assertEqual(iv, enc_ctxt.get_iv())

        dec_ctxt = self.crypto.create_decryption_ctxt(key, iv, 0)
        dec_val = dec_ctxt.update(enc_val)
        self.assertEqual(value, dec_val,
                         'Expected value {%s} but got {%s}' % (value, dec_val))
        self.assertEqual(iv, dec_ctxt.get_iv())

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

    def test_invalid_key(self):
        iv = self.crypto.create_iv()
        for key in ('objKey', 'a' * 31, 'a' * 33, 'a' * 16, 'a' * 24):
            try:
                self.crypto.create_encryption_ctxt(key, iv)
                self.fail('Expected ValueError to be raised for key %s' % key)
            except ValueError as err:
                self.assertTrue(err.message.startswith('Invalid key'))
            try:
                self.crypto.create_decryption_ctxt(key, iv, 0)
                self.fail('Expected ValueError to be raised for key %s' % key)
            except ValueError as err:
                self.assertTrue(err.message.startswith('Invalid key'))

    def test_invalid_iv(self):
        key = 'objL7wjV6L79Sfs4y7dy41273l0k6Wki'
        iv = 'badIv'
        try:
            self.crypto.create_encryption_ctxt(key, iv)
            self.fail('Expected ValueError to be raised')
        except ValueError as err:
            self.assertTrue(err.message.startswith('Invalid nonce'))
        try:
            self.crypto.create_decryption_ctxt(key, iv, 0)
            self.fail('Expected ValueError to be raised')
        except ValueError as err:
            self.assertTrue(err.message.startswith('Invalid nonce'))

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

if __name__ == '__main__':
    unittest.main()
