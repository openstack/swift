# Copyright (c) 2015-2016 OpenStack Foundation
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
import os
import unittest

import mock
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from swift.common.exceptions import EncryptionException
from swift.common.middleware.crypto import crypto_utils
from swift.common.middleware.crypto.crypto_utils import (
    CRYPTO_KEY_CALLBACK, Crypto, CryptoWSGIContext)
from swift.common.swob import HTTPException
from test.unit import FakeLogger
from test.unit.common.middleware.crypto.crypto_helpers import fetch_crypto_keys


class TestCryptoWsgiContext(unittest.TestCase):
    def setUp(self):
        class FakeFilter(object):
            app = None
            crypto = Crypto({})

        self.fake_logger = FakeLogger()
        self.crypto_context = CryptoWSGIContext(
            FakeFilter(), 'object', self.fake_logger)

    def test_get_keys(self):
        # ok
        env = {CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        keys = self.crypto_context.get_keys(env)
        self.assertDictEqual(fetch_crypto_keys(), keys)

        # only default required keys are checked
        subset_keys = {'object': fetch_crypto_keys()['object']}
        env = {CRYPTO_KEY_CALLBACK: lambda: subset_keys}
        keys = self.crypto_context.get_keys(env)
        self.assertDictEqual(subset_keys, keys)

        # only specified required keys are checked
        subset_keys = {'container': fetch_crypto_keys()['container']}
        env = {CRYPTO_KEY_CALLBACK: lambda: subset_keys}
        keys = self.crypto_context.get_keys(env, required=['container'])
        self.assertDictEqual(subset_keys, keys)

        subset_keys = {'object': fetch_crypto_keys()['object'],
                       'container': fetch_crypto_keys()['container']}
        env = {CRYPTO_KEY_CALLBACK: lambda: subset_keys}
        keys = self.crypto_context.get_keys(
            env, required=['object', 'container'])
        self.assertDictEqual(subset_keys, keys)

    def test_get_keys_missing_callback(self):
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys({})
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn('missing callback',
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_get_keys_callback_exception(self):
        def callback():
            raise Exception('boom')
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys({CRYPTO_KEY_CALLBACK: callback})
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn('from callback: boom',
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_get_keys_missing_key_for_default_required_list(self):
        bad_keys = dict(fetch_crypto_keys())
        bad_keys.pop('object')
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys(
                {CRYPTO_KEY_CALLBACK: lambda: bad_keys})
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn("Missing key for 'object'",
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_get_keys_missing_object_key_for_specified_required_list(self):
        bad_keys = dict(fetch_crypto_keys())
        bad_keys.pop('object')
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys(
                {CRYPTO_KEY_CALLBACK: lambda: bad_keys},
                required=['object', 'container'])
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn("Missing key for 'object'",
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_get_keys_missing_container_key_for_specified_required_list(self):
        bad_keys = dict(fetch_crypto_keys())
        bad_keys.pop('container')
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys(
                {CRYPTO_KEY_CALLBACK: lambda: bad_keys},
                required=['object', 'container'])
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn("Missing key for 'container'",
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_bad_object_key_for_default_required_list(self):
        bad_keys = dict(fetch_crypto_keys())
        bad_keys['object'] = 'the minor key'
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys(
                {CRYPTO_KEY_CALLBACK: lambda: bad_keys})
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn("Bad key for 'object'",
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_bad_container_key_for_default_required_list(self):
        bad_keys = dict(fetch_crypto_keys())
        bad_keys['container'] = 'the major key'
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys(
                {CRYPTO_KEY_CALLBACK: lambda: bad_keys},
                required=['object', 'container'])
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn("Bad key for 'container'",
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_get_keys_not_a_dict(self):
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys(
                {CRYPTO_KEY_CALLBACK: lambda: ['key', 'quay', 'qui']})
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn("Did not get a keys dict",
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)


class TestModuleMethods(unittest.TestCase):
    meta = {'iv': '0123456789abcdef', 'cipher': 'AES_CTR_256'}
    serialized_meta = '%7B%22cipher%22%3A+%22AES_CTR_256%22%2C+%22' \
                      'iv%22%3A+%22MDEyMzQ1Njc4OWFiY2RlZg%3D%3D%22%7D'

    meta_with_key = {'iv': '0123456789abcdef', 'cipher': 'AES_CTR_256',
                     'body_key': {'key': 'fedcba9876543210fedcba9876543210',
                                  'iv': 'fedcba9876543210'}}
    serialized_meta_with_key = '%7B%22body_key%22%3A+%7B%22iv%22%3A+%22ZmVkY' \
                               '2JhOTg3NjU0MzIxMA%3D%3D%22%2C+%22key%22%3A+%' \
                               '22ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMT' \
                               'A%3D%22%7D%2C+%22cipher%22%3A+%22AES_CTR_256' \
                               '%22%2C+%22iv%22%3A+%22MDEyMzQ1Njc4OWFiY2RlZg' \
                               '%3D%3D%22%7D'

    def test_dump_crypto_meta(self):
        actual = crypto_utils.dump_crypto_meta(self.meta)
        self.assertEqual(self.serialized_meta, actual)

        actual = crypto_utils.dump_crypto_meta(self.meta_with_key)
        self.assertEqual(self.serialized_meta_with_key, actual)

    def test_load_crypto_meta(self):
        actual = crypto_utils.load_crypto_meta(self.serialized_meta)
        self.assertEqual(self.meta, actual)

        actual = crypto_utils.load_crypto_meta(self.serialized_meta_with_key)
        self.assertEqual(self.meta_with_key, actual)

        def assert_raises(value, message):
            with self.assertRaises(EncryptionException) as cm:
                crypto_utils.load_crypto_meta(value)
            self.assertIn('Bad crypto meta %r' % value, cm.exception.message)
            self.assertIn(message, cm.exception.message)

        assert_raises(None, 'crypto meta not a string')
        assert_raises(99, 'crypto meta not a string')
        assert_raises('', 'No JSON object could be decoded')
        assert_raises('abc', 'No JSON object could be decoded')
        assert_raises('[]', 'crypto meta not a Mapping')
        assert_raises('{"iv": "abcdef"}', 'Incorrect padding')
        assert_raises('{"iv": []}', 'must be string or buffer')
        assert_raises('{"iv": {}}', 'must be string or buffer')
        assert_raises('{"iv": 99}', 'must be string or buffer')
        assert_raises('{"key": "abcdef"}', 'Incorrect padding')
        assert_raises('{"key": []}', 'must be string or buffer')
        assert_raises('{"key": {}}', 'must be string or buffer')
        assert_raises('{"key": 99}', 'must be string or buffer')
        assert_raises('{"body_key": {"iv": "abcdef"}}', 'Incorrect padding')
        assert_raises('{"body_key": {"iv": []}}', 'must be string or buffer')
        assert_raises('{"body_key": {"iv": {}}}', 'must be string or buffer')
        assert_raises('{"body_key": {"iv": 99}}', 'must be string or buffer')
        assert_raises('{"body_key": {"key": "abcdef"}}', 'Incorrect padding')
        assert_raises('{"body_key": {"key": []}}', 'must be string or buffer')
        assert_raises('{"body_key": {"key": {}}}', 'must be string or buffer')
        assert_raises('{"body_key": {"key": 99}}', 'must be string or buffer')

    def test_dump_then_load_crypto_meta(self):
        actual = crypto_utils.load_crypto_meta(
            crypto_utils.dump_crypto_meta(self.meta))
        self.assertEqual(self.meta, actual)

        actual = crypto_utils.load_crypto_meta(
            crypto_utils.dump_crypto_meta(self.meta_with_key))
        self.assertEqual(self.meta_with_key, actual)

    def test_append_crypto_meta(self):
        actual = crypto_utils.append_crypto_meta('abc', self.meta)
        expected = 'abc; swift_meta=%s' % self.serialized_meta
        self.assertEqual(actual, expected)

        actual = crypto_utils.append_crypto_meta('abc', self.meta_with_key)
        expected = 'abc; swift_meta=%s' % self.serialized_meta_with_key
        self.assertEqual(actual, expected)

    def test_extract_crypto_meta(self):
        val, meta = crypto_utils.extract_crypto_meta(
            'abc; swift_meta=%s' % self.serialized_meta)
        self.assertEqual('abc', val)
        self.assertDictEqual(self.meta, meta)

        val, meta = crypto_utils.extract_crypto_meta(
            'abc; swift_meta=%s' % self.serialized_meta_with_key)
        self.assertEqual('abc', val)
        self.assertDictEqual(self.meta_with_key, meta)

        val, meta = crypto_utils.extract_crypto_meta('abc')
        self.assertEqual('abc', val)
        self.assertIsNone(meta)

        # other param names will be ignored
        val, meta = crypto_utils.extract_crypto_meta('abc; foo=bar')
        self.assertEqual('abc', val)
        self.assertIsNone(meta)

        val, meta = crypto_utils.extract_crypto_meta(
            'abc; swift_meta=%s; foo=bar' % self.serialized_meta_with_key)
        self.assertEqual('abc', val)
        self.assertDictEqual(self.meta_with_key, meta)

    def test_append_then_extract_crypto_meta(self):
        val = 'abc'
        actual = crypto_utils.extract_crypto_meta(
            crypto_utils.append_crypto_meta(val, self.meta))
        self.assertEqual((val, self.meta), actual)


class TestCrypto(unittest.TestCase):

    def setUp(self):
        self.crypto = Crypto({})

    def test_create_encryption_context(self):
        value = 'encrypt me' * 100  # more than one cipher block
        key = os.urandom(32)
        iv = os.urandom(16)
        ctxt = self.crypto.create_encryption_ctxt(key, iv)
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
        value = 'decrypt me' * 100  # more than one cipher block
        key = os.urandom(32)
        iv = os.urandom(16)
        ctxt = self.crypto.create_decryption_ctxt(key, iv, 0)
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

        with self.assertRaises(ValueError) as cm:
            self.crypto.create_decryption_ctxt(key, iv, -1)
        self.assertEqual("Offset must not be negative", cm.exception.message)

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
        chunks = ['0123456789abcdef', 'ghijklmnopqrstuv']
        key = 'objL7wjV6L79Sfs4y7dy41273l0k6Wki'
        iv = self.crypto.create_iv()
        enc_ctxt = self.crypto.create_encryption_ctxt(key, iv)
        enc_val = [enc_ctxt.update(chunk) for chunk in chunks]
        self.assertTrue(''.join(enc_val) != chunks)

        # Simulate a ranged GET from byte 19 to 32 : 'jklmnopqrstuv'
        dec_ctxt = self.crypto.create_decryption_ctxt(key, iv, 19)
        ranged_chunks = [enc_val[1][3:]]
        dec_val = [dec_ctxt.update(chunk) for chunk in ranged_chunks]
        self.assertEqual('jklmnopqrstuv', ''.join(dec_val),
                         'Expected value {%s} but got {%s}' %
                         ('jklmnopqrstuv', ''.join(dec_val)))

    def test_create_decryption_context_non_zero_offset(self):
        # Verify that iv increments for each 16 bytes of offset.
        # For a ranged GET we pass a non-zero offset so that the decrypter
        # counter is incremented to the correct value to start decrypting at
        # that offset into the object body. The counter should increment by one
        # from the starting IV value for every 16 bytes offset into the object
        # body, until it reaches 2^128 -1 when it should wrap to zero. We check
        # that is happening by verifying a decrypted value using various
        # offsets.
        key = 'objL7wjV6L79Sfs4y7dy41273l0k6Wki'

        def do_test():
            for offset, exp_iv in mappings.items():
                dec_ctxt = self.crypto.create_decryption_ctxt(key, iv, offset)
                offset_in_block = offset % 16
                cipher = Cipher(algorithms.AES(key),
                                modes.CTR(exp_iv),
                                backend=default_backend())
                expected = cipher.decryptor().update(
                    'p' * offset_in_block + 'ciphertext')
                actual = dec_ctxt.update('ciphertext')
                expected = expected[offset % 16:]
                self.assertEqual(expected, actual,
                                 'Expected %r but got %r, iv=%s and offset=%s'
                                 % (expected, actual, iv, offset))

        iv = '0000000010000000'
        mappings = {
            2: '0000000010000000',
            16: '0000000010000001',
            19: '0000000010000001',
            48: '0000000010000003',
            1024: '000000001000000p',
            5119: '000000001000001o'
        }
        do_test()

        # choose max iv value and test that it wraps to zero
        iv = chr(0xff) * 16
        mappings = {
            2: iv,
            16: str(bytearray.fromhex('00' * 16)),  # iv wraps to 0
            19: str(bytearray.fromhex('00' * 16)),
            48: str(bytearray.fromhex('00' * 15 + '02')),
            1024: str(bytearray.fromhex('00' * 15 + '3f')),
            5119: str(bytearray.fromhex('00' * 14 + '013E'))
        }
        do_test()

        iv = chr(0x0) * 16
        mappings = {
            2: iv,
            16: str(bytearray.fromhex('00' * 15 + '01')),
            19: str(bytearray.fromhex('00' * 15 + '01')),
            48: str(bytearray.fromhex('00' * 15 + '03')),
            1024: str(bytearray.fromhex('00' * 15 + '40')),
            5119: str(bytearray.fromhex('00' * 14 + '013F'))
        }
        do_test()

        iv = chr(0x0) * 8 + chr(0xff) * 8
        mappings = {
            2: iv,
            16: str(bytearray.fromhex('00' * 7 + '01' + '00' * 8)),
            19: str(bytearray.fromhex('00' * 7 + '01' + '00' * 8)),
            48: str(bytearray.fromhex('00' * 7 + '01' + '00' * 7 + '02')),
            1024: str(bytearray.fromhex('00' * 7 + '01' + '00' * 7 + '3F')),
            5119: str(bytearray.fromhex('00' * 7 + '01' + '00' * 6 + '013E'))
        }
        do_test()

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
        # crude check that we get back different values on each call
        self.assertNotEqual(self.crypto.create_iv(), self.crypto.create_iv())

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
        with mock.patch(
                'swift.common.middleware.crypto.crypto_utils.Crypto.create_iv',
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
