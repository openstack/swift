# Copyright (c) 2016 OpenStack Foundation
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

from swift.common import crypto_utils
from swift.common.crypto_utils import CRYPTO_KEY_CALLBACK
from swift.common.middleware.crypto import Crypto
from swift.common.swob import HTTPException
from test.unit import FakeLogger
from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys


class TestCryptoWsgiContext(unittest.TestCase):
    def setUp(self):
        class FakeFilter(object):
            app = None
            crypto = Crypto({})

        self.fake_logger = FakeLogger()
        self.crypto_context = crypto_utils.CryptoWSGIContext(
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
        self.assertIn('%s not in env' % CRYPTO_KEY_CALLBACK,
                      self.fake_logger.get_lines_for_level('error')[0])
        self.assertIn('Unable to retrieve encryption keys.', cm.exception.body)

    def test_get_keys_callback_exception(self):
        def callback():
            raise Exception('boom')
        with self.assertRaises(HTTPException) as cm:
            self.crypto_context.get_keys({CRYPTO_KEY_CALLBACK: callback})
        self.assertIn('500 Internal Error', cm.exception.message)
        self.assertIn('from %s: boom' % CRYPTO_KEY_CALLBACK,
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

    def test_append_then_extract_crypto_meta(self):
        val = 'abc'
        actual = crypto_utils.extract_crypto_meta(
            crypto_utils.append_crypto_meta(val, self.meta))
        self.assertEqual((val, self.meta), actual)
