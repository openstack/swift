# Copyright (c) 2026 NVIDIA
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
from io import BytesIO
import unittest
from unittest import mock

from swift.common.middleware.s3api.s3checksum import \
    ChecksummingInput, get_checksum_hasher, validate_checksum_value
from swift.common.middleware.s3api.exception import \
    S3InputChecksumMismatch, S3InputChecksumTrailerInvalid
from swift.common.utils import checksum

from test.unit import requires_crc32c, requires_crc64nvme


class TestChecksummingInput(unittest.TestCase):
    def test_matching_checksum(self):
        body = b'123456789'
        expected_checksum = base64.b64encode(
            checksum.crc32(body).digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': expected_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56789', wrapped.read())
        self.assertFalse(wrapped.wsgi_input.closed)

    def test_mismatched_checksum(self):
        body = b'123456789'
        mismatched_checksum = base64.b64encode(
            checksum.crc32(b'not the body').digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': mismatched_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumMismatch) as raised:
            wrapped.read()
        self.assertEqual(('CRC32',), raised.exception.args)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_invalid_checksum_source(self):
        body = b'123456789'
        checksum_source = {
            'x-amz-checksum-crc32': 'not-a-valid-checksum',
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumTrailerInvalid) as raised:
            wrapped.read()
        self.assertEqual('x-amz-checksum-crc32', raised.exception.trailer)

    def test_empty_body(self):
        expected_checksum = base64.b64encode(
            checksum.crc32(b'').digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': expected_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(b''), 0, checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        self.assertEqual(b'', wrapped.read())
        self.assertFalse(wrapped.wsgi_input.closed)

    def test_missing_checksum_source(self):
        # the client promised a checksum trailer but never sent one
        body = b'123456789'
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', {})

        with self.assertRaises(S3InputChecksumTrailerInvalid) as raised:
            wrapped.read()
        self.assertEqual('x-amz-checksum-crc32', raised.exception.trailer)

    def test_body_is_too_short(self):
        body = b'123456789'
        expected_checksum = base64.b64encode(
            checksum.crc32(body).digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': expected_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body) + 1, checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumMismatch) as raised:
            wrapped.read()
        self.assertEqual(('CRC32',), raised.exception.args)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_body_is_too_long(self):
        body = b'123456789'
        expected_checksum = base64.b64encode(
            checksum.crc32(body).digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': expected_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body) - 1, checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumMismatch) as raised:
            wrapped.read()
        self.assertEqual(('CRC32',), raised.exception.args)
        self.assertTrue(wrapped.wsgi_input.closed)


class TestGetChecksumHasher(unittest.TestCase):
    def _check_hasher(self, crc):
        hasher = get_checksum_hasher('x-amz-checksum-%s' % crc)
        self.assertEqual(crc, hasher.name)

    def test_get_checksum_hasher(self):
        self._check_hasher('crc32')
        self._check_hasher('sha1')
        self._check_hasher('sha256')

    @requires_crc32c
    def test_get_checksum_hasher_crc32c(self):
        self._check_hasher('crc32c')

    @requires_crc64nvme
    def test_get_checksum_hasher_crc64nvme(self):
        self._check_hasher('crc64nvme')

    def test_get_checksum_hasher_invalid(self):
        # the algorithm is not recognised
        def do_test(crc):
            with self.assertRaises(NotImplementedError):
                get_checksum_hasher('x-amz-checksum-%s' % crc)

        do_test('nonsense')
        do_test('')

    def test_get_checksum_hasher_no_implementation(self):
        # the algorithm is known but the platform has no implementation
        with mock.patch.object(checksum, '_select_crc32c_impl',
                               side_effect=NotImplementedError):
            with self.assertRaises(NotImplementedError):
                get_checksum_hasher('x-amz-checksum-crc32c')
        with mock.patch.object(checksum, '_select_crc64nvme_impl',
                               side_effect=NotImplementedError):
            with self.assertRaises(NotImplementedError):
                get_checksum_hasher('x-amz-checksum-crc64nvme')


class TestValidateChecksumValue(unittest.TestCase):
    def test_valid_value(self):
        hasher = checksum.crc32(b'123456789')
        self.assertEqual(4, len(hasher.digest()))
        b64digest = base64.b64encode(hasher.digest()).decode('ascii')
        self.assertEqual(hasher.digest(),
                         validate_checksum_value(hasher, b64digest))

    def test_wrong_size_value(self):
        hasher = checksum.crc32(b'123456789')
        b64digest = base64.b64encode(b'x' * 8).decode('ascii')
        with self.assertRaises(ValueError):
            validate_checksum_value(hasher, b64digest)

    def test_not_base64_value(self):
        hasher = checksum.crc32(b'123456789')
        with self.assertRaises(ValueError):
            validate_checksum_value(hasher, 'not-a-valid-checksum')


if __name__ == '__main__':
    unittest.main()
