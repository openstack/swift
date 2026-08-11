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
    ChecksummingInput, get_checksum_hasher
from swift.common.middleware.s3api.exception import \
    S3InputChecksumMismatch, S3InputChecksumTrailerInvalid
from swift.common.utils import checksum


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

        with self.assertRaises(S3InputChecksumMismatch):
            wrapped.read()
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

        with self.assertRaises(S3InputChecksumMismatch):
            wrapped.read()
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

        with self.assertRaises(S3InputChecksumMismatch):
            wrapped.read()
        self.assertTrue(wrapped.wsgi_input.closed)


class TestModuleFunctions(unittest.TestCase):
    def test_get_checksum_hasher(self):
        def do_test(crc):
            hasher = get_checksum_hasher('x-amz-checksum-%s' % crc)
            self.assertEqual(crc, hasher.name)

        do_test('crc32')
        do_test('sha1')
        do_test('sha256')

        try:
            checksum._select_crc32c_impl()
        except NotImplementedError:
            # This *should* always have a kernel implementation available as
            # a fallback, but debian packaging (at least) has bumped into
            # issues with even *that* not being available before
            pass
        else:
            do_test('crc32c')

        try:
            checksum._select_crc64nvme_impl()
        except NotImplementedError:
            pass
        else:
            do_test('crc64nvme')

    def test_get_checksum_hasher_invalid(self):
        def do_test(crc):
            with self.assertRaises(NotImplementedError):
                get_checksum_hasher('x-amz-checksum-%s' % crc)

        with mock.patch.object(checksum, '_select_crc64nvme_impl',
                               side_effect=NotImplementedError):
            do_test('crc64nvme')
        do_test('nonsense')
        do_test('')


if __name__ == '__main__':
    unittest.main()
