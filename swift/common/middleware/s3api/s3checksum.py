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

from hashlib import sha1, sha256

from swift.common.middleware.s3api.exception import \
    S3InputChecksumMismatch, S3InputChecksumTrailerInvalid
from swift.common.utils import checksum, InputProxy, strict_b64decode


CHECKSUMS_BY_HEADER = {
    'x-amz-checksum-crc32': checksum.crc32,
    'x-amz-checksum-crc32c': checksum.crc32c,
    'x-amz-checksum-crc64nvme': checksum.crc64nvme,
    'x-amz-checksum-sha1': sha1,
    'x-amz-checksum-sha256': sha256,
}


def get_checksum_hasher(header):
    """
    Return a checksum hasher for an x-amz-checksum-* header.

    :raises NotImplementedError: if the checksum algorithm is not supported
    """
    try:
        return CHECKSUMS_BY_HEADER[header]()
    except (KeyError, NotImplementedError):
        # We don't want to import s3response in this module
        # so we cannot raise S3NotImplemented here.
        raise NotImplementedError(
            'The %s algorithm is not supported.' % header)


def validate_checksum_value(checksum_hasher, b64digest):
    """Decode a checksum value with the exact size required by its hasher."""
    return strict_b64decode(
        b64digest,
        exact_size=checksum_hasher.digest_size,
    )


class ChecksummingInput(InputProxy):
    """
    wsgi.input wrapper to calculate the X-Amz-Checksum-* of the input as it's
    read. The calculated value is checked against an expected value that is
    sent in either the request headers or trailers. To allow for the latter,
    the expected value is lazy fetched once the input has been read.

    :param wsgi_input: file-like object to be wrapped.
    :param content_length: the expected number of bytes to be read.
    :param checksum_hasher: a hasher to calculate the checksum of read bytes.
    :param checksum_key: the name of the header or trailer that will have
        the expected checksum value to be checked.
    :param checksum_source: a dict that will have the ``checksum_key``.
    """

    def __init__(self, wsgi_input, content_length, checksum_hasher,
                 checksum_key, checksum_source):
        """Initialize a checksum-validating input wrapper."""
        super().__init__(wsgi_input)
        self._expected_length = content_length
        self._checksum_hasher = checksum_hasher
        self._checksum_key = checksum_key
        self._checksum_source = checksum_source

    def chunk_update(self, chunk, eof, *args, **kwargs):
        """Update and validate the checksum as input chunks are consumed."""
        # Note that "chunk" is just whatever was read from the input; this
        # says nothing about whether the underlying stream uses aws-chunked.
        self._checksum_hasher.update(chunk)
        if self.bytes_received < self._expected_length:
            # Wrapped input is likely to have timed out before this clause is
            # reached with eof=True, but just in case...
            error = eof
        elif self.bytes_received == self._expected_length:
            # Lazy fetch checksum value because it may have come in trailers.
            b64digest = self._checksum_source.get(self._checksum_key)
            try:
                expected_raw_checksum = validate_checksum_value(
                    self._checksum_hasher, b64digest)
            except ValueError:
                # Header values were validated before reading the body, so an
                # invalid value here must have come from a trailer.
                raise S3InputChecksumTrailerInvalid(self._checksum_key)
            error = self._checksum_hasher.digest() != expected_raw_checksum
        else:
            # The underlying wsgi.Input stops reading at content-length so we
            # do not expect to reach this clause, but just in case...
            error = True

        if error:
            self.close()
            # Since we do not return the last chunk, the PUT never completes.
            raise S3InputChecksumMismatch(self._checksum_hasher.name.upper())
        return chunk
