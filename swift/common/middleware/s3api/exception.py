# Copyright (c) 2014 OpenStack Foundation.
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


class S3Exception(Exception):
    pass


class NotS3Request(S3Exception):
    pass


class ACLError(S3Exception):
    pass


class InvalidSubresource(S3Exception):
    def __init__(self, resource, cause):
        self.resource = resource
        self.cause = cause


class S3InputError(BaseException):
    """
    There was an error with the client input detected on read().

    Inherit from BaseException (rather than Exception) so it cuts from the
    proxy-server app (which will presumably be the one reading the input)
    through all the layers of the pipeline back to s3api. It should never
    escape the s3api middleware.
    """


class S3InputIncomplete(S3InputError):
    pass


class S3InputSizeError(S3InputError):
    def __init__(self, expected, provided):
        self.expected = expected
        self.provided = provided


class S3InputChunkTooSmall(S3InputError):
    def __init__(self, bad_chunk_size, chunk_number):
        self.bad_chunk_size = bad_chunk_size
        self.chunk_number = chunk_number


class S3InputMalformedTrailer(S3InputError):
    pass


class S3InputChunkSignatureMismatch(S3InputError):
    """
    Client provided a chunk-signature, but it doesn't match the data.

    This should result in a 403 going back to the client.
    """


class S3InputMissingSecret(S3InputError):
    """
    Client provided per-chunk signatures, but we have no secret with which to
    verify them.

    This happens if the auth middleware responsible for the user never called
    the provided ``check_signature`` callback.
    """


class S3InputSHA256Mismatch(S3InputError):
    """
    Client provided a X-Amz-Content-SHA256, but it doesn't match the data.

    This should result in a BadDigest going back to the client.
    """
    def __init__(self, expected, computed):
        self.expected = expected
        self.computed = computed


class S3InputChecksumMismatch(S3InputError):
    """
    Client provided a X-Amz-Checksum-* header, but it doesn't match the data.

    This should result in a InvalidRequest going back to the client.
    """


class S3InputChecksumTrailerInvalid(S3InputError):
    """
    Client provided a X-Amz-Checksum-* trailer, but it is not a valid format.

    This should result in a InvalidRequest going back to the client.
    """
    def __init__(self, trailer_name):
        self.trailer = trailer_name
