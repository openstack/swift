# Copyright (c) 2022 NVIDIA
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
import binascii
import hmac

from swift.common.utils import strict_b64decode


DEFAULT_ALLOWED_DIGESTS = 'sha1 sha256 sha512'
DEPRECATED_DIGESTS = {'sha1'}
SUPPORTED_DIGESTS = set(DEFAULT_ALLOWED_DIGESTS.split()) | DEPRECATED_DIGESTS


def get_hmac(request_method, path, expires, key, digest="sha1",
             ip_range=None):
    """
    Returns the hexdigest string of the HMAC (see RFC 2104) for
    the request.

    :param request_method: Request method to allow.
    :param path: The path to the resource to allow access to.
    :param expires: Unix timestamp as an int for when the URL
                    expires.
    :param key: HMAC shared secret.
    :param digest: constructor or the string name for the digest to use in
                   calculating the HMAC
                   Defaults to SHA1
    :param ip_range: The ip range from which the resource is allowed
                     to be accessed. We need to put the ip_range as the
                     first argument to hmac to avoid manipulation of the path
                     due to newlines being valid in paths
                     e.g. /v1/a/c/o\\n127.0.0.1
    :returns: hexdigest str of the HMAC for the request using the specified
              digest algorithm.
    """
    # These are the three mandatory fields.
    parts = [request_method, str(expires), path]
    formats = [b"%s", b"%s", b"%s"]

    if ip_range:
        parts.insert(0, ip_range)
        formats.insert(0, b"ip=%s")

    if isinstance(key, str):
        key = key.encode('utf8')

    message = b'\n'.join(
        fmt % (part if isinstance(part, bytes)
               else part.encode("utf-8"))
        for fmt, part in zip(formats, parts))

    return hmac.new(key, message, digest).hexdigest()


def get_allowed_digests(conf_digests, logger=None):
    """
    Pulls out 'allowed_digests' from the supplied conf. Then compares them with
    the list of supported and deprecated digests and returns whatever remain.

    When something is unsupported or deprecated it'll log a warning.

    :param conf_digests: iterable of allowed digests. If empty, defaults to
        DEFAULT_ALLOWED_DIGESTS.
    :param logger: optional logger; if provided, use it issue deprecation
        warnings
    :returns: A set of allowed digests that are supported and a set of
        deprecated digests.
    :raises: ValueError, if there are no digests left to return.
    """
    allowed_digests = set(digest.lower() for digest in conf_digests)
    if not allowed_digests:
        allowed_digests = SUPPORTED_DIGESTS

    not_supported = allowed_digests - SUPPORTED_DIGESTS
    if not_supported:
        if logger:
            logger.warning('The following digest algorithms are configured '
                           'but not supported: %s', ', '.join(not_supported))
        allowed_digests -= not_supported
    deprecated = allowed_digests & DEPRECATED_DIGESTS
    if deprecated and logger:
        if not conf_digests:
            logger.warning('The following digest algorithms are allowed by '
                           'default but deprecated: %s. Support will be '
                           'disabled by default in a future release, and '
                           'later removed entirely.', ', '.join(deprecated))
        else:
            logger.warning('The following digest algorithms are configured '
                           'but deprecated: %s. Support will be removed in a '
                           'future release.', ', '.join(deprecated))
    if not allowed_digests:
        raise ValueError('No valid digest algorithms are configured')

    return allowed_digests, deprecated


def extract_digest_and_algorithm(value):
    """
    Returns a tuple of (digest_algorithm, hex_encoded_digest)
    from a client-provided string of the form::

       <hex-encoded digest>

    or::

       <algorithm>:<base64-encoded digest>

    Note that hex-encoded strings must use one of sha1, sha256, or sha512.

    :raises: ValueError on parse failures
    """
    if ':' in value:
        algo, value = value.split(':', 1)
        # accept both standard and url-safe base64
        if ('-' in value or '_' in value) and not (
                '+' in value or '/' in value):
            value = value.replace('-', '+').replace('_', '/')
        value = binascii.hexlify(
            strict_b64decode(value + '==')).decode('ascii')
    else:
        binascii.unhexlify(value)  # make sure it decodes
        algo = {
            40: 'sha1',
            64: 'sha256',
            128: 'sha512',
        }.get(len(value))
        if not algo:
            raise ValueError('Bad digest length')
    return algo, value
