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
import binascii
import json
import os
import urllib
from hashlib import md5

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from swift import gettext_ as _
from swift.common.exceptions import EncryptionException
from swift.common.request_helpers import strip_sys_meta_prefix, \
    strip_object_transient_sysmeta_prefix
from swift.common.swob import HTTPInternalServerError
from swift.common.utils import get_logger
from swift.common.wsgi import WSGIContext

CRYPTO_KEY_CALLBACK = 'swift.callback.fetch_crypto_keys'


# AES will accept several key sizes - we are using 256 bits i.e. 32 bytes
KEY_LENGTH = 32


class Crypto(object):
    """
    Used by middleware: Calls crypto alg
    """
    def __init__(self, conf=None):
        conf = {} if conf is None else conf
        self.logger = get_logger(conf, log_route="crypto")

    def create_encryption_ctxt(self, key, iv):
        """
        Creates a crypto context for encrypting

        :param key: 256-bit key
        :param iv: 128-bit iv or nonce used for encryption
        :raises: ValueError on invalid key or iv
        :returns: an instance of :class:`CryptoContext`
        """
        self.check_key(key)
        engine = Cipher(algorithms.AES(key), modes.CTR(iv),
                        backend=default_backend())
        enc = engine.encryptor()
        return CryptoContext(enc, iv, 0)

    def create_decryption_ctxt(self, key, iv, offset):
        """
        Creates a crypto context for decrypting

        :param key: 256-bit key
        :param iv: 128-bit iv or nonce used for decryption
        :param offset: offset into the message; used for range reads
        :returns: an instance of :class:`CryptoContext`
        """
        self.check_key(key)
        if offset < 0:
            raise ValueError('Offset must not be negative')
        if offset > 0:
            # Adjust IV so that it is correct for decryption at offset.
            # ( 1<< (16 *8)) is to make 'ivl' big enough so that the following
            # bytearray.fromhex() can be successful in all conditions.
            ivl = long(binascii.hexlify(iv), 16)
            ivl += int(offset / 16) + (1 << (16 * 8))
            ivstr = format(ivl, 'x')
            iv = str(bytearray.fromhex(ivstr[(len(ivstr) - 2 * 16):]))

        engine = Cipher(algorithms.AES(key), modes.CTR(iv),
                        backend=default_backend())
        dec = engine.decryptor()
        # Adjust decryption boundary to AES block size of 16 bytes
        dec.update('*' * (offset % 16))
        return CryptoContext(dec, iv, offset)

    def get_required_iv_length(self):
        return algorithms.AES.block_size / 8

    def _get_derived_iv(self, base):
        target_length = self.get_required_iv_length()
        if len(base) < target_length:
            return base.zfill(target_length)
        elif len(base) > target_length:
            hash = md5()
            hash.update(base)
            return hash.hexdigest()[-target_length:]
        else:
            return base

    def _get_random_iv(self):
        # this method is separated out here so that tests can mock it
        return os.urandom(self.get_required_iv_length())

    def create_iv(self, iv_base=None):
        if iv_base:
            return self._get_derived_iv(iv_base)
        return self._get_random_iv()

    def get_cipher(self):
        return 'AES_CTR_256'

    def create_crypto_meta(self, iv_base=None):
        # create a set of parameters
        return {'iv': self.create_iv(iv_base), 'cipher': self.get_cipher()}

    def check_crypto_meta(self, meta):
        """
        Check that crypto meta dict has valid items.

        :param meta: a dict
        :raises EncryptionException: if an error is found in the crypto meta
        """
        try:
            if meta['cipher'] != self.get_cipher():
                raise EncryptionException('Bad crypto meta: Cipher must be %s'
                                          % self.get_cipher())
            if len(meta['iv']) != self.get_required_iv_length():
                raise EncryptionException(
                    'Bad crypto meta: IV must be length %s bytes'
                    % self.get_required_iv_length())
        except KeyError as err:
            raise EncryptionException(
                'Bad crypto meta: Missing %s' % err)

    def create_random_key(self):
        # helper method to create random key of correct length
        return os.urandom(KEY_LENGTH)

    def wrap_key(self, wrapping_key, key_to_wrap):
        # we don't use an RFC 3394 key wrap algorithm such as cryptography's
        # aes_wrap_key because it's slower and we have iv material readily
        # available so don't need a deterministic algorithm
        iv = self._get_random_iv()
        encryptor = Cipher(algorithms.AES(wrapping_key), modes.CTR(iv),
                           backend=default_backend()).encryptor()
        return {'key': encryptor.update(key_to_wrap), 'iv': iv}

    def unwrap_key(self, wrapping_key, context):
        # unwrap a key from dict of form returned by wrap_key
        # check the key length early - unwrapping won't change the length
        self.check_key(context['key'])
        decryptor = Cipher(algorithms.AES(wrapping_key),
                           modes.CTR(context['iv']),
                           backend=default_backend()).decryptor()
        return decryptor.update(context['key'])

    def check_key(self, key):
        if len(key) != KEY_LENGTH:
            raise ValueError("Key must be length %s bytes" % KEY_LENGTH)


class CryptoContext(object):
    """
    Crypto context used in encryption middleware.  Created by calling
    :func:`create_encryption_ctxt` or :func:`create_decryption_ctxt`.
    """
    def __init__(self, engine, iv, offset):
        self.engine = engine
        self.iv = iv
        self.offset = offset

    def update(self, chunk):
        return self.engine.update(chunk)


class CryptoWSGIContext(WSGIContext):
    """
    Base class for contexts used by crypto middlewares.
    """
    def __init__(self, crypto_app, server_type, logger):
        super(CryptoWSGIContext, self).__init__(crypto_app.app)
        self.crypto = crypto_app.crypto
        self.logger = logger
        self.server_type = server_type

    def get_keys(self, env, required=None):
        # Get the key(s) from the keymaster
        required = required if required is not None else [self.server_type]
        try:
            fetch_crypto_keys = env[CRYPTO_KEY_CALLBACK]
        except KeyError:
            self.logger.exception(_(
                'ERROR get_keys() %s not in env') % CRYPTO_KEY_CALLBACK)
            raise HTTPInternalServerError(
                "Unable to retrieve encryption keys.")

        try:
            keys = fetch_crypto_keys()
        except Exception as err:  # noqa
            self.logger.exception(_(
                'ERROR get_keys(): from %(callback)s: %(err)s'),
                {'callback': CRYPTO_KEY_CALLBACK, 'err': str(err)})
            raise HTTPInternalServerError(
                "Unable to retrieve encryption keys.")

        for name in required:
            try:
                key = keys[name]
                self.crypto.check_key(key)
                continue
            except KeyError:
                self.logger.exception(_("Missing key for %r") % name)
            except TypeError:
                self.logger.exception(_("Did not get a keys dict"))
            except ValueError as e:
                # don't include the key in any messages!
                self.logger.exception(_("Bad key for %(name)r: %(err)s") %
                                      {'name': name, 'err': e})
            raise HTTPInternalServerError(
                "Unable to retrieve encryption keys.")

        return keys


def dump_crypto_meta(crypto_meta):
    """
    Serialize crypto meta to a form suitable for including in a header value.

    The crypto-meta is serialized as a json object. The iv and key values are
    random bytes and as a result need to be base64 encoded before sending over
    the wire. Base64 encoding returns a bytes object in py3, to future proof
    the code, decode this data to produce a string, which is what the
    json.dumps function expects.

    :param crypto_meta: a dict containing crypto meta items
    :returns: a string serialization of a crypto meta dict
    """
    def b64_encode_meta(crypto_meta):
        return {
            name: (base64.b64encode(value).decode() if name in ('iv', 'key')
                   else b64_encode_meta(value) if isinstance(value, dict)
                   else value)
            for name, value in crypto_meta.items()}

    # use sort_keys=True to make serialized form predictable for testing
    return urllib.quote_plus(
        json.dumps(b64_encode_meta(crypto_meta), sort_keys=True))


def load_crypto_meta(value):
    """
    Build the crypto_meta from the json object.

    Note that json.loads always produces unicode strings, to ensure the
    resultant crypto_meta matches the original object cast all key and value
    data to a str except the key and iv which are base64 decoded. This will
    work in py3 as well where all strings are unicode implying the cast is
    effectively a no-op.

    :param value: a string serialization of a crypto meta dict
    :returns: a dict containing crypto meta items
    :raises EncryptionException: if an error occurs while parsing the
                                 crypto meta
    """
    def b64_decode_meta(crypto_meta):
        return {
            str(name): (base64.b64decode(val) if name in ('iv', 'key')
                        else b64_decode_meta(val) if isinstance(val, dict)
                        else str(val))
            for name, val in crypto_meta.items()}

    try:
        value = urllib.unquote_plus(value)
        return b64_decode_meta(json.loads(value))
    except (KeyError, ValueError, TypeError) as err:
        msg = 'Bad crypto meta %s: %s' % (value, err)
        raise EncryptionException(msg)


def append_crypto_meta(value, crypto_meta):
    """
    Serialize and append crypto metadata to an encrypted value.

    :param value: value to which serialized crypto meta will be appended.
    :param crypto_meta: a dict of crypto meta
    :return: a string of the form <value>; swift_meta=<serialized crypto meta>
    """
    return '%s; swift_meta=%s' % (value, dump_crypto_meta(crypto_meta))


def extract_crypto_meta(value):
    """
    Extract and deserialize any crypto meta from the end of a value.

    :param value: string that may have crypto meta at end
    :return: a tuple of the form:
            (<value without crypto meta>, <deserialized crypto meta> or None)
    """
    crypto_meta = None
    parts = value.rsplit(';', 1)
    if len(parts) == 2:
        value, param = parts
        crypto_meta_tag = 'swift_meta='
        if param.strip().startswith(crypto_meta_tag):
            param = param.strip()[len(crypto_meta_tag):]
            crypto_meta = load_crypto_meta(param)
    return value, crypto_meta


def is_crypto_meta(header, server_type):
    return (strip_sys_meta_prefix(
        server_type, header.lower()).startswith('crypto-meta') or
        strip_object_transient_sysmeta_prefix(
        header.lower()).startswith('crypto-meta'))
