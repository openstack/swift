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
import base64
import binascii
import collections
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import six
from six.moves.urllib import parse as urlparse

from swift import gettext_ as _
from swift.common.exceptions import EncryptionException, UnknownSecretIdError
from swift.common.swob import HTTPInternalServerError
from swift.common.utils import get_logger
from swift.common.wsgi import WSGIContext
from cgi import parse_header

CRYPTO_KEY_CALLBACK = 'swift.callback.fetch_crypto_keys'


class Crypto(object):
    """
    Used by middleware: Calls cryptography library
    """
    cipher = 'AES_CTR_256'
    # AES will accept several key sizes - we are using 256 bits i.e. 32 bytes
    key_length = 32
    iv_length = algorithms.AES.block_size // 8

    def __init__(self, conf=None):
        self.logger = get_logger(conf, log_route="crypto")
        # memoize backend to avoid repeated iteration over entry points
        self.backend = default_backend()

    def create_encryption_ctxt(self, key, iv):
        """
        Creates a crypto context for encrypting

        :param key: 256-bit key
        :param iv: 128-bit iv or nonce used for encryption
        :raises ValueError: on invalid key or iv
        :returns: an instance of an encryptor
        """
        self.check_key(key)
        engine = Cipher(algorithms.AES(key), modes.CTR(iv),
                        backend=self.backend)
        return engine.encryptor()

    def create_decryption_ctxt(self, key, iv, offset):
        """
        Creates a crypto context for decrypting

        :param key: 256-bit key
        :param iv: 128-bit iv or nonce used for decryption
        :param offset: offset into the message; used for range reads
        :returns: an instance of a decryptor
        """
        self.check_key(key)
        if offset < 0:
            raise ValueError('Offset must not be negative')
        if offset:
            # Adjust IV so that it is correct for decryption at offset.
            # The CTR mode offset is incremented for every AES block and taken
            # modulo 2^128.
            offset_blocks, offset_in_block = divmod(offset, self.iv_length)
            ivl = int(binascii.hexlify(iv), 16) + offset_blocks
            ivl %= 1 << algorithms.AES.block_size
            iv = bytes(bytearray.fromhex(format(
                ivl, '0%dx' % (2 * self.iv_length))))
        else:
            offset_in_block = 0

        engine = Cipher(algorithms.AES(key), modes.CTR(iv),
                        backend=self.backend)
        dec = engine.decryptor()
        # Adjust decryption boundary within current AES block
        dec.update(b'*' * offset_in_block)
        return dec

    def create_iv(self):
        return os.urandom(self.iv_length)

    def create_crypto_meta(self):
        # create a set of parameters
        return {'iv': self.create_iv(), 'cipher': self.cipher}

    def check_crypto_meta(self, meta):
        """
        Check that crypto meta dict has valid items.

        :param meta: a dict
        :raises EncryptionException: if an error is found in the crypto meta
        """
        try:
            if meta['cipher'] != self.cipher:
                raise EncryptionException('Bad crypto meta: Cipher must be %s'
                                          % self.cipher)
            if len(meta['iv']) != self.iv_length:
                raise EncryptionException(
                    'Bad crypto meta: IV must be length %s bytes'
                    % self.iv_length)
        except KeyError as err:
            raise EncryptionException(
                'Bad crypto meta: Missing %s' % err)

    def create_random_key(self):
        # helper method to create random key of correct length
        return os.urandom(self.key_length)

    def wrap_key(self, wrapping_key, key_to_wrap):
        # we don't use an RFC 3394 key wrap algorithm such as cryptography's
        # aes_wrap_key because it's slower and we have iv material readily
        # available so don't need a deterministic algorithm
        iv = self.create_iv()
        encryptor = Cipher(algorithms.AES(wrapping_key), modes.CTR(iv),
                           backend=self.backend).encryptor()
        return {'key': encryptor.update(key_to_wrap), 'iv': iv}

    def unwrap_key(self, wrapping_key, context):
        # unwrap a key from dict of form returned by wrap_key
        # check the key length early - unwrapping won't change the length
        self.check_key(context['key'])
        decryptor = Cipher(algorithms.AES(wrapping_key),
                           modes.CTR(context['iv']),
                           backend=self.backend).decryptor()
        return decryptor.update(context['key'])

    def check_key(self, key):
        if len(key) != self.key_length:
            raise ValueError("Key must be length %s bytes" % self.key_length)


class CryptoWSGIContext(WSGIContext):
    """
    Base class for contexts used by crypto middlewares.
    """
    def __init__(self, crypto_app, server_type, logger):
        super(CryptoWSGIContext, self).__init__(crypto_app.app)
        self.crypto = crypto_app.crypto
        self.logger = logger
        self.server_type = server_type

    def get_keys(self, env, required=None, key_id=None):
        # Get the key(s) from the keymaster
        required = required if required is not None else [self.server_type]
        try:
            fetch_crypto_keys = env[CRYPTO_KEY_CALLBACK]
        except KeyError:
            self.logger.exception(_('ERROR get_keys() missing callback'))
            raise HTTPInternalServerError(
                "Unable to retrieve encryption keys.")

        err = None
        try:
            keys = fetch_crypto_keys(key_id=key_id)
        except UnknownSecretIdError as err:
            self.logger.error('get_keys(): unknown key id: %s', err)
            raise
        except Exception as err:  # noqa
            self.logger.exception('get_keys(): from callback: %s', err)
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

    def get_multiple_keys(self, env):
        # get a list of keys from the keymaster containing one dict of keys for
        # each of the keymaster root secret ids
        keys = [self.get_keys(env)]
        active_key_id = keys[0]['id']
        for other_key_id in keys[0].get('all_ids', []):
            if other_key_id == active_key_id:
                continue
            keys.append(self.get_keys(env, key_id=other_key_id))
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
    return urlparse.quote_plus(
        json.dumps(b64_encode_meta(crypto_meta), sort_keys=True))


def load_crypto_meta(value, b64decode=True):
    """
    Build the crypto_meta from the json object.

    Note that json.loads always produces unicode strings; to ensure the
    resultant crypto_meta matches the original object:
        * cast all keys to str (effectively a no-op on py3),
        * base64 decode 'key' and 'iv' values to bytes, and
        * encode remaining string values as UTF-8 on py2 (while leaving them
          as native unicode strings on py3).

    :param value: a string serialization of a crypto meta dict
    :param b64decode: decode the 'key' and 'iv' values to bytes, default True
    :returns: a dict containing crypto meta items
    :raises EncryptionException: if an error occurs while parsing the
                                 crypto meta
    """
    def b64_decode_meta(crypto_meta):
        return {
            str(name): (
                base64.b64decode(val) if name in ('iv', 'key') and b64decode
                else b64_decode_meta(val) if isinstance(val, dict)
                else val.encode('utf8') if six.PY2 else val)
            for name, val in crypto_meta.items()}

    try:
        if not isinstance(value, six.string_types):
            raise ValueError('crypto meta not a string')
        val = json.loads(urlparse.unquote_plus(value))
        if not isinstance(val, collections.Mapping):
            raise ValueError('crypto meta not a Mapping')
        return b64_decode_meta(val)
    except (KeyError, ValueError, TypeError) as err:
        msg = 'Bad crypto meta %r: %s' % (value, err)
        raise EncryptionException(msg)


def append_crypto_meta(value, crypto_meta):
    """
    Serialize and append crypto metadata to an encrypted value.

    :param value: value to which serialized crypto meta will be appended.
    :param crypto_meta: a dict of crypto meta
    :return: a string of the form <value>; swift_meta=<serialized crypto meta>
    """
    if not isinstance(value, str):
        raise ValueError
    return '%s; swift_meta=%s' % (value, dump_crypto_meta(crypto_meta))


def extract_crypto_meta(value):
    """
    Extract and deserialize any crypto meta from the end of a value.

    :param value: string that may have crypto meta at end
    :return: a tuple of the form:
            (<value without crypto meta>, <deserialized crypto meta> or None)
    """
    swift_meta = None
    value, meta = parse_header(value)
    if 'swift_meta' in meta:
        swift_meta = load_crypto_meta(meta['swift_meta'])
    return value, swift_meta
