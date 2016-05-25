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

import binascii
from hashlib import md5
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from swift.common.exceptions import EncryptionException
from swift.common.utils import get_logger

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

    def get_iv(self):
        return self.iv
