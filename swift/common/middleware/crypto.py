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
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from hashlib import md5

from swift.common.utils import get_logger


class Crypto(object):
    """
    Used by middleware: Calls crypto alg
    """
    def __init__(self, conf):
        # TODO - remove conf if there will be no conf items
        self.conf = conf
        self.logger = get_logger(conf, log_route="crypto")

    def _check_key(self, key):
        if len(key) != 32:
            raise ValueError("Invalid key: length should be 32")

    def create_encryption_ctxt(self, key, iv):
        """
        Creates a crypto context for encrypting

        :param key: 256-bit key
        :param iv: 128-bit iv or nonce used for encryption
        :raises: ValueError on invalid key or iv
        :returns: an instance of :class:`CryptoContext`
        """
        self._check_key(key)
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
        :raises: ValueError on invalid key, iv or offset
        :returns: an instance of :class:`CryptoContext`
        """
        # TODO: check keys when we first fetch them from the keymaster
        self._check_key(key)
        if offset < 0:
            raise ValueError('Invalid offset: should be >= 0')
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

    def get_block_size(self):
        return algorithms.AES.block_size

    def get_required_iv_length(self):
        return self.get_block_size() / 8

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
        # here for tests to mock
        return os.urandom(self.get_required_iv_length())

    def create_iv(self, iv_base=None):
        if iv_base:
            return self._get_derived_iv(iv_base)
        return self._get_random_iv()

    def get_cipher(self):
        return 'AES_CTR_256'

    def get_crypto_meta(self, iv_base=None):
        # create a set of parameters
        return {'iv': self.create_iv(iv_base), 'cipher': self.get_cipher()}


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
