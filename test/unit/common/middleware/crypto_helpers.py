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
import hashlib


def fetch_crypto_keys():
    # Notice that these are not long enough for the real encryption.
    return {'account': 'account_key',
            'container': 'container_key',
            'object': 'object_key'}


def md5hex(s):
    return hashlib.md5(s).hexdigest()


def fake_decrypt(chunk):
    return chunk.swapcase()


def fake_encrypt(chunk):
    return chunk.swapcase()


class FakeEncryptionContext(object):

    def update(self, chunk):
        return fake_encrypt(chunk)


class FakeDecryptionContext(object):

    def update(self, chunk):
        return fake_decrypt(chunk)


class FakeCrypto(object):
    def __init__(self, *args):
        pass

    def create_encryption_ctxt(self, key, iv):
        return FakeEncryptionContext()

    def create_decryption_ctxt(self, key, iv, offset):
        return FakeDecryptionContext()

    def create_iv(self):
        return "test_iv"

    def get_cipher(self):
        return "test_cipher"
