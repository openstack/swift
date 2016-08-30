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
import hashlib

from swift.common.middleware.crypto.crypto_utils import Crypto


def fetch_crypto_keys():
    return {'account': 'This is an account key 012345678',
            'container': 'This is a container key 01234567',
            'object': 'This is an object key 0123456789',
            'id': {'v': 'fake', 'path': '/a/c/fake'}}


def md5hex(s):
    return hashlib.md5(s).hexdigest()


def encrypt(val, key=None, iv=None, ctxt=None):
    if ctxt is None:
        ctxt = Crypto({}).create_encryption_ctxt(key, iv)
    enc_val = ctxt.update(val)
    return enc_val


def decrypt(key, iv, enc_val):
    dec_ctxt = Crypto({}).create_decryption_ctxt(key, iv, 0)
    dec_val = dec_ctxt.update(enc_val)
    return dec_val


FAKE_IV = "This is an IV123"
# do not use this example encryption_root_secret in production, use a randomly
# generated value with high entropy
TEST_KEYMASTER_CONF = {'encryption_root_secret': base64.b64encode(b'x' * 32)}


def fake_get_crypto_meta(**kwargs):
    meta = {'iv': FAKE_IV, 'cipher': Crypto.cipher}
    meta.update(kwargs)
    return meta
