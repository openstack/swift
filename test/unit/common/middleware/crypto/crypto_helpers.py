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

from swift.common.exceptions import UnknownSecretIdError
from swift.common.middleware.crypto.crypto_utils import Crypto
from swift.common.utils import md5


def fetch_crypto_keys(key_id=None):
    id_to_keys = {None: {'account': b'This is an account key 012345678',
                         'container': b'This is a container key 01234567',
                         'object': b'This is an object key 0123456789'},
                  'myid': {'account': b'This is an account key 123456789',
                           'container': b'This is a container key 12345678',
                           'object': b'This is an object key 1234567890'}}
    key_id = key_id or {}
    secret_id = key_id.get('secret_id') or None
    try:
        keys = dict(id_to_keys[secret_id])
    except KeyError:
        raise UnknownSecretIdError(secret_id)
    keys['id'] = {'v': 'fake', 'path': '/a/c/fake'}
    if secret_id:
        keys['id']['secret_id'] = secret_id
    keys['all_ids'] = [{'v': 'fake', 'path': '/a/c/fake'},
                       {'v': 'fake', 'path': '/a/c/fake', 'secret_id': 'myid'}]
    return keys


def md5hex(s):
    return md5(s, usedforsecurity=False).hexdigest()


def encrypt(val, key=None, iv=None, ctxt=None):
    if ctxt is None:
        ctxt = Crypto({}).create_encryption_ctxt(key, iv)
    enc_val = ctxt.update(val)
    return enc_val


def decrypt(key, iv, enc_val):
    dec_ctxt = Crypto({}).create_decryption_ctxt(key, iv, 0)
    dec_val = dec_ctxt.update(enc_val)
    return dec_val


FAKE_IV = b"This is an IV123"
# do not use this example encryption_root_secret in production, use a randomly
# generated value with high entropy
TEST_KEYMASTER_CONF = {
    'encryption_root_secret': base64.b64encode(b'x' * 32),
    'encryption_root_secret_1': base64.b64encode(b'y' * 32),
    'encryption_root_secret_2': base64.b64encode(b'z' * 32)
}


def fake_get_crypto_meta(**kwargs):
    meta = {'iv': FAKE_IV, 'cipher': Crypto.cipher}
    meta.update(kwargs)
    return meta
