# Copyright (c) 2016 OpenStack Foundation
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
import unittest

from swift.common import utils
from swift.common.middleware import crypto


class TestCrypto(unittest.TestCase):
    def test_filter_factory(self):
        factory = crypto.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory({}), crypto.decrypter.Decrypter)
        self.assertIsInstance(factory({}).app, crypto.encrypter.Encrypter)
        self.assertIn('encryption', utils._swift_admin_info)
        self.assertDictEqual(
            {'enabled': True}, utils._swift_admin_info['encryption'])
        self.assertNotIn('encryption', utils._swift_info)

        factory = crypto.filter_factory({'disable_encryption': True})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory({}), crypto.decrypter.Decrypter)
        self.assertIsInstance(factory({}).app, crypto.encrypter.Encrypter)
        self.assertIn('encryption', utils._swift_admin_info)
        self.assertDictEqual(
            {'enabled': False}, utils._swift_admin_info['encryption'])
        self.assertNotIn('encryption', utils._swift_info)
