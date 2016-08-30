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
import mock

from swift.common import utils
from swift.common.middleware import crypto


class TestCrypto(unittest.TestCase):
    def test_filter_factory(self):
        def do_test(conf, expect_enabled):
            fake_app = object()

            with mock.patch.dict('swift.common.utils._swift_admin_info',
                                 clear=True):
                # we're not expecting utils._swift_info to be modified but mock
                # it anyway just in case it is
                with mock.patch.dict('swift.common.utils._swift_info',
                                     clear=True):
                    # Sanity checks...
                    self.assertNotIn('encryption', utils._swift_admin_info)
                    self.assertNotIn('encryption',
                                     utils.get_swift_info(admin=True))
                    self.assertNotIn('encryption',
                                     utils.get_swift_info(admin=True)['admin'])

                    factory = crypto.filter_factory(conf)
                    self.assertTrue(callable(factory))
                    filtered_app = factory(fake_app)

                    self.assertNotIn('encryption', utils._swift_info)
                    self.assertNotIn('encryption', utils.get_swift_info())
                    self.assertNotIn('encryption',
                                     utils.get_swift_info(admin=True))

                    self.assertIn('encryption', utils._swift_admin_info)
                    self.assertDictEqual({'enabled': expect_enabled},
                                         utils._swift_admin_info['encryption'])
                    self.assertIn('encryption',
                                  utils.get_swift_info(admin=True)['admin'])
                    self.assertDictEqual(
                        {'enabled': expect_enabled},
                        utils.get_swift_info(
                            admin=True)['admin']['encryption'])

            self.assertIsInstance(filtered_app, crypto.decrypter.Decrypter)
            self.assertIsInstance(filtered_app.app, crypto.encrypter.Encrypter)
            self.assertIs(filtered_app.app.app, fake_app)

        # default enabled
        do_test({}, True)

        # explicitly enabled
        do_test({'disable_encryption': False}, True)

        # explicitly disabled
        do_test({'disable_encryption': True}, False)
