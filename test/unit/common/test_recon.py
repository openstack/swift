# -*- coding:utf-8 -*-
# Copyright (c) 2010-2021 OpenStack Foundation
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

from unittest import TestCase
from swift.common import recon


class TestCommonRecon(TestCase):
    def test_server_type_to_recon_file(self):
        # valid server types will come out as <server_type>.recon
        for server_type in ('object', 'container', 'account', 'ACCount'):
            self.assertEqual(recon.server_type_to_recon_file(server_type),
                             "%s.recon" % server_type.lower())

        # other values will return a ValueError
        for bad_server_type in ('obj', '', None, 'other', 'Account '):
            self.assertRaises(ValueError,
                              recon.server_type_to_recon_file, bad_server_type)
