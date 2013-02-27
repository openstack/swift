# Copyright (c) 2010-2012 OpenStack, LLC.
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

from swift.common.ring.utils import build_tier_tree, tiers_for_dev


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.test_dev = {'zone': 1, 'ip': '192.168.1.1',
                         'port': '6000', 'id': 0}

        def get_test_devs():
            dev0 = {'zone': 1, 'ip': '192.168.1.1', 'port': '6000', 'id': 0}
            dev1 = {'zone': 1, 'ip': '192.168.1.1', 'port': '6000', 'id': 1}
            dev2 = {'zone': 1, 'ip': '192.168.1.1', 'port': '6000', 'id': 2}
            dev3 = {'zone': 1, 'ip': '192.168.1.2', 'port': '6000', 'id': 3}
            dev4 = {'zone': 1, 'ip': '192.168.1.2', 'port': '6000', 'id': 4}
            dev5 = {'zone': 1, 'ip': '192.168.1.2', 'port': '6000', 'id': 5}
            dev6 = {'zone': 2, 'ip': '192.168.2.1', 'port': '6000', 'id': 6}
            dev7 = {'zone': 2, 'ip': '192.168.2.1', 'port': '6000', 'id': 7}
            dev8 = {'zone': 2, 'ip': '192.168.2.1', 'port': '6000', 'id': 8}
            dev9 = {'zone': 2, 'ip': '192.168.2.2', 'port': '6000', 'id': 9}
            dev10 = {'zone': 2, 'ip': '192.168.2.2', 'port': '6000', 'id': 10}
            dev11 = {'zone': 2, 'ip': '192.168.2.2', 'port': '6000', 'id': 11}
            return [dev0, dev1, dev2, dev3, dev4, dev5,
                    dev6, dev7, dev8, dev9, dev10, dev11]

        self.test_devs = get_test_devs()

    def test_tiers_for_dev(self):
        self.assertEqual(tiers_for_dev(self.test_dev),
                ((1,), (1, '192.168.1.1:6000'), (1, '192.168.1.1:6000', 0)))

    def test_build_tier_tree(self):
        ret = build_tier_tree(self.test_devs)
        self.assertEqual(len(ret), 7)
        self.assertEqual(ret[()], set([(2,), (1,)]))
        self.assertEqual(ret[(1,)],
                         set([(1, '192.168.1.2:6000'),
                              (1, '192.168.1.1:6000')]))
        self.assertEqual(ret[(2,)],
                         set([(2, '192.168.2.2:6000'),
                              (2, '192.168.2.1:6000')]))
        self.assertEqual(ret[(1, '192.168.1.1:6000')],
                         set([(1, '192.168.1.1:6000', 0),
                              (1, '192.168.1.1:6000', 1),
                              (1, '192.168.1.1:6000', 2)]))
        self.assertEqual(ret[(1, '192.168.1.2:6000')],
                         set([(1, '192.168.1.2:6000', 3),
                              (1, '192.168.1.2:6000', 4),
                              (1, '192.168.1.2:6000', 5)]))
        self.assertEqual(ret[(2, '192.168.2.1:6000')],
                         set([(2, '192.168.2.1:6000', 6),
                              (2, '192.168.2.1:6000', 7),
                              (2, '192.168.2.1:6000', 8)]))
        self.assertEqual(ret[(2, '192.168.2.2:6000')],
                         set([(2, '192.168.2.2:6000', 9),
                              (2, '192.168.2.2:6000', 10),
                              (2, '192.168.2.2:6000', 11)]))


if __name__ == '__main__':
    unittest.main()
