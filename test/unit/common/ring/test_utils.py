# Copyright (c) 2010-2012 OpenStack Foundation
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

from swift.common.ring.utils import (build_tier_tree, tiers_for_dev,
                                     parse_search_value, parse_args,
                                     build_dev_from_opts)


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.test_dev = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                         'port': '6000', 'id': 0}

        def get_test_devs():
            dev0 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6000', 'id': 0}
            dev1 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6000', 'id': 1}
            dev2 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6000', 'id': 2}
            dev3 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6000', 'id': 3}
            dev4 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6000', 'id': 4}
            dev5 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6000', 'id': 5}
            dev6 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6000', 'id': 6}
            dev7 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6000', 'id': 7}
            dev8 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6000', 'id': 8}
            dev9 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                    'port': '6000', 'id': 9}
            dev10 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                     'port': '6000', 'id': 10}
            dev11 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                     'port': '6000', 'id': 11}
            return [dev0, dev1, dev2, dev3, dev4, dev5,
                    dev6, dev7, dev8, dev9, dev10, dev11]

        self.test_devs = get_test_devs()

    def test_tiers_for_dev(self):
        self.assertEqual(
            tiers_for_dev(self.test_dev),
            ((1,),
             (1, 1),
             (1, 1, '192.168.1.1:6000'),
             (1, 1, '192.168.1.1:6000', 0)))

    def test_build_tier_tree(self):
        ret = build_tier_tree(self.test_devs)
        self.assertEqual(len(ret), 8)
        self.assertEqual(ret[()], set([(1,)]))
        self.assertEqual(ret[(1,)], set([(1, 1), (1, 2)]))
        self.assertEqual(ret[(1, 1)],
                         set([(1, 1, '192.168.1.2:6000'),
                              (1, 1, '192.168.1.1:6000')]))
        self.assertEqual(ret[(1, 2)],
                         set([(1, 2, '192.168.2.2:6000'),
                              (1, 2, '192.168.2.1:6000')]))
        self.assertEqual(ret[(1, 1, '192.168.1.1:6000')],
                         set([(1, 1, '192.168.1.1:6000', 0),
                              (1, 1, '192.168.1.1:6000', 1),
                              (1, 1, '192.168.1.1:6000', 2)]))
        self.assertEqual(ret[(1, 1, '192.168.1.2:6000')],
                         set([(1, 1, '192.168.1.2:6000', 3),
                              (1, 1, '192.168.1.2:6000', 4),
                              (1, 1, '192.168.1.2:6000', 5)]))
        self.assertEqual(ret[(1, 2, '192.168.2.1:6000')],
                         set([(1, 2, '192.168.2.1:6000', 6),
                              (1, 2, '192.168.2.1:6000', 7),
                              (1, 2, '192.168.2.1:6000', 8)]))
        self.assertEqual(ret[(1, 2, '192.168.2.2:6000')],
                         set([(1, 2, '192.168.2.2:6000', 9),
                              (1, 2, '192.168.2.2:6000', 10),
                              (1, 2, '192.168.2.2:6000', 11)]))

    def test_parse_search_value(self):
        res = parse_search_value('r0')
        self.assertEqual(res, {'region': 0})
        res = parse_search_value('r1')
        self.assertEqual(res, {'region': 1})
        res = parse_search_value('r1z2')
        self.assertEqual(res, {'region': 1, 'zone': 2})
        res = parse_search_value('d1')
        self.assertEqual(res, {'id': 1})
        res = parse_search_value('z1')
        self.assertEqual(res, {'zone': 1})
        res = parse_search_value('-127.0.0.1')
        self.assertEqual(res, {'ip': '127.0.0.1'})
        res = parse_search_value('-[127.0.0.1]:10001')
        self.assertEqual(res, {'ip': '127.0.0.1', 'port': 10001})
        res = parse_search_value(':10001')
        self.assertEqual(res, {'port': 10001})
        res = parse_search_value('R127.0.0.10')
        self.assertEqual(res, {'replication_ip': '127.0.0.10'})
        res = parse_search_value('R[127.0.0.10]:20000')
        self.assertEqual(res, {'replication_ip': '127.0.0.10',
                               'replication_port': 20000})
        res = parse_search_value('R:20000')
        self.assertEqual(res, {'replication_port': 20000})
        res = parse_search_value('/sdb1')
        self.assertEqual(res, {'device': 'sdb1'})
        res = parse_search_value('_meta1')
        self.assertEqual(res, {'meta': 'meta1'})
        self.assertRaises(ValueError, parse_search_value, 'OMGPONIES')

    def test_replication_defaults(self):
        args = '-r 1 -z 1 -i 127.0.0.1 -p 6010 -d d1 -w 100'.split()
        opts, _ = parse_args(args)
        device = build_dev_from_opts(opts)
        expected = {
            'device': 'd1',
            'ip': '127.0.0.1',
            'meta': '',
            'port': 6010,
            'region': 1,
            'replication_ip': '127.0.0.1',
            'replication_port': 6010,
            'weight': 100.0,
            'zone': 1,
        }
        self.assertEquals(device, expected)


if __name__ == '__main__':
    unittest.main()
