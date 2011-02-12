# Copyright (c) 2010-2011 OpenStack, LLC.
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

""" Tests for swift.common.compressing_file_reader """

import unittest
import cStringIO

from swift.common.compressing_file_reader import CompressingFileReader

class TestCompressingFileReader(unittest.TestCase):

    def test_read(self):
        plain = 'obj\ndata'
        s = cStringIO.StringIO(plain)
        expected = '\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\xcaO\xca\xe2JI,'\
                   'I\x04\x00\x00\x00\xff\xff\x03\x00P(\xa8\x1f\x08\x00\x00'\
                   '\x00'
        x = CompressingFileReader(s)
        compressed = ''.join(iter(lambda: x.read(), ''))
        self.assertEquals(compressed, expected)
        self.assertEquals(x.read(), '')
