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

import re
import unittest
import swift


class TestVersioning(unittest.TestCase):
    def test_canonical_version_is_clean(self):
        """Ensure that a non-clean canonical_version never happens"""
        pattern = re.compile('^\d+(\.\d+)*$')
        self.assertTrue(pattern.match(swift.__canonical_version__) is not None)

    def test_canonical_version_equals_version_for_final(self):
        version = swift.Version('7.8.9', True)
        self.assertEquals(version.pretty_version, '7.8.9')
        self.assertEquals(version.canonical_version, '7.8.9')

    def test_version_has_dev_suffix_for_non_final(self):
        version = swift.Version('7.8.9', False)
        self.assertEquals(version.pretty_version, '7.8.9-dev')
        self.assertEquals(version.canonical_version, '7.8.9')

if __name__ == '__main__':
    unittest.main()
