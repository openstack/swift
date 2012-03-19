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
from swift.container import replicator


class TestReplicator(unittest.TestCase):
    """
    swift.container.replicator is currently just a subclass with some class
    variables overridden, but at least this test stub will ensure proper Python
    syntax.
    """

    def test_placeholder(self):
        pass


if __name__ == '__main__':
    unittest.main()
