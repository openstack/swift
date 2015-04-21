# Copyright (c) 2010-2013 OpenStack, LLC.
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

from test.unit.proxy import test_server
from test.unit.proxy.test_server import teardown

from swift.obj import mem_server


def setup():
    test_server.do_setup(mem_server)


class TestController(test_server.TestController):
    pass


class TestProxyServer(test_server.TestProxyServer):
    pass


class TestObjectController(test_server.TestObjectController):
    def test_PUT_no_etag_fallocate(self):
        # mem server doesn't call fallocate(), believe it or not
        pass

    # these tests all go looking in the filesystem
    def test_policy_IO(self):
        pass

    def test_PUT_ec(self):
        pass

    def test_PUT_ec_multiple_segments(self):
        pass

    def test_PUT_ec_fragment_archive_etag_mismatch(self):
        pass


class TestContainerController(test_server.TestContainerController):
    pass


class TestAccountController(test_server.TestAccountController):
    pass


class TestAccountControllerFakeGetResponse(
        test_server.TestAccountControllerFakeGetResponse):
    pass


if __name__ == '__main__':
    setup()
    try:
        unittest.main()
    finally:
        teardown()
