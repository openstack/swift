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

"""Tests for swift.obj.rpc_grpc"""

import unittest
from swift.obj import rpc_grpc, fmgr_pb2
import mock


class TestRpcGrpc(unittest.TestCase):

    def setUp(self):
        self.socket_path = "/path/to/rpc.socket"
        self.part_power = 18

    def test_vfile_list_partitions(self):
        with mock.patch("swift.obj.rpc_grpc.connections.connections") as m_conn:
            ret = rpc_grpc.list_partitions(self.socket_path, self.part_power)
            arg = fmgr_pb2.ListPartitionsInfo(partition_bits=self.part_power)
            m_conn[self.socket_path].stub.ListPartitions.assert_called_once_with(arg)


if __name__ == '__main__':
    unittest.main()
