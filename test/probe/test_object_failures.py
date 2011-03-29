#!/usr/bin/python -u
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

import unittest
import os
from uuid import uuid4

from swift.common import client, direct_client
from swift.common.utils import hash_path, readconf
from swift.obj.server import write_metadata, read_metadata
from test.probe.common import kill_pids, reset_environment


class TestObjectFailures(unittest.TestCase):

    def setUp(self):
        self.pids, self.port2server, self.account_ring, self.container_ring, \
            self.object_ring, self.url, self.token, self.account = \
                reset_environment()

    def tearDown(self):
        kill_pids(self.pids)

    def _get_data_file_path(self, obj_dir):
        files = sorted(os.listdir(obj_dir), reverse=True)
        for file in files:
            return os.path.join(obj_dir, file)

    def _setup_data_file(self, container, obj, data):
        client.put_container(self.url, self.token, container)
        client.put_object(self.url, self.token, container, obj, data)
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        self.assertEquals(odata, data)
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        onode = onodes[0]
        node_id = (onode['port'] - 6000) / 10
        device = onode['device']
        hash_str = hash_path(self.account, container, obj)
        obj_server_conf = readconf('/etc/swift/object-server/%s.conf' %
                                   node_id)
        devices = obj_server_conf['app:object-server']['devices']
        obj_dir = '%s/%s/objects/%s/%s/%s/' % (devices,
                                               device, opart,
                                               hash_str[-3:], hash_str)
        data_file = self._get_data_file_path(obj_dir)
        return onode, opart, data_file

    def run_quarantine(self):
        container = 'container-%s' % uuid4()
        obj = 'object-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj,
                                                        'VERIFY')
        with open(data_file) as fp:
            metadata = read_metadata(fp)
        metadata['ETag'] = 'badetag'
        with open(data_file) as fp:
            write_metadata(fp, metadata)

        odata = direct_client.direct_get_object(onode, opart,
                                          self.account, container, obj)[-1]
        self.assertEquals(odata, 'VERIFY')
        try:
            resp = direct_client.direct_get_object(onode, opart, self.account,
                                                   container, obj)
            raise "Did not quarantine object"
        except client.ClientException, e:
            self.assertEquals(e.http_status, 404)

    def run_quarantine_range_etag(self):
        container = 'container-range-%s' % uuid4()
        obj = 'object-range-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj,
                                                        'RANGE')
        with open(data_file) as fp:
            metadata = read_metadata(fp)
        metadata['ETag'] = 'badetag'
        with open(data_file) as fp:
            write_metadata(fp, metadata)
        for header, result in [({'Range': 'bytes=0-2'}, 'RAN'),
                               ({'Range': 'bytes=1-11'}, 'ANGE'),
                               ({'Range': 'bytes=0-11'}, 'RANGE')]:
            odata = direct_client.direct_get_object(onode, opart,
                                              self.account, container, obj,
                                              headers=header)[-1]

            self.assertEquals(odata, result)

        try:
            resp = direct_client.direct_get_object(onode, opart, self.account,
                                                   container, obj)
            raise "Did not quarantine object"
        except client.ClientException, e:
            self.assertEquals(e.http_status, 404)

    def run_quarantine_zero_byte_get(self):
        container = 'container-zbyte-%s' % uuid4()
        obj = 'object-zbyte-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj, 'DATA')
        with open(data_file) as fp:
            metadata = read_metadata(fp)
        os.unlink(data_file)

        with open(data_file, 'w') as fp:
            write_metadata(fp, metadata)
        try:
            resp = direct_client.direct_get_object(onode, opart, self.account,
                                                   container, obj,
                                                   conn_timeout=1,
                                                   response_timeout=1)
            raise "Did not quarantine object"
        except client.ClientException, e:
            self.assertEquals(e.http_status, 404)

    def run_quarantine_zero_byte_head(self):
        container = 'container-zbyte-%s' % uuid4()
        obj = 'object-zbyte-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj, 'DATA')
        with open(data_file) as fp:
            metadata = read_metadata(fp)
        os.unlink(data_file)

        with open(data_file, 'w') as fp:
            write_metadata(fp, metadata)
        try:
            resp = direct_client.direct_head_object(onode, opart, self.account,
                                                   container, obj,
                                                   conn_timeout=1,
                                                   response_timeout=1)
            raise "Did not quarantine object"
        except client.ClientException, e:
            self.assertEquals(e.http_status, 404)

    def run_quarantine_zero_byte_post(self):
        container = 'container-zbyte-%s' % uuid4()
        obj = 'object-zbyte-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj, 'DATA')
        with open(data_file) as fp:
            metadata = read_metadata(fp)
        os.unlink(data_file)

        with open(data_file, 'w') as fp:
            write_metadata(fp, metadata)
        try:
            resp = direct_client.direct_post_object(
                onode, opart, self.account,
                container, obj,
                {'X-Object-Meta-1': 'One', 'X-Object-Meta-Two': 'Two'},
                conn_timeout=1,
                response_timeout=1)
            raise "Did not quarantine object"
        except client.ClientException, e:
            self.assertEquals(e.http_status, 404)

    def test_runner(self):
        self.run_quarantine()
        self.run_quarantine_range_etag()
        self.run_quarantine_zero_byte_get()
        self.run_quarantine_zero_byte_head()
        self.run_quarantine_zero_byte_post()

if __name__ == '__main__':
    unittest.main()
