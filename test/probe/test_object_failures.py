#!/usr/bin/python -u
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

import time
from os import listdir, unlink
from os.path import join as path_join
from unittest import main
from uuid import uuid4

from swiftclient import client

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.utils import hash_path, readconf
from swift.obj.diskfile import write_metadata, read_metadata, get_data_dir
from test.probe.common import ReplProbeTest


RETRIES = 5


def get_data_file_path(obj_dir):
    files = []
    # We might need to try a few times if a request hasn't yet settled. For
    # instance, a PUT can return success when just 2 of 3 nodes has completed.
    for attempt in range(RETRIES + 1):
        try:
            files = sorted(listdir(obj_dir), reverse=True)
            break
        except Exception:
            if attempt < RETRIES:
                time.sleep(1)
            else:
                raise
    for filename in files:
        return path_join(obj_dir, filename)


class TestObjectFailures(ReplProbeTest):

    def _setup_data_file(self, container, obj, data):
        client.put_container(self.url, self.token, container,
                             headers={'X-Storage-Policy':
                                      self.policy.name})
        client.put_object(self.url, self.token, container, obj, data)
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        self.assertEqual(odata, data)
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        onode = onodes[0]
        node_id = (onode['port'] - 6000) / 10
        device = onode['device']
        hash_str = hash_path(self.account, container, obj)
        obj_server_conf = readconf(self.configs['object-server'][node_id])
        devices = obj_server_conf['app:object-server']['devices']
        obj_dir = '%s/%s/%s/%s/%s/%s/' % (devices, device,
                                          get_data_dir(self.policy),
                                          opart, hash_str[-3:], hash_str)
        data_file = get_data_file_path(obj_dir)
        return onode, opart, data_file

    def run_quarantine(self):
        container = 'container-%s' % uuid4()
        obj = 'object-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj,
                                                        'VERIFY')
        # Stash the on disk data for future comparison - this may not equal
        # 'VERIFY' if for example the proxy has crypto enabled
        backend_data = direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]

        metadata = read_metadata(data_file)
        metadata['ETag'] = 'badetag'
        write_metadata(data_file, metadata)

        odata = direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]
        self.assertEqual(odata, backend_data)
        try:
            direct_client.direct_get_object(
                onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
            raise Exception("Did not quarantine object")
        except ClientException as err:
            self.assertEqual(err.http_status, 404)

    def run_quarantine_range_etag(self):
        container = 'container-range-%s' % uuid4()
        obj = 'object-range-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj,
                                                        'RANGE')
        # Stash the on disk data for future comparison - this may not equal
        # 'VERIFY' if for example the proxy has crypto enabled
        backend_data = direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]

        metadata = read_metadata(data_file)
        metadata['ETag'] = 'badetag'
        write_metadata(data_file, metadata)
        base_headers = {'X-Backend-Storage-Policy-Index': self.policy.idx}
        for header, result in [({'Range': 'bytes=0-2'}, backend_data[0:3]),
                               ({'Range': 'bytes=1-11'}, backend_data[1:]),
                               ({'Range': 'bytes=0-11'}, backend_data)]:
            req_headers = base_headers.copy()
            req_headers.update(header)
            odata = direct_client.direct_get_object(
                onode, opart, self.account, container, obj,
                headers=req_headers)[-1]
            self.assertEqual(odata, result)

        try:
            direct_client.direct_get_object(
                onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
            raise Exception("Did not quarantine object")
        except ClientException as err:
            self.assertEqual(err.http_status, 404)

    def run_quarantine_zero_byte_get(self):
        container = 'container-zbyte-%s' % uuid4()
        obj = 'object-zbyte-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj, 'DATA')
        metadata = read_metadata(data_file)
        unlink(data_file)

        with open(data_file, 'w') as fpointer:
            write_metadata(fpointer, metadata)
        try:
            direct_client.direct_get_object(
                onode, opart, self.account, container, obj, conn_timeout=1,
                response_timeout=1, headers={'X-Backend-Storage-Policy-Index':
                                             self.policy.idx})
            raise Exception("Did not quarantine object")
        except ClientException as err:
            self.assertEqual(err.http_status, 404)

    def run_quarantine_zero_byte_head(self):
        container = 'container-zbyte-%s' % uuid4()
        obj = 'object-zbyte-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj, 'DATA')
        metadata = read_metadata(data_file)
        unlink(data_file)

        with open(data_file, 'w') as fpointer:
            write_metadata(fpointer, metadata)
        try:
            direct_client.direct_head_object(
                onode, opart, self.account, container, obj, conn_timeout=1,
                response_timeout=1, headers={'X-Backend-Storage-Policy-Index':
                                             self.policy.idx})
            raise Exception("Did not quarantine object")
        except ClientException as err:
            self.assertEqual(err.http_status, 404)

    def run_quarantine_zero_byte_post(self):
        container = 'container-zbyte-%s' % uuid4()
        obj = 'object-zbyte-%s' % uuid4()
        onode, opart, data_file = self._setup_data_file(container, obj, 'DATA')
        metadata = read_metadata(data_file)
        unlink(data_file)

        with open(data_file, 'w') as fpointer:
            write_metadata(fpointer, metadata)
        try:
            headers = {'X-Object-Meta-1': 'One', 'X-Object-Meta-Two': 'Two',
                       'X-Backend-Storage-Policy-Index': self.policy.idx}
            direct_client.direct_post_object(
                onode, opart, self.account,
                container, obj,
                headers=headers,
                conn_timeout=1,
                response_timeout=1)
            raise Exception("Did not quarantine object")
        except ClientException as err:
            self.assertEqual(err.http_status, 404)

    def test_runner(self):
        self.run_quarantine()
        self.run_quarantine_range_etag()
        self.run_quarantine_zero_byte_get()
        self.run_quarantine_zero_byte_head()
        self.run_quarantine_zero_byte_post()


if __name__ == '__main__':
    main()
