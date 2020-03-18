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

from unittest import main

from swiftclient import client

from swift.common.request_helpers import get_reserved_name

from test.probe.common import ReplProbeTest


class TestObjectVersioning(ReplProbeTest):

    def _assert_account_level(self, container_name, hdr_cont_count,
                              hdr_obj_count, hdr_bytes, cont_count,
                              cont_bytes):

        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(hdr_cont_count, headers['x-account-container-count'])
        self.assertEqual(hdr_obj_count, headers['x-account-object-count'])
        self.assertEqual(hdr_bytes, headers['x-account-bytes-used'])
        self.assertEqual(len(containers), 1)
        container = containers[0]
        self.assertEqual(container_name, container['name'])
        self.assertEqual(cont_count, container['count'])
        self.assertEqual(cont_bytes, container['bytes'])

    def test_account_listing(self):
        versions_header_key = 'X-Versions-Enabled'

        # Create container1
        container_name = 'container1'
        obj_name = 'object1'
        client.put_container(self.url, self.token, container_name)

        # Assert account level sees it
        self._assert_account_level(
            container_name,
            hdr_cont_count='1',
            hdr_obj_count='0',
            hdr_bytes='0',
            cont_count=0,
            cont_bytes=0)

        # Enable versioning
        hdrs = {versions_header_key: 'True'}
        client.post_container(self.url, self.token, container_name, hdrs)

        # write multiple versions of same obj
        client.put_object(self.url, self.token, container_name, obj_name,
                          'version1')
        client.put_object(self.url, self.token, container_name, obj_name,
                          'version2')

        # Assert account level doesn't see object data yet, but it
        # does see the update for the hidden container
        self._assert_account_level(
            container_name,
            hdr_cont_count='2',
            hdr_obj_count='0',
            hdr_bytes='0',
            cont_count=0,
            cont_bytes=0)

        # Get to final state
        self.get_to_final_state()

        # Assert account level now sees updated values
        # N.B: Note difference in values between header and container listing
        # header object count is counting both symlink + object versions
        # listing count is counting only symlink (in primary container)
        self._assert_account_level(
            container_name,
            hdr_cont_count='2',
            hdr_obj_count='3',
            hdr_bytes='16',
            cont_count=1,
            cont_bytes=16)

        client.delete_object(self.url, self.token, container_name, obj_name)
        _headers, current_versions = client.get_container(
            self.url, self.token, container_name)
        self.assertEqual(len(current_versions), 0)
        _headers, all_versions = client.get_container(
            self.url, self.token, container_name, query_string='versions')
        self.assertEqual(len(all_versions), 3)

        # directly delete primary container to leave an orphan hidden
        # container
        self.direct_delete_container(container=container_name)

        # Get to final state
        self.get_to_final_state()

        # The container count decreases, as well as object count. But bytes
        # do not. The discrepancy between header object count, container
        # object count and bytes should indicate orphan hidden container is
        # still around consuming storage
        self._assert_account_level(
            container_name,
            hdr_cont_count='1',
            hdr_obj_count='3',
            hdr_bytes='16',
            cont_count=0,
            cont_bytes=16)

        # Can't HEAD or list anything, though
        with self.assertRaises(client.ClientException) as caught:
            client.head_container(self.url, self.token, container_name)
        self.assertEqual(caught.exception.http_status, 404)
        with self.assertRaises(client.ClientException) as caught:
            client.get_container(self.url, self.token, container_name)
        self.assertEqual(caught.exception.http_status, 404)
        with self.assertRaises(client.ClientException) as caught:
            client.get_container(self.url, self.token, container_name,
                                 query_string='versions')
        self.assertEqual(caught.exception.http_status, 404)
        with self.assertRaises(client.ClientException) as caught:
            client.get_object(
                self.url, self.token, container_name, all_versions[1]['name'],
                query_string='version-id=%s' % all_versions[1]['version_id'])
        # A little funny -- maybe this should 404 instead?
        self.assertEqual(caught.exception.http_status, 400)

        # Fix isn't too bad -- just make the container again!
        client.put_container(self.url, self.token, container_name)
        _headers, current_versions = client.get_container(
            self.url, self.token, container_name)
        self.assertEqual(len(current_versions), 0)
        _headers, all_versions = client.get_container(
            self.url, self.token, container_name, query_string='versions')
        self.assertEqual(len(all_versions), 3)

        # ... but to actually *access* the versions, you have to enable
        # versioning again
        with self.assertRaises(client.ClientException) as caught:
            client.get_object(
                self.url, self.token, container_name, all_versions[1]['name'],
                query_string='version-id=%s' % all_versions[1]['version_id'])
        self.assertEqual(caught.exception.http_status, 400)
        self.assertIn(b'version-aware operations require',
                      caught.exception.http_response_content)
        client.post_container(self.url, self.token, container_name,
                              headers={'X-Versions-Enabled': 'true'})
        client.get_object(
            self.url, self.token, container_name, all_versions[1]['name'],
            query_string='version-id=%s' % all_versions[1]['version_id'])

    def test_missing_versions_container(self):
        versions_header_key = 'X-Versions-Enabled'

        # Create container1
        container_name = 'container1'
        obj_name = 'object1'
        client.put_container(self.url, self.token, container_name)

        # Write some data
        client.put_object(self.url, self.token, container_name, obj_name,
                          b'null version')

        # Enable versioning
        hdrs = {versions_header_key: 'True'}
        client.post_container(self.url, self.token, container_name, hdrs)

        # But directly delete hidden container to leave an orphan primary
        # container
        self.direct_delete_container(container=get_reserved_name(
            'versions', container_name))

        # Could be worse; we can still list versions and GET data
        _headers, all_versions = client.get_container(
            self.url, self.token, container_name, query_string='versions')
        self.assertEqual(len(all_versions), 1)
        self.assertEqual(all_versions[0]['name'], obj_name)
        self.assertEqual(all_versions[0]['version_id'], 'null')

        _headers, data = client.get_object(
            self.url, self.token, container_name, obj_name)
        self.assertEqual(data, b'null version')

        _headers, data = client.get_object(
            self.url, self.token, container_name, obj_name,
            query_string='version-id=null')
        self.assertEqual(data, b'null version')

        # But most any write is going to fail
        with self.assertRaises(client.ClientException) as caught:
            client.put_object(self.url, self.token, container_name, obj_name,
                              b'new version')
        self.assertEqual(caught.exception.http_status, 500)
        with self.assertRaises(client.ClientException) as caught:
            client.delete_object(self.url, self.token, container_name,
                                 obj_name)
        self.assertEqual(caught.exception.http_status, 500)

        # Version-aware delete can work, though!
        client.delete_object(self.url, self.token, container_name, obj_name,
                             query_string='version-id=null')

        # Re-enabling versioning should square us
        hdrs = {versions_header_key: 'True'}
        client.post_container(self.url, self.token, container_name, hdrs)

        client.put_object(self.url, self.token, container_name, obj_name,
                          b'new version')

        _headers, all_versions = client.get_container(
            self.url, self.token, container_name, query_string='versions')
        self.assertEqual(len(all_versions), 1)
        self.assertEqual(all_versions[0]['name'], obj_name)
        self.assertNotEqual(all_versions[0]['version_id'], 'null')

        _headers, data = client.get_object(
            self.url, self.token, container_name, obj_name)
        self.assertEqual(data, b'new version')


if __name__ == '__main__':
    main()
