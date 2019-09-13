#!/usr/bin/python -u
# Copyright (c) 2019 OpenStack Foundation
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

from io import BytesIO
from uuid import uuid4

from swift.common.request_helpers import get_reserved_name

from test.probe.common import ReplProbeTest

from swiftclient import client, ClientException


class TestReservedNames(ReplProbeTest):

    def test_simple_crud(self):
        int_client = self.make_internal_client()

        # Create reserve named container
        user_cont = 'container-%s' % uuid4()
        reserved_cont = get_reserved_name('container-%s' % uuid4())
        client.put_container(self.url, self.token, user_cont)
        int_client.create_container(self.account, reserved_cont)

        # Check that we can list both reserved and non-reserved containers
        self.assertEqual([reserved_cont, user_cont], [
            c['name'] for c in int_client.iter_containers(self.account)])

        # sanity, user can't get to reserved name
        with self.assertRaises(ClientException) as cm:
            client.head_container(self.url, self.token, reserved_cont)
        self.assertEqual(412, cm.exception.http_status)

        user_obj = 'obj-%s' % uuid4()
        reserved_obj = get_reserved_name('obj-%s' % uuid4())

        # InternalClient can write & read reserved names fine
        int_client.upload_object(
            BytesIO(b'data'), self.account, reserved_cont, reserved_obj)
        int_client.get_object_metadata(
            self.account, reserved_cont, reserved_obj)
        _, _, app_iter = int_client.get_object(
            self.account, reserved_cont, reserved_obj)
        self.assertEqual(b''.join(app_iter), b'data')
        self.assertEqual([reserved_obj], [
            o['name']
            for o in int_client.iter_objects(self.account, reserved_cont)])

        # But reserved objects must be in reserved containers, and
        # user objects must be in user containers (at least for now)
        int_client.upload_object(
            BytesIO(b'data'), self.account, reserved_cont, user_obj,
            acceptable_statuses=(400,))

        int_client.upload_object(
            BytesIO(b'data'), self.account, user_cont, reserved_obj,
            acceptable_statuses=(400,))

        # Make sure we can clean up, too
        int_client.delete_object(self.account, reserved_cont, reserved_obj)
        int_client.delete_container(self.account, reserved_cont)

    def test_symlink_target(self):
        if 'symlink' not in self.cluster_info:
            raise unittest.SkipTest(
                "Symlink not enabled in proxy; can't test "
                "symlink to reserved name")
        int_client = self.make_internal_client()

        # create link container first, ensure account gets created too
        client.put_container(self.url, self.token, 'c1')

        # Create reserve named container
        tgt_cont = get_reserved_name('container-%s' % uuid4())
        int_client.create_container(self.account, tgt_cont)

        # sanity, user can't get to reserved name
        with self.assertRaises(ClientException) as cm:
            client.head_container(self.url, self.token, tgt_cont)
        self.assertEqual(412, cm.exception.http_status)

        tgt_obj = get_reserved_name('obj-%s' % uuid4())
        int_client.upload_object(
            BytesIO(b'target object'), self.account, tgt_cont, tgt_obj)
        metadata = int_client.get_object_metadata(
            self.account, tgt_cont, tgt_obj)
        etag = metadata['etag']

        # users can write a dynamic symlink that targets a reserved
        # name object
        client.put_object(
            self.url, self.token, 'c1', 'symlink',
            headers={
                'X-Symlink-Target': '%s/%s' % (tgt_cont, tgt_obj),
                'Content-Type': 'application/symlink',
            })

        # but can't read the symlink
        with self.assertRaises(ClientException) as cm:
            client.get_object(self.url, self.token, 'c1', 'symlink')
        self.assertEqual(412, cm.exception.http_status)

        # user's can't create static symlink to reserved name
        with self.assertRaises(ClientException) as cm:
            client.put_object(
                self.url, self.token, 'c1', 'static-symlink',
                headers={
                    'X-Symlink-Target': '%s/%s' % (tgt_cont, tgt_obj),
                    'X-Symlink-Target-Etag': etag,
                    'Content-Type': 'application/symlink',
                })
        self.assertEqual(412, cm.exception.http_status)

        # clean-up
        client.delete_object(self.url, self.token, 'c1', 'symlink')
        int_client.delete_object(self.account, tgt_cont, tgt_obj)
        int_client.delete_container(self.account, tgt_cont)
