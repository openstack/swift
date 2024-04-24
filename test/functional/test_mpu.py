# Copyright (c) 2024 Nvidia
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
import json
import unittest
from unittest import SkipTest
from uuid import uuid4

import test.functional as tf
from test.functional import ResponseError
from test.functional.tests import BaseEnv


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestMpuEnv(BaseEnv):
    user_cont = uuid4().hex
    user_obj = uuid4().hex

    @classmethod
    def setUp(cls):
        if tf.skip or tf.skip2:
            raise SkipTest

        cls._create_container(cls.user_cont)  # use_account=1

    @classmethod
    def _make_request(self, env, token, parsed, conn, method,
                      container, obj='', headers=None, body=b'',
                      query_string=None):
        headers = headers or {}
        headers.update({'X-Auth-Token': token})
        path = '%s/%s/%s' % (parsed.path, container, obj) if obj \
               else '%s/%s' % (parsed.path, container)
        if query_string:
            path += '?%s' % query_string
        conn.request(method, path, body, headers)
        resp = tf.check_response(conn)
        # to read the buffer and keep it in the attribute, call resp.content
        resp.content
        return resp

    @classmethod
    def _create_container(cls, name, headers=None, use_account=1):
        headers = headers or {}
        resp = tf.retry(cls._make_request, method='PUT', container=name,
                        headers=headers, use_account=use_account)
        if resp.status not in (201, 202):
            raise ResponseError(resp)
        return name


class TestMpu(unittest.TestCase):
    env = TestMpuEnv

    def setUp(self):
        super(TestMpu, self).setUp()
        self.env.setUp()
        if 'mpu' not in tf.cluster_info:
            raise SkipTest("MPU not enabled")

    def test_mpu_create(self):
        part_size = 5 * 1024 * 1024
        # create upload
        resp = tf.retry(self.env._make_request, method='POST',
                        headers={'Content-Type': 'application/test'},
                        container=self.env.user_cont, obj=self.env.user_obj,
                        query_string='uploads')
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # user object was not created
        resp = tf.retry(self.env._make_request, method='GET',
                        container=self.env.user_cont, obj=self.env.user_obj)
        self.assertEqual(404, resp.status)

        # upload parts
        etags = []
        part_1 = b'a' * part_size
        resp = tf.retry(self.env._make_request, method='PUT',
                        container=self.env.user_cont, obj=self.env.user_obj,
                        query_string='upload-id=%s&part-number=1' % upload_id,
                        body=part_1)
        self.assertEqual(200, resp.status)
        etags.append(resp.getheader('Etag'))
        part_2 = b'b' * part_size
        resp = tf.retry(self.env._make_request, method='PUT',
                        container=self.env.user_cont, obj=self.env.user_obj,
                        query_string='upload-id=%s&part-number=2' % upload_id,
                        body=part_2)
        self.assertEqual(200, resp.status)
        etags.append(resp.getheader('Etag'))

        # list parts
        resp = tf.retry(self.env._make_request, method='GET',
                        container=self.env.user_cont, obj=self.env.user_obj,
                        query_string='upload-id=%s' % upload_id)
        self.assertEqual(200, resp.status)
        parts = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(['%s/%s/%d' % (self.env.user_obj, upload_id, i + 1)
                          for i in range(2)], parts)

        # complete mpu
        manifest = [{'part_number': i + 1, 'etag': etag}
                    for i, etag in enumerate(etags)]
        resp = tf.retry(self.env._make_request, method='POST',
                        container=self.env.user_cont, obj=self.env.user_obj,
                        query_string='upload-id=%s' % upload_id,
                        body=json.dumps(manifest).encode('ascii'))
        self.assertEqual(200, resp.status, resp.content)

        # GET the user object
        resp = tf.retry(self.env._make_request, method='GET',
                        container=self.env.user_cont, obj=self.env.user_obj)
        self.assertEqual(200, resp.status)
        self.assertEqual(str(2 * 5 * 1024 * 1024),
                         resp.getheader('Content-Length'))
        self.assertEqual('application/test',
                         resp.getheader('Content-Type'))
        self.assertEqual(2 * part_size, len(resp.content))
        self.assertEqual(part_1, resp.content[:5 * 1024 * 1024])
        self.assertEqual(part_2, resp.content[-5 * 1024 * 1024:])
