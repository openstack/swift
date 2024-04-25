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


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestMPU(unittest.TestCase):
    user_cont = uuid4().hex

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

    def _create_container(self, name, headers=None, use_account=1):
        headers = headers or {}
        resp = tf.retry(self._make_request, method='PUT', container=name,
                        headers=headers, use_account=use_account)
        if resp.status not in (201, 202):
            raise ResponseError(resp)

    def setUp(self):
        super(TestMPU, self).setUp()
        if tf.skip or tf.skip2:
            raise SkipTest
        if 'mpu' not in tf.cluster_info:
            raise SkipTest("MPU not enabled")
        try:
            self._create_container(self.user_cont)  # use_account=1
        except ResponseError as err:
            self.fail('Creating container: %s' % err)
        self.user_obj = uuid4().hex
        self._post_acl()  # always clear acls

    def _post_acl(self, read_acl='', write_acl=''):
        resp = tf.retry(self._make_request, method='POST',
                        headers={'X-Container-Read': read_acl,
                                 'X-Container-Write': write_acl},
                        container=self.user_cont)
        self.assertEqual(resp.status, 204)

    def _create_mpu(self, use_account=1, url_account=1):
        return tf.retry(self._make_request, method='POST',
                        headers={'Content-Type': 'application/test'},
                        container=self.user_cont, obj=self.user_obj,
                        query_string='uploads', use_account=use_account,
                        url_account=url_account)

    def _upload_part(self, upload_id, part_num, part_body,
                     use_account=1, url_account=1):
        return tf.retry(self._make_request, method='PUT',
                        container=self.user_cont, obj=self.user_obj,
                        query_string='upload-id=%s&part-number=%d'
                                     % (upload_id, part_num),
                        body=part_body, use_account=use_account,
                        url_account=url_account)

    def _upload_parts(self, upload_id, num_parts,
                      use_account=1, url_account=1):
        part_size = 5 * 1024 * 1024
        responses = []
        for part_index in range(num_parts):
            part_body = b'a' * part_size
            resp = self._upload_part(upload_id, part_index + 1, part_body,
                                     use_account, url_account)
            responses.append(resp)
        return responses

    def _complete_mpu(self, upload_id, etags,
                      use_account=1, url_account=1):
        # part numbers are assumed to be [etags] index + 1
        manifest = [{'part_number': i + 1, 'etag': etag}
                    for i, etag in enumerate(etags)]
        return tf.retry(self._make_request, method='POST',
                        container=self.user_cont, obj=self.user_obj,
                        query_string='upload-id=%s' % upload_id,
                        body=json.dumps(manifest).encode('ascii'),
                        use_account=use_account, url_account=url_account)

    def test_create_upload_complete_read_mpu(self):
        part_size = 5 * 1024 * 1024
        # create upload
        resp = self._create_mpu()
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # user object was not created
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj)
        self.assertEqual(404, resp.status)

        # upload parts
        etags = []
        part_1 = b'a' * part_size
        resp = self._upload_part(upload_id, 1, part_1)
        self.assertEqual(200, resp.status)
        etags.append(resp.getheader('Etag'))
        part_2 = b'b' * part_size
        resp = self._upload_part(upload_id, 2, part_2)
        self.assertEqual(200, resp.status)
        etags.append(resp.getheader('Etag'))

        # list parts
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj,
                        query_string='upload-id=%s' % upload_id)
        self.assertEqual(200, resp.status)
        parts = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(['%s/%s/%d' % (self.user_obj, upload_id, i + 1)
                          for i in range(2)], parts)

        # complete mpu
        resp = self._complete_mpu(upload_id, etags)
        self.assertEqual(200, resp.status, resp.content)

        # GET the user object
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj)
        self.assertEqual(200, resp.status)
        self.assertEqual(str(2 * 5 * 1024 * 1024),
                         resp.getheader('Content-Length'))
        self.assertEqual('application/test',
                         resp.getheader('Content-Type'))
        self.assertEqual(2 * part_size, len(resp.content))
        self.assertEqual(part_1, resp.content[:5 * 1024 * 1024])
        self.assertEqual(part_2, resp.content[-5 * 1024 * 1024:])

    def test_create_mpu_via_container_acl(self):
        # other account cannot create mpu without acl
        resp = self._create_mpu(use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual(200, resp.status)

        # other account cannot create mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._create_mpu(use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual(200, resp.status)

        # other account can create mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._create_mpu(use_account=2, url_account=1)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

    def test_upload_part_via_container_acl(self):
        resp = self._create_mpu()
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # other account cannot upload part without acl
        responses = self._upload_parts(upload_id, num_parts=1,
                                       use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual([200], [resp.status for resp in responses])

        # other account cannot upload part with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses = self._upload_parts(upload_id, num_parts=1,
                                       use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual([200], [resp.status for resp in responses])

        # other account can upload part with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses = self._upload_parts(upload_id, num_parts=1,
                                       use_account=2, url_account=1)
        self.assertEqual([200], [resp.status for resp in responses])

    def test_complete_mpu_via_container_acl(self):
        # create an in-progress mpu
        resp = self._create_mpu()
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        responses = self._upload_parts(upload_id, num_parts=1)
        self.assertEqual([200], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]

        # other account cannot complete mpu without acl
        resp = self._complete_mpu(upload_id, etags,
                                  use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual(200, resp.status)

        # other account cannot complete mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._complete_mpu(upload_id, etags,
                                  use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual(404, resp.status)

        # other account can complete mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._complete_mpu(upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(404, resp.status)

        # sanity check - creating account can read completed mpu
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj)
        self.assertEqual(200, resp.status)

    def test_read_mpu_via_container_acl(self):
        # create an mpu
        resp = self._create_mpu()
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        responses = self._upload_parts(upload_id, num_parts=1)
        self.assertEqual([200], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]
        resp = self._complete_mpu(upload_id, etags)
        self.assertEqual(200, resp.status)

        # same account can read it without acl
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj)
        self.assertEqual(200, resp.status)

        # other account cannot read it without acl
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj,
                        use_account=2, url_account=1)
        self.assertEqual(resp.status, 403)

        # other account cannot read it with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj,
                        use_account=2, url_account=1)
        self.assertEqual(resp.status, 403)

        # other account can read it with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.user_obj,
                        use_account=2, url_account=1)
        # TODO: XXX fix assertion!
        self.assertEqual(resp.status, 409)
