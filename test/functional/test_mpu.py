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
import binascii
import json
import mock
import unittest
from unittest import SkipTest
from uuid import uuid4

import test.functional as tf
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import normalize_etag
from swift.common.utils import md5
from test.functional import ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class BaseTestMPU(unittest.TestCase):
    user_cont = uuid4().hex

    def setUp(self):
        super(BaseTestMPU, self).setUp()
        if tf.skip or tf.skip2:
            raise SkipTest
        if 'mpu' not in tf.cluster_info:
            raise SkipTest("MPU not enabled")
        try:
            self._create_container(self.user_cont)  # use_account=1
        except ResponseError as err:
            self.fail('Creating container: %s' % err)
        self._post_acl()  # always clear acls
        self.part_size = 5 * 1024 * 1024

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

    def _upload_obj(self, name, body, headers=None, use_account=1,
                    url_account=1):
        return tf.retry(self._make_request,
                        method='PUT',
                        container=self.user_cont,
                        obj=name,
                        body=body,
                        headers=headers,
                        use_account=use_account,
                        url_account=url_account)

    def _create_container(self, name=None, headers=None, use_account=1):
        name = name or uuid4().hex
        headers = headers or {}
        resp = tf.retry(self._make_request, method='PUT', container=name,
                        headers=headers, use_account=use_account)
        if resp.status not in (201, 202):
            raise ResponseError(resp)
        return name

    def _post_acl(self, read_acl='', write_acl=''):
        resp = tf.retry(self._make_request, method='POST',
                        headers={'X-Container-Read': read_acl,
                                 'X-Container-Write': write_acl},
                        container=self.user_cont)
        self.assertEqual(resp.status, 204)

    def _create_mpu(self, name, container=None, use_account=1, url_account=1,
                    extra_create_headers=None):
        container = container or self.user_cont
        headers = HeaderKeyDict({'Content-Type': 'application/test'})
        headers.update(extra_create_headers or {})
        return tf.retry(self._make_request, method='POST',
                        headers=headers,
                        container=container, obj=name,
                        query_string='uploads', use_account=use_account,
                        url_account=url_account)

    def _upload_part(self, name, upload_id, part_num, part_body,
                     headers=None, use_account=1, url_account=1):
        return tf.retry(self._make_request,
                        method='PUT',
                        container=self.user_cont,
                        obj=name,
                        query_string='upload-id=%s&part-number=%d'
                                     % (upload_id, part_num),
                        body=part_body,
                        headers=headers,
                        use_account=use_account,
                        url_account=url_account)

    def _upload_parts(self, name, upload_id, num_parts,
                      headers=None, use_account=1, url_account=1):
        responses = []
        part_bodies = []
        for part_index in range(num_parts):
            part_body = b'a' * self.part_size
            resp = self._upload_part(
                name,
                upload_id,
                part_index + 1,
                part_body,
                headers=headers,
                use_account=use_account,
                url_account=url_account)
            responses.append(resp)
            part_bodies.append(part_body)
        return responses, part_bodies

    def _complete_mpu(self, name, upload_id, etags,
                      use_account=1, url_account=1):
        # part numbers are assumed to be [etags] index + 1
        manifest = [{'part_number': i + 1, 'etag': etag}
                    for i, etag in enumerate(etags)]
        return tf.retry(self._make_request, method='POST',
                        container=self.user_cont, obj=name,
                        query_string='upload-id=%s' % upload_id,
                        body=json.dumps(manifest).encode('ascii'),
                        use_account=use_account, url_account=url_account)

    def _abort_mpu(self, name, upload_id, container=None,
                   use_account=1, url_account=1):
        container = container or self.user_cont
        return tf.retry(self._make_request, method='DELETE',
                        container=container, obj=name,
                        query_string='upload-id=%s' % upload_id,
                        use_account=use_account, url_account=url_account)

    def _make_mpu(self, name, num_parts=2, extra_create_headers=None):
        # create an mpu
        resp = self._create_mpu(name,
                                extra_create_headers=extra_create_headers)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        responses, part_bodies = self._upload_parts(
            name, upload_id, num_parts=num_parts)
        self.assertEqual([200, 200], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]
        part_hashes = [
            md5(part_body, usedforsecurity=False).hexdigest().encode('ascii')
            for part_body in part_bodies]
        hasher = md5(usedforsecurity=False)
        for part_hash in part_hashes:
            hasher.update(binascii.a2b_hex(part_hash))
        expected_mpu_etag = '%s-%d' % (hasher.hexdigest(), num_parts)
        resp = self._complete_mpu(name, upload_id, etags)
        self.assertEqual(200, resp.status)
        resp_dict = json.loads(resp.content)
        self.assertIn('Etag', resp_dict)
        self.assertEqual(expected_mpu_etag, normalize_etag(resp_dict['Etag']))
        return upload_id, expected_mpu_etag


class TestMPU(BaseTestMPU):
    # tests in this class create MPUs

    @tf.requires_policies
    def setUp(self):
        super(TestMPU, self).setUp()

    def test_list_mpus(self):
        container = self._create_container()
        created = []
        for obj in ('obj2', 'obj1', 'obj2'):
            resp = self._create_mpu(obj, container=container)
            self.assertEqual(200, resp.status, obj)
            created.append('%s/%s' % (obj, resp.headers.get('X-Upload-Id')))
        resp = self._create_mpu('obj4', container=container)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        resp = self._abort_mpu('obj4', upload_id, container=container)
        self.assertEqual(204, resp.status)

        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json')
        self.assertEqual(200, resp.status)
        # expect ordered by (name, created time)
        expected = [created[1], created[0], created[2]]
        actual = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

    def test_create_upload_complete_read_mpu(self):
        name = uuid4().hex
        part_size = 5 * 1024 * 1024
        # create upload
        resp = self._create_mpu(name)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'text/html; charset=UTF-8',
             'X-Upload-Id': mock.ANY,
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # user object was not created
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name)
        self.assertEqual(404, resp.status)

        # upload parts
        part_resp_etags = []
        part_bodies = [b'a' * part_size, b'b' * part_size]
        part_etags = [md5(part_body, usedforsecurity=False).hexdigest()
                      for part_body in part_bodies]
        resp = self._upload_part(name, upload_id, 1, part_bodies[0])
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'text/html; charset=UTF-8',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % part_etags[0],
             'Last-Modified': mock.ANY,
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers)
        part_resp_etags.append(resp.getheader('Etag'))
        resp = self._upload_part(name, upload_id, 2, part_bodies[1])
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'text/html; charset=UTF-8',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % part_etags[1],
             'Last-Modified': mock.ANY,
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers)
        part_resp_etags.append(resp.getheader('Etag'))
        etag_hasher = md5(usedforsecurity=False)
        for part_etag in part_resp_etags:
            etag_hasher.update(binascii.a2b_hex(normalize_etag(part_etag)))
        exp_mpu_etag = etag_hasher.hexdigest() + '-2'

        # list parts
        exp_policy = self.policies.default['name']
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name,
                        query_string='upload-id=%s' % upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/json; charset=utf-8',
             'Content-Length': str(len(resp.content)),
             'X-Storage-Policy': exp_policy,
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers)
        self.assertEqual(
            [{'name': '%s/%s/%06d' % (name, upload_id, i + 1),
              'hash': part_etags[i],
              'bytes': part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}
             for i in range(2)],
            json.loads(resp.content))

        # complete mpu
        resp = self._complete_mpu(name, upload_id, part_resp_etags)
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual(
            {'Content-Type': 'text/html; charset=UTF-8',
             'Transfer-Encoding': 'chunked',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'), body)
        self.assertEqual(exp_mpu_etag, body.get('Etag'))
        self.assertEqual([], body['Errors'], body)

        # GET the user object
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: mpu etags are always quoted by MPU middleware
             'Etag': '"%s"' % exp_mpu_etag,
             'X-Upload-Id': upload_id,
             'X-Parts-Count': '2',
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': '10485760',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        self.assertEqual(2 * part_size, len(resp.content))
        self.assertEqual(part_bodies[0], resp.content[:5 * 1024 * 1024])
        self.assertEqual(part_bodies[1], resp.content[-5 * 1024 * 1024:])

    def test_make_mpu_no_user_content_type(self):
        name = uuid4().hex
        self._make_mpu(name, extra_create_headers={'content_type': None})
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual('application/test',
                         resp.getheader('Content-Type'))

    def test_create_mpu_via_container_acl(self):
        # other account cannot create mpu without acl
        name = uuid4().hex
        resp = self._create_mpu(name, use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot create mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._create_mpu(name, use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can create mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._create_mpu(name, use_account=2, url_account=1)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

    def test_upload_part(self):
        name = uuid4().hex
        resp = self._create_mpu(name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        part_body = b'a' * self.part_size
        resp = self._upload_part(name, upload_id, 1, part_body)
        self.assertEqual(200, resp.status)

    def test_upload_part_copy_using_PUT(self):
        # upload source object
        src_name = uuid4().hex
        src_body = ''.join([chr(i) * 1000 for i in range(ord('a'), ord('z'))])
        src_body = src_body.encode('utf8')
        resp = self._upload_obj(src_name, src_body)
        self.assertEqual(201, resp.status)

        # create mpu session
        mpu_name = uuid4().hex
        resp = self._create_mpu(mpu_name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy to part from source object
        headers = {'x-copy-from': '/'.join([self.user_cont, src_name])}
        resp = self._upload_part(
            mpu_name, upload_id, 1, b'', headers=headers)
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(200, resp.status)

        # verify mpu content
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            len(src_body), int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body, resp.content)

    def test_upload_part_copy_using_COPY(self):
        # upload source object
        src_name = uuid4().hex
        src_body = ''.join([chr(i) * 1000 for i in range(ord('a'), ord('z'))])
        src_body = src_body.encode('utf8')
        resp = self._upload_obj(src_name, src_body)
        self.assertEqual(201, resp.status)

        # create mpu session
        mpu_name = uuid4().hex
        resp = self._create_mpu(mpu_name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy to part from source object
        resp = tf.retry(
            self._make_request,
            method='COPY',
            container=self.user_cont,
            obj=src_name,
            query_string='upload-id=%s&part-number=%d' % (upload_id, 1),
            headers={
                'destination': '/'.join([self.user_cont, mpu_name]),
            }
        )
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(200, resp.status)

        # verify mpu content
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            len(src_body), int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body, resp.content)

    def test_upload_part_copy_with_range_using_PUT(self):
        # upload source object
        src_name = uuid4().hex
        src_body = ''.join([chr(i) * 1000 for i in range(ord('a'), ord('z'))])
        src_body = src_body.encode('utf8')
        resp = self._upload_obj(src_name, src_body)
        self.assertEqual(201, resp.status)

        # create mpu session
        mpu_name = uuid4().hex
        resp = self._create_mpu(mpu_name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy to part from source object
        headers = {'x-copy-from': '/'.join([self.user_cont, src_name]),
                   'range': 'bytes=501-1500'}
        resp = self._upload_part(
            mpu_name, upload_id, 1, b'', headers=headers)
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(200, resp.status)

        # verify mpu content
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1000, int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body[501:1501], resp.content)

    def test_upload_part_copy_with_range_using_COPY(self):
        # upload source object
        src_name = uuid4().hex
        src_body = ''.join([chr(i) * 1000 for i in range(ord('a'), ord('z'))])
        src_body = src_body.encode('utf8')
        resp = self._upload_obj(src_name, src_body)
        self.assertEqual(201, resp.status)

        # create mpu session
        mpu_name = uuid4().hex
        resp = self._create_mpu(mpu_name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy to part from source object
        resp = tf.retry(
            self._make_request,
            method='COPY',
            container=self.user_cont,
            obj=src_name,
            query_string='upload-id=%s&part-number=%d' % (upload_id, 1),
            headers={
                'destination': '/'.join([self.user_cont, mpu_name]),
                'range': 'bytes=501-1500',
            }
        )
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(200, resp.status)

        # verify mpu content
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1000, int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body[501:1501], resp.content)

    def test_upload_part_copy_from_other_mpu_using_PUT(self):
        # upload source mpu object
        src_name = uuid4().hex
        self._make_mpu(src_name)

        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=src_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))
        src_body = resp.content

        # create mpu session
        # note: the part-number applies to the target mpu session, not the
        # source object
        mpu_name = uuid4().hex
        resp = self._create_mpu(mpu_name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy new part from source object
        # note: the part-number applies to the target mpu session, not the
        # source object
        resp = tf.retry(
            self._make_request,
            method='PUT',
            container=self.user_cont,
            obj=mpu_name,
            query_string='upload-id=%s&part-number=%d' % (upload_id, 1),
            headers={
                'x-copy-from': '/'.join([self.user_cont, src_name]),
            }
        )
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(200, resp.status)

        # verify mpu content - one part copied from entire other mpu
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1, int(resp.headers.get('x-parts-count')))
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body, resp.content)

    def test_upload_part_copy_from_other_mpu_using_COPY(self):
        # upload source mpu object
        src_name = uuid4().hex
        self._make_mpu(src_name)

        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=src_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))
        src_body = resp.content

        # create mpu session
        mpu_name = uuid4().hex
        resp = self._create_mpu(mpu_name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy new part from source object
        # note: the part-number applies to the target mpu session, not the
        # source object
        resp = tf.retry(
            self._make_request,
            method='COPY',
            container=self.user_cont,
            obj=src_name,
            query_string='upload-id=%s&part-number=%d' % (upload_id, 1),
            headers={
                'destination': '/'.join([self.user_cont, mpu_name]),
            }
        )
        self.assertEqual(200, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(200, resp.status)

        # verify mpu content - one part copied from entire other mpu
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1, int(resp.headers.get('x-parts-count')))
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body, resp.content)

    def test_upload_part_via_container_acl(self):
        name = uuid4().hex
        resp = self._create_mpu(name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # other account cannot upload part without acl
        responses, _ = self._upload_parts(name, upload_id, num_parts=1,
                                          use_account=2, url_account=1)
        self.assertEqual([403], [resp.status for resp in responses])

        # other account cannot upload part with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses, _ = self._upload_parts(name, upload_id, num_parts=1,
                                          use_account=2, url_account=1)
        self.assertEqual([403], [resp.status for resp in responses])

        # other account can upload part with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses, _ = self._upload_parts(name, upload_id, num_parts=1,
                                          use_account=2, url_account=1)
        self.assertEqual([200], [resp.status for resp in responses])

    def test_complete_mpu_via_container_acl(self):
        # create an in-progress mpu
        name = uuid4().hex
        resp = self._create_mpu(name)
        self.assertEqual(200, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        responses, _ = self._upload_parts(name, upload_id, num_parts=1)
        self.assertEqual([200], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]

        # other account cannot complete mpu without acl
        resp = self._complete_mpu(name, upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot complete mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._complete_mpu(name, upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can complete mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._complete_mpu(name, upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(200, resp.status)

        # sanity check - creating account can read completed mpu
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)

    def test_post_mpu(self):
        name = uuid4().hex
        create_headers = {'content-type': 'application/test1',
                          'x-object-meta-test': 'one'}
        upload_id, mpu_etag = self._make_mpu(
            name, extra_create_headers=create_headers)
        resp = tf.retry(self._make_request, method='HEAD',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual({
            'Content-Type': 'application/test1',
            'Etag': '"%s"' % mpu_etag,
            'X-Upload-Id': upload_id,
            'X-Parts-Count': '2',
            'Last-Modified': mock.ANY,
            'X-Timestamp': mock.ANY,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(self.part_size * 2),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
            'x-object-meta-test': 'one',
        }, resp.headers)

        # update content-type and metadata
        post_headers = {'content-type': 'application/test2',
                        'x-object-meta-test-2': 'two'}
        resp = tf.retry(self._make_request, method='POST',
                        container=self.user_cont, obj=name,
                        headers=post_headers)
        self.assertEqual(202, resp.status)
        resp = tf.retry(self._make_request, method='HEAD',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual({
            'Content-Type': 'application/test2',
            'Etag': '"%s"' % mpu_etag,
            'X-Upload-Id': upload_id,
            'X-Parts-Count': '2',
            'Last-Modified': mock.ANY,
            'X-Timestamp': mock.ANY,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(self.part_size * 2),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
            'x-object-meta-test-2': 'two',
        }, resp.headers)

        # update only metadata
        post_headers = {'x-object-meta-test-3': 'three'}
        resp = tf.retry(self._make_request, method='POST',
                        container=self.user_cont, obj=name,
                        headers=post_headers)
        self.assertEqual(202, resp.status)
        resp = tf.retry(self._make_request, method='HEAD',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual({
            'Content-Type': 'application/test2',
            'Etag': '"%s"' % mpu_etag,
            'X-Upload-Id': upload_id,
            'X-Parts-Count': '2',
            'Last-Modified': mock.ANY,
            'X-Timestamp': mock.ANY,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(self.part_size * 2),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
            'x-object-meta-test-3': 'three',
        }, resp.headers)


class TestExistingMPU(BaseTestMPU):
    # all tests in this class use the same pre-existing MPU, created once
    # during a call to setUp()
    user_cont = uuid4().hex
    user_obj = user_obj_etag = upload_id = None

    def setUp(self):
        super(TestExistingMPU, self).setUp()
        # ...but only create an mpu once
        self.num_parts = 2
        if not TestExistingMPU.user_obj:
            user_obj = uuid4().hex
            TestExistingMPU.upload_id, TestExistingMPU.user_obj_etag = \
                self._make_mpu(user_obj, num_parts=self.num_parts)
            TestExistingMPU.user_obj = user_obj

    def _verify_listing(self, resp):
        self.maxDiff = None
        self.assertEqual('1', resp.headers['X-Container-Object-Count'])
        self.assertEqual('0', resp.headers['X-Container-Bytes-Used'])
        listing = json.loads(resp.content)
        self.assertEqual(1, len(listing))
        expected = {
            'name': self.user_obj,
            'hash': self.user_obj_etag,
            'bytes': 2 * self.part_size,
            'content_type': 'application/test',
            'last_modified': mock.ANY,
        }
        self.assertEqual(expected, listing[0])

    def test_container_listing_with_mpu(self):
        # GET the user container
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont,
                        query_string='format=json')
        self.assertEqual(200, resp.status)
        self._verify_listing(resp)

    def test_container_listing_with_mpu_via_container_acl(self):
        # other account cannot get listing without acl
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot get listing with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can get listing with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(200, resp.status)
        self._verify_listing(resp)

    def test_read_mpu_via_container_acl(self):
        # same account can read mpu without acl
        name = self.user_obj
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name)
        self.assertEqual(200, resp.status)

        # other account cannot read mpu without acl
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name,
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot read mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name,
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can read mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name,
                        use_account=2, url_account=1)
        self.assertEqual(200, resp.status)

    def _do_test_get_mpu(self, method):
        resp = tf.retry(self._make_request, method=method,
                        container=self.user_cont, obj=self.user_obj)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % self.user_obj_etag,
             'X-Upload-Id': self.upload_id,
             'X-Parts-Count': str(self.num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': str(self.part_size * self.num_parts),
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        return resp

    def test_get_mpu(self):
        resp = self._do_test_get_mpu('GET')
        self.assertEqual(2 * self.part_size, len(resp.content))

    def test_head_mpu(self):
        resp = self._do_test_get_mpu('HEAD')
        self.assertEqual(0, len(resp.content))
