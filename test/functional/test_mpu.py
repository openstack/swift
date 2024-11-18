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
from swift.common.swob import wsgi_quote, str_to_wsgi
from swift.common.middleware.mpu import MPUEtagHasher
from swift.common.swob import normalize_etag
from swift.common.utils import md5
from test.functional import ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class BaseTestMPU(unittest.TestCase):
    def setUp(self):
        super(BaseTestMPU, self).setUp()
        if tf.skip or tf.skip2:
            raise SkipTest
        if 'mpu' not in tf.cluster_info:
            raise SkipTest("MPU not enabled")
        try:
            self.user_cont = self._create_container()  # use_account=1
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

    def _post_acl(self, container=None, read_acl='', write_acl=''):
        container = container or self.user_cont
        resp = tf.retry(self._make_request, method='POST',
                        headers={'X-Container-Read': read_acl,
                                 'X-Container-Write': write_acl},
                        container=container)
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
                     headers=None, container=None, use_account=1,
                     url_account=1):
        container = container or self.user_cont
        return tf.retry(self._make_request, method='PUT',
                        container=container, obj=name,
                        query_string='upload-id=%s&part-number=%d'
                                     % (upload_id, part_num),
                        body=part_body,
                        headers=headers,
                        use_account=use_account,
                        url_account=url_account)

    def _upload_parts(self, name, upload_id, num_parts,
                      headers=None, container=None, use_account=1,
                      url_account=1):
        responses = []
        part_bodies = []
        for part_index in range(num_parts):
            part_body = chr(ord('a') + part_index) * self.part_size
            part_body = part_body.encode()
            resp = self._upload_part(
                name,
                upload_id,
                part_index + 1,
                part_body,
                headers=headers,
                container=container,
                use_account=use_account,
                url_account=url_account)
            responses.append(resp)
            part_bodies.append(part_body)
        return responses, part_bodies

    def _complete_mpu(self, name, upload_id, etags, container=None,
                      use_account=1, url_account=1):
        container = container or self.user_cont
        # part numbers are assumed to be [etags] index + 1
        manifest = [{'part_number': i + 1, 'etag': etag}
                    for i, etag in enumerate(etags)]
        return tf.retry(self._make_request, method='POST',
                        container=container, obj=name,
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

    def _make_mpu(self, name, container=None, num_parts=2,
                  extra_create_headers=None):
        container = container or self.user_cont
        # create an mpu
        resp = self._create_mpu(name, container=container,
                                extra_create_headers=extra_create_headers)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        # upload parts
        responses, part_bodies = self._upload_parts(
            name, upload_id, num_parts=num_parts, container=container)
        self.assertEqual([201, 201], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]
        part_hashes = [
            md5(part_body, usedforsecurity=False).hexdigest().encode('ascii')
            for part_body in part_bodies]
        hasher = md5(usedforsecurity=False)
        for part_hash in part_hashes:
            hasher.update(binascii.a2b_hex(part_hash))
        expected_mpu_etag = '%s-%d' % (hasher.hexdigest(), num_parts)
        # complete
        resp = self._complete_mpu(name, upload_id, etags, container=container)
        self.assertEqual(202, resp.status)
        resp_dict = json.loads(resp.content)
        self.assertIn('Etag', resp_dict)
        self.assertEqual(expected_mpu_etag, normalize_etag(resp_dict['Etag']))
        # verify
        resp = tf.retry(self._make_request, method='GET',
                        container=container, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual(str(num_parts),
                         resp.headers.get('X-Parts-Count'))
        self.assertEqual(str(num_parts * self.part_size),
                         resp.headers.get('Content-Length'))
        return upload_id, expected_mpu_etag, resp.content


class TestMPU(BaseTestMPU):

    @tf.requires_policies
    def setUp(self):
        super(TestMPU, self).setUp()
        self.mpu_name = uuid4().hex

    def test_list_mpu_sessions(self):
        container = self._create_container()
        created = []
        for obj in ('objy2', 'objx1', 'objy2'):
            resp = self._create_mpu(obj, container=container)
            self.assertEqual(202, resp.status, obj)
            created.append('%s/%s' % (obj, resp.headers.get('X-Upload-Id')))
        resp = self._create_mpu('objx4', container=container)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        resp = self._abort_mpu('objx4', upload_id, container=container)
        self.assertEqual(204, resp.status)

        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json')
        self.assertEqual(200, resp.status)
        # expect ordered by (name, created time)
        expected = [created[1], created[0], created[2]]
        actual = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # marker
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&marker=objy2')
        self.assertEqual(200, resp.status)
        expected = [created[0], created[2]]
        actual = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # end_marker
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&end_marker=objy')
        self.assertEqual(200, resp.status)
        expected = [created[1]]
        actual = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # prefix
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&prefix=objx')
        self.assertEqual(200, resp.status)
        expected = [created[1]]
        actual = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # limit
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&limit=2')
        self.assertEqual(200, resp.status)
        expected = [created[1], created[0]]
        actual = [item['name'] for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

    def test_create_upload_complete_read_mpu(self):
        # create upload
        user_headers = {
            'Content-Disposition': 'attachment',
            'Content-Encoding': 'none',
            'Content-Language': 'en-US',
            'Cache-Control': 'no-cache',
            'Expires': 'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        resp = self._create_mpu(self.mpu_name,
                                extra_create_headers=user_headers)
        self.assertEqual(202, resp.status)
        self.assertIn(b'Accepted', resp.content)
        self.assertEqual(
            {'Content-Type': 'text/html; charset=UTF-8',
             'X-Upload-Id': mock.ANY,
             'Content-Length': str(len(resp.content)),
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # user object was not created
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(404, resp.status)

        # upload parts
        part_resp_etags = []
        part_bodies = [b'a' * self.part_size, b'b' * self.part_size]
        part_etags = [md5(part_body, usedforsecurity=False).hexdigest()
                      for part_body in part_bodies]
        resp = self._upload_part(self.mpu_name, upload_id, 1, part_bodies[0])
        self.assertEqual(201, resp.status)
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
        resp = self._upload_part(self.mpu_name, upload_id, 2, part_bodies[1])
        self.assertEqual(201, resp.status)
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
                        container=self.user_cont, obj=self.mpu_name,
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
            [{'name': '%s/%s/%06d' % (self.mpu_name, upload_id, i + 1),
              'hash': part_etags[i],
              'bytes': self.part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}
             for i in range(2)],
            json.loads(resp.content))

        # complete mpu
        resp = self._complete_mpu(self.mpu_name, upload_id, part_resp_etags)
        self.assertEqual(202, resp.status, resp.content)
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
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        exp_headers = {
            'Content-Type': 'application/test',
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
            'Date': mock.ANY,
        }
        exp_headers.update(user_headers)
        self.assertEqual(exp_headers, resp.headers)
        self.assertEqual(2 * self.part_size, len(resp.content))
        self.assertEqual(part_bodies[0], resp.content[:5 * 1024 * 1024])
        self.assertEqual(part_bodies[1], resp.content[-5 * 1024 * 1024:])

    def test_create_upload_complete_mpu_retry_complete(self):
        # create upload
        resp = self._create_mpu(self.mpu_name)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # upload 3 parts
        part_resp_etags = []
        for i, x in enumerate([b'a', b'b', b'c']):
            part_body = x * self.part_size
            resp = self._upload_part(
                self.mpu_name, upload_id, i + 1, part_body)
            self.assertEqual(201, resp.status)
            part_resp_etags.append(resp.getheader('Etag'))

        etag_hasher = MPUEtagHasher()
        for part_etag in part_resp_etags[:2]:
            etag_hasher.update(part_etag)
            exp_mpu_etag = etag_hasher.etag

        # complete mpu using only 2 parts
        resp = self._complete_mpu(
            self.mpu_name, upload_id, part_resp_etags[:2])
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'), body)
        self.assertEqual(exp_mpu_etag, body.get('Etag'), body)
        self.assertEqual([], body['Errors'], body)

        # retry complete mpu with same manifest
        resp = self._complete_mpu(
            self.mpu_name, upload_id, part_resp_etags[:2])
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'), body)
        self.assertEqual(exp_mpu_etag, body.get('Etag'))
        self.assertEqual([], body['Errors'], body)

        # retry complete mpu with different manifest
        resp = self._complete_mpu(
            self.mpu_name, upload_id, part_resp_etags[1:])
        self.assertEqual(404, resp.status, resp.content)

        # retry complete on a different object path
        resp = self._complete_mpu(
            self.mpu_name + '-not', upload_id, part_resp_etags[:2])
        self.assertEqual(400, resp.status)
        self.assertIn(b'No such upload-id', resp.content)

        # GET the user object - check
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
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
        self.assertEqual(2 * self.part_size, len(resp.content))

    def test_make_mpu_auto_detect_content_type(self):
        # verify that content-type is detected and set on the manifest
        mpu_name = self.mpu_name + '.html'
        upload_id, expected_mpu_etag, content = self._make_mpu(
            mpu_name, extra_create_headers={'content-type': None})

        # GET the user object
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual('text/html', resp.getheader('Content-Type'))

        # GET the user container
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont,
                        query_string='format=json')
        self.assertEqual(200, resp.status)
        listing = json.loads(resp.content)
        self.assertEqual(1, len(listing))
        expected = {
            'name': mpu_name,
            'hash': '%s' % expected_mpu_etag,
            'bytes': 2 * self.part_size,
            'content_type': 'text/html',
            'last_modified': mock.ANY,
        }
        self.assertEqual(expected, listing[0])

    def test_create_mpu_via_container_acl(self):
        # other account cannot create mpu without acl
        resp = self._create_mpu(self.mpu_name, use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot create mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._create_mpu(self.mpu_name, use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can create mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._create_mpu(self.mpu_name, use_account=2, url_account=1)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

    def test_create_upload_abort_retry_abort(self):
        # create upload
        resp = self._create_mpu(self.mpu_name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # upload parts
        part_bodies = [b'a' * self.part_size]
        resp = self._upload_part(self.mpu_name, upload_id, 1, part_bodies[0])
        self.assertEqual(201, resp.status)

        # abort
        resp = self._abort_mpu(self.mpu_name, upload_id)
        self.assertEqual(204, resp.status)

        # user object was not created
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(404, resp.status)

        # retry abort
        resp = self._abort_mpu(self.mpu_name, upload_id)
        self.assertEqual(204, resp.status)

        # retry abort on a different object path
        resp = self._abort_mpu(self.mpu_name + '-not', upload_id)
        self.assertEqual(400, resp.status)
        self.assertIn(b'No such upload-id', resp.content)

    def test_upload_part(self):
        resp = self._create_mpu(self.mpu_name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        part_body = b'a' * self.part_size
        resp = self._upload_part(self.mpu_name, upload_id, 1, part_body)
        self.assertEqual(201, resp.status)

    def test_upload_part_bad_etag(self):
        resp = self._create_mpu(self.mpu_name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        part_body = b'a' * self.part_size
        headers = {'Etag': 'you must be mistaken'}
        resp = self._upload_part(self.mpu_name, upload_id, 1, part_body,
                                 headers=headers)
        self.assertEqual(422, resp.status)

    def test_upload_part_after_failed_complete(self):
        resp = self._create_mpu(self.mpu_name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        part_body = b'a' * self.part_size
        resp = self._upload_part(self.mpu_name, upload_id, 1, part_body)
        self.assertEqual(201, resp.status)
        part_resp_etag_1 = resp.getheader('Etag')

        # try to complete mpu using 2 parts but second part doesn't exist
        resp = self._complete_mpu(
            self.mpu_name, upload_id, [part_resp_etag_1] * 2)
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('400 Bad Request', body.get('Response Status'), body)
        self.assertEqual([["000002", "404 Not Found"]], body['Errors'], body)

        # upload second part
        part_body = b'b' * self.part_size
        resp = self._upload_part(self.mpu_name, upload_id, 2, part_body)
        self.assertEqual(201, resp.status)
        part_resp_etag_2 = resp.getheader('Etag')

        etag_hasher = MPUEtagHasher()
        for part_etag in (part_resp_etag_1, part_resp_etag_2):
            etag_hasher.update(part_etag)
            exp_mpu_etag = etag_hasher.etag

        # try to complete mpu again now second part exists
        resp = self._complete_mpu(
            self.mpu_name, upload_id, [part_resp_etag_1, part_resp_etag_2])
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'), body)
        self.assertEqual(exp_mpu_etag, body.get('Etag'), body)
        self.assertEqual([], body['Errors'], body)

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
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy to part from source object
        headers = {'x-copy-from': '/'.join([self.user_cont, src_name])}
        resp = self._upload_part(
            mpu_name, upload_id, 1, b'', headers=headers)
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(202, resp.status)

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
        self.assertEqual(202, resp.status)
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
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(202, resp.status)

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
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # copy to part from source object
        headers = {'x-copy-from': '/'.join([self.user_cont, src_name]),
                   'range': 'bytes=501-1500'}
        resp = self._upload_part(
            mpu_name, upload_id, 1, b'', headers=headers)
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(202, resp.status)

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
        self.assertEqual(202, resp.status)
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
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(202, resp.status)

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
        self.assertEqual(202, resp.status)
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
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(202, resp.status)

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
        self.assertEqual(202, resp.status)
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
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual('%s/%s' % (self.user_cont, src_name),
                         resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = self._complete_mpu(mpu_name, upload_id, [part_etag])
        self.assertEqual(202, resp.status)

        # verify mpu content - one part copied from entire other mpu
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1, int(resp.headers.get('x-parts-count')))
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))
        self.assertEqual(src_body, resp.content)

    def test_upload_part_via_container_acl(self):
        resp = self._create_mpu(self.mpu_name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # other account cannot upload part without acl
        responses, _ = self._upload_parts(
            self.mpu_name, upload_id, num_parts=1, use_account=2,
            url_account=1)
        self.assertEqual([403], [resp.status for resp in responses])

        # other account cannot upload part with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses, _ = self._upload_parts(
            self.mpu_name, upload_id, num_parts=1, use_account=2,
            url_account=1)
        self.assertEqual([403], [resp.status for resp in responses])

        # other account can upload part with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses, _ = self._upload_parts(
            self.mpu_name, upload_id, num_parts=1, use_account=2,
            url_account=1)
        self.assertEqual([201], [resp.status for resp in responses])

    def test_list_parts(self):
        name = uuid4().hex
        resp = self._create_mpu(name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        part_etags = []
        for i, part_body in enumerate([b'a' * self.part_size,
                                       b'b' * self.part_size]):
            resp = self._upload_part(name, upload_id, i + 1, part_body)
            self.assertEqual(201, resp.status)
            part_etags.append(normalize_etag(resp.headers.get('Etag')))

        expected = [{'name': '%s/%s/%06d' % (name, upload_id, i + 1),
                     'hash': part_etag,
                     'bytes': self.part_size,
                     'content_type': 'application/octet-stream',
                     'last_modified': mock.ANY}
                    for i, part_etag in enumerate(part_etags)]

        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=name,
                        query_string='upload-id=%s' % upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(expected, json.loads(resp.content))

        # with limit
        resp = tf.retry(
            self._make_request, method='GET',
            container=self.user_cont, obj=name,
            query_string='upload-id=%s&limit=1' % upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(expected[:1], json.loads(resp.content))

        # with part-number-marker
        resp = tf.retry(
            self._make_request, method='GET',
            container=self.user_cont, obj=name,
            query_string='upload-id=%s&part-number-marker=1' % upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(expected[1:], json.loads(resp.content))

    def test_complete_mpu_via_container_acl(self):
        # create an in-progress mpu
        resp = self._create_mpu(self.mpu_name)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        responses, _ = self._upload_parts(
            self.mpu_name, upload_id, num_parts=1)
        self.assertEqual([201], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]

        # other account cannot complete mpu without acl
        resp = self._complete_mpu(self.mpu_name, upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot complete mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._complete_mpu(self.mpu_name, upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can complete mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = self._complete_mpu(self.mpu_name, upload_id, etags,
                                  use_account=2, url_account=1)
        self.assertEqual(202, resp.status)

        # sanity check - creating account can read completed mpu
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)

    def test_post_mpu(self):
        name = uuid4().hex
        create_headers = {'content-type': 'application/test1',
                          'x-object-meta-test': 'one'}
        upload_id, mpu_etag, _body = self._make_mpu(
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

    def test_container_listing_with_mpu(self):
        container = self._create_container()
        _, mpu_etag, _ = self._make_mpu(self.mpu_name, container=container)
        # GET the user container
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='format=json')
        self.assertEqual(200, resp.status)
        self.assertEqual('1', resp.headers['X-Container-Object-Count'])
        self.assertEqual('0', resp.headers['X-Container-Bytes-Used'])
        listing = json.loads(resp.content)
        self.assertEqual(1, len(listing))
        expected = {
            'name': self.mpu_name,
            'hash': mpu_etag,
            'bytes': 2 * self.part_size,
            'content_type': 'application/test',
            'last_modified': mock.ANY,
        }
        self.assertEqual(expected, listing[0])

    def test_container_listing_with_mpu_via_container_acl(self):
        container = self._create_container()
        _, mpu_etag, _ = self._make_mpu(self.mpu_name, container=container)
        # other account cannot get listing without acl
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot get listing with write acl
        self._post_acl(container=container,
                       write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can get listing with read acl
        self._post_acl(container=container,
                       read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(self._make_request, method='GET',
                        container=container,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(200, resp.status)
        self.assertEqual(200, resp.status)
        self.assertEqual('1', resp.headers['X-Container-Object-Count'])
        self.assertEqual('0', resp.headers['X-Container-Bytes-Used'])
        listing = json.loads(resp.content)
        self.assertEqual(1, len(listing))
        expected = {
            'name': self.mpu_name,
            'hash': mpu_etag,
            'bytes': 2 * self.part_size,
            'content_type': 'application/test',
            'last_modified': mock.ANY,
        }
        self.assertEqual(expected, listing[0])

    def test_read_mpu_via_container_acl(self):
        self._make_mpu(self.mpu_name)
        # same account can read mpu without acl
        name = self.mpu_name
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

    def _do_test_get_head_mpu(self, method, conditional=False, headers=None):
        num_parts = 2
        upload_id, mpu_etag, _body = self._make_mpu(
            self.mpu_name, num_parts=num_parts)
        headers = headers or {}
        if conditional:
            headers['If-Match'] = mpu_etag
        resp = tf.retry(self._make_request, method=method,
                        container=self.user_cont, obj=self.mpu_name,
                        headers=headers)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu_etag,
             'X-Upload-Id': upload_id,
             'X-Parts-Count': str(num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': str(self.part_size * num_parts),
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        return resp

    def test_get_mpu(self):
        resp = self._do_test_get_head_mpu('GET')
        self.assertEqual(2 * self.part_size, len(resp.content))

    def test_head_mpu(self):
        resp = self._do_test_get_head_mpu('HEAD')
        self.assertEqual(b'', resp.content)

    def test_get_mpu_if_match(self):
        resp = self._do_test_get_head_mpu('GET', conditional=True)
        self.assertEqual(2 * self.part_size, len(resp.content))

    def test_head_mpu_if_match(self):
        resp = self._do_test_get_head_mpu('HEAD', conditional=True)
        self.assertEqual(b'', resp.content)

    def test_get_mpu_if_none_match_does_not_match(self):
        resp = self._do_test_get_head_mpu('GET',
                                          headers={'If-None-Match': 'foo'})
        self.assertEqual(2 * self.part_size, len(resp.content))

    def test_head_mpu_if_none_match_does_not_match(self):
        resp = self._do_test_get_head_mpu('HEAD',
                                          headers={'If-None-Match': 'foo'})
        self.assertEqual(b'', resp.content)

    def _do_test_get_head_mpu_if_match_does_not_match(self, method):
        num_parts = 2
        upload_id, mpu_etag, _body = self._make_mpu(
            self.mpu_name, num_parts=num_parts)
        headers = {'If-Match': 'foo'}
        resp = tf.retry(self._make_request, method=method,
                        container=self.user_cont, obj=self.mpu_name,
                        headers=headers)
        self.assertEqual(412, resp.status)
        self.assertEqual(
            {'Content-Type': 'text/html; charset=UTF-8',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu_etag,
             'X-Upload-Id': upload_id,
             'X-Parts-Count': str(num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        self.assertEqual(b'', resp.content)

    def test_get_mpu_if_match_does_not_match_412(self):
        self._do_test_get_head_mpu_if_match_does_not_match('GET')

    def test_head_mpu_if_match_does_not_match_412(self):
        self._do_test_get_head_mpu_if_match_does_not_match('HEAD')

    def _do_test_get_head_mpu_if_none_match_does_match(self, method):
        num_parts = 2
        upload_id, mpu_etag, _body = self._make_mpu(
            self.mpu_name, num_parts=num_parts)
        headers = {'If-None-Match': mpu_etag}
        resp = tf.retry(self._make_request, method=method,
                        container=self.user_cont, obj=self.mpu_name,
                        headers=headers)
        self.assertEqual(304, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu_etag,
             'X-Upload-Id': upload_id,
             'X-Parts-Count': str(num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        self.assertEqual(b'', resp.content)

    def test_get_mpu_if_none_match_does_match_304(self):
        self._do_test_get_head_mpu_if_none_match_does_match('GET')

    def test_head_mpu_if_none_match_does_match_304(self):
        self._do_test_get_head_mpu_if_none_match_does_match('HEAD')

    def test_get_mpu_with_part_number(self):
        num_parts = 2
        upload_id, mpu_etag, _body = self._make_mpu(
            self.mpu_name, num_parts=num_parts)
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name,
                        query_string='part-number=1')
        self.assertEqual(206, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu_etag,
             'X-Upload-Id': upload_id,
             'X-Parts-Count': str(num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': str(self.part_size),
             'Content-Range': 'bytes 0-5242879/10485760',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        self.assertEqual(self.part_size, len(resp.content))

    def test_copy_mpu(self):
        upload_id, mpu_etag, mpu_body = self._make_mpu(self.mpu_name)
        copy_name = uuid4().hex
        resp = tf.retry(
            self._make_request,
            method='COPY',
            container=self.user_cont,
            obj=self.mpu_name,
            headers={
                'destination': '/'.join([self.user_cont, copy_name]),
            },
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(
            wsgi_quote(str_to_wsgi('%s/%s' % (self.user_cont, self.mpu_name))),
            resp.headers.get('X-Copied-From'))

        # GET the copy
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=copy_name)
        self.assertEqual(200, resp.status)
        self.assertNotIn('X-Parts-Count', resp.headers)
        # verify copy content
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))
        self.assertEqual(mpu_body[0], resp.content[1])
        self.assertEqual(mpu_body[-1], resp.content[-1])

    def test_copy_mpu_with_part_number(self):
        upload_id, mpu_etag, mpu_body = self._make_mpu(self.mpu_name)
        copy_name = uuid4().hex
        resp = tf.retry(
            self._make_request,
            method='COPY',
            container=self.user_cont,
            obj=self.mpu_name,
            headers={
                'destination': '/'.join([self.user_cont, copy_name]),
            },
            query_string='part-number=1',
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(
            wsgi_quote(str_to_wsgi('%s/%s' % (self.user_cont, self.mpu_name))),
            resp.headers.get('X-Copied-From'))

        # GET the copy
        resp = tf.retry(self._make_request, method='GET',
                        container=self.user_cont, obj=copy_name)
        self.assertNotIn('X-Parts-Count', resp.headers)
        self.assertEqual(200, resp.status)
        # verify copy content
        self.assertEqual(
            self.part_size, int(resp.headers.get('Content-Length')))
        self.assertEqual(mpu_body[0], resp.content[1])
        self.assertEqual(mpu_body[self.part_size - 1], resp.content[-1])


class TestMPUUTF8(TestMPU):

    @tf.requires_policies
    def setUp(self):
        super(TestMPUUTF8, self).setUp()
        self.mpu_name = uuid4().hex + u'\N{SNOWMAN}'
