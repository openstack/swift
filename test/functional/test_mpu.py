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


def _get_default_policy():
    for policy in tf.cluster_info['swift'].get('policies', []):
        if policy.get('default'):
            return policy.get('name')
    return None


def _make_request(env, token, parsed, conn, method,
                  container=None, obj='', headers=None, body=b'',
                  query_string=None):
    headers = headers or {}
    headers.update({'X-Auth-Token': token})
    if obj:
        path = '%s/%s/%s' % (parsed.path, container, obj)
    elif container:
        path = '%s/%s' % (parsed.path, container)
    else:
        path = parsed.path
    if query_string:
        path += '?%s' % query_string
    conn.request(method, path, body, headers)
    resp = tf.check_response(conn)
    # to read the buffer and keep it in the attribute, call resp.content
    resp.content
    return resp


def _create_container(name=None, headers=None, use_account=1):
    name = name or uuid4().hex
    headers = headers or {}
    resp = tf.retry(_make_request, method='PUT', container=name,
                    headers=headers, use_account=use_account)
    if resp.status not in (201, 202):
        raise ResponseError(resp)
    return name


def _upload_obj(container, name, body, headers=None, use_account=1,
                url_account=1):
    return tf.retry(_make_request,
                    method='PUT',
                    container=container,
                    obj=name,
                    body=body,
                    headers=headers,
                    use_account=use_account,
                    url_account=url_account)


def _create_mpu(container, name, use_account=1, url_account=1,
                extra_create_headers=None):
    headers = HeaderKeyDict({'Content-Type': 'application/test'})
    headers.update(extra_create_headers or {})
    return tf.retry(_make_request,
                    method='POST',
                    headers=headers,
                    container=container,
                    obj=name,
                    query_string='uploads',
                    use_account=use_account,
                    url_account=url_account)


def _upload_part(container, name, upload_id, part_num, part_body,
                 headers=None, use_account=1, url_account=1):
    return tf.retry(_make_request,
                    method='PUT',
                    container=container,
                    obj=name,
                    query_string='upload-id=%s&part-number=%d'
                                 % (upload_id, part_num),
                    body=part_body,
                    headers=headers,
                    use_account=use_account,
                    url_account=url_account)


def _complete_mpu(container, name, upload_id, etags, use_account=1,
                  url_account=1):
    # part numbers are assumed to be [etags] index + 1
    manifest = [{'part_number': i + 1, 'etag': etag}
                for i, etag in enumerate(etags)]
    return tf.retry(_make_request,
                    method='POST',
                    container=container,
                    obj=name,
                    query_string='upload-id=%s' % upload_id,
                    body=json.dumps(manifest).encode('ascii'),
                    use_account=use_account,
                    url_account=url_account)


def _abort_mpu(container, name, upload_id, use_account=1, url_account=1):
    return tf.retry(_make_request,
                    method='DELETE',
                    container=container,
                    obj=name,
                    query_string='upload-id=%s' % upload_id,
                    use_account=use_account,
                    url_account=url_account)


def list_mpu_sessions(container):
    return tf.retry(_make_request, method='GET',
                    container=container,
                    query_string='uploads&format=json')


def _post_acl(container, read_acl='', write_acl=''):
    return tf.retry(_make_request, method='POST',
                    headers={'X-Container-Read': read_acl,
                             'X-Container-Write': write_acl},
                    container=container)


class ObjectClient(object):
    def __init__(self, container, name=None):
        self.container = container
        self.name = name or uuid4().hex
        self.rel_path = '%s/%s' % (self.container, self.name)
        self.body = None

    def create(self):
        src_body = ''.join([chr(i) * 1000
                            for i in range(ord('a'), ord('z'))])
        self.body = src_body.encode('utf8')
        return _upload_obj(self.container, self.name, self.body)

    @property
    def size(self):
        return len(self.body) if self.body else 0


class MPUClient(ObjectClient):
    def __init__(self, container, name=None, use_account=1, url_account=1):
        super(MPUClient, self).__init__(container, name)
        self.use_account = use_account
        self.url_account = url_account
        self.upload_id = None
        self.mpu_etag = None
        self.num_parts = 0
        # keys are part number i.e. indexed from 1
        self.part_etags = {}
        self.part_bodies = {}

    @property
    def size(self):
        return sum(len(body) for body in self.part_bodies.values())

    def create(self, extra_create_headers=None):
        resp = _create_mpu(
            self.container,
            self.name,
            use_account=self.use_account,
            url_account=self.url_account,
            extra_create_headers=extra_create_headers
        )
        if resp.status == 202:
            self.upload_id = resp.headers.get('X-Upload-Id')
        return resp

    def upload_part(self, part_num, part_body, headers=None,
                    use_account=None, url_account=None):
        resp = _upload_part(
            self.container,
            self.name,
            self.upload_id,
            part_num=part_num,
            part_body=part_body,
            headers=headers,
            use_account=use_account or self.use_account,
            url_account=url_account or self.url_account
        )
        self.part_etags[part_num] = resp.headers.get('Etag')
        self.part_bodies[part_num] = part_body
        return resp

    def upload_parts(self, num_parts, part_size, headers=None,
                     use_account=None, url_account=None):
        responses = []
        part_bodies = []
        for part_num in range(num_parts):
            part_body = chr(ord('a') + part_num) * part_size
            part_body = part_body.encode()
            resp = self.upload_part(
                part_num + 1,
                part_body,
                headers=headers,
                use_account=use_account or self.use_account,
                url_account=url_account or self.url_account
            )
            responses.append(resp)
            part_bodies.append(part_body)
        return responses, part_bodies

    def complete(self, etags, use_account=None, url_account=None):
        self.num_parts = len(etags)
        resp = _complete_mpu(
            self.container,
            self.name,
            self.upload_id,
            etags=etags,
            use_account=use_account or self.use_account,
            url_account=url_account or self.url_account
        )
        try:
            resp_dict = json.loads(resp.content)
            self.mpu_etag = normalize_etag(resp_dict['Etag'])
        except Exception:  # noqa
            pass
        return resp

    def abort(self, use_account=None, url_account=None):
        return _abort_mpu(
            self.container,
            self.name,
            self.upload_id,
            use_account=use_account or self.use_account,
            url_account=url_account or self.url_account
        )


class TestMPU(unittest.TestCase):
    def _assert_dict_equal_piecemeal(self, expected, actual):
        errors = []
        for k, v in expected.items():
            if k not in actual:
                errors.append('%s not found' % k)
                continue
            if actual[k] != v:
                errors.append('%s not equal, expected %s, actual %s'
                              % (k, v, actual[k]))

        for k, v in actual.items():
            if k not in expected:
                errors.append('Unexpected in actual: %s = %s' % (k, v))

        if errors:
            self.maxDiff = None
            self.fail(str(errors))

    @classmethod
    def _get_user_container(cls):
        if not cls.user_cont:
            cls.user_cont = cls._create_container()
        return cls.user_cont

    @classmethod
    def _get_singleton_src_obj(cls):
        # Get or create a single unique source object in user container.
        # Don't modify this object.
        if not cls.singleton_src:
            obj = ObjectClient(cls._get_user_container())
            resp = obj.create()
            if resp.status != 201:
                raise AssertionError('Failed to create src obj: %s'
                                     % resp.status)
            cls.singleton_src = obj
        return cls.singleton_src

    @classmethod
    def setUpClass(cls):
        cls.user_cont = None
        cls.singleton_mpu = cls.singleton_src = None
        cls.containers = set()

    @classmethod
    def tearDownClass(cls):
        for container in cls.containers:
            _post_acl(container)  # always clear acls
            resp = list_mpu_sessions(container)
            if resp.status != 200:
                continue
            sessions = json.loads(resp.content)
            for session in sessions:
                _abort_mpu(container, session['name'], session['upload_id'])

    @classmethod
    def _create_container(cls, name=None, headers=None, use_account=1):
        container = _create_container(
            name=name, headers=headers, use_account=use_account)
        cls.containers.add(container)
        return container

    def setUp(self):
        if tf.skip or tf.skip2:
            raise SkipTest
        mpu_info = tf.cluster_info.get('mpu')
        if not mpu_info:
            raise SkipTest("MPU not enabled")
        self.assertIn('min_part_size', mpu_info)
        self.part_size = mpu_info.get('min_part_size')
        self._get_user_container()
        self.containers_with_acl = set()
        self.mpu_name = uuid4().hex
        self.default_policy = _get_default_policy()

    def tearDown(self):
        for container in self.containers_with_acl:
            _post_acl(container)  # always clear acls

    def _skip_if_max_file_size_too_small(self, size):
        max_file_size = tf.cluster_info['swift']['max_file_size']
        if max_file_size < size:
            self.skipTest('max_file_size %s < required size %s'
                          % (max_file_size, size))

    def _post_acl(self, container=None, read_acl='', write_acl=''):
        container = container or self.user_cont
        resp = _post_acl(container, read_acl, write_acl)
        self.assertEqual(resp.status, 204)
        self.containers_with_acl.add(container)

    def _make_mpu(self, container, name, num_parts=1,
                  extra_create_headers=None):
        mpu = MPUClient(container, name)
        resp = mpu.create(extra_create_headers=extra_create_headers)
        self.assertEqual(202, resp.status)
        self.assertIsNotNone(mpu.upload_id)
        # upload parts
        responses, part_bodies = mpu.upload_parts(
            num_parts=num_parts, part_size=self.part_size)
        self.assertEqual([201] * num_parts,
                         [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]
        part_hashes = [
            md5(part_body, usedforsecurity=False).hexdigest().encode('ascii')
            for part_body in part_bodies]
        hasher = md5(usedforsecurity=False)
        for part_hash in part_hashes:
            hasher.update(binascii.a2b_hex(part_hash))
        expected_mpu_etag = '%s-%d' % (hasher.hexdigest(), num_parts)
        # complete
        resp = mpu.complete(etags)
        self.assertEqual(202, resp.status)
        self.assertEqual(expected_mpu_etag, mpu.mpu_etag)
        # verify
        resp = tf.retry(_make_request, method='GET',
                        container=container, obj=name)
        self.assertEqual(200, resp.status)
        self.assertEqual(str(mpu.num_parts),
                         resp.headers.get('X-Parts-Count'))
        self.assertEqual(str(mpu.size),
                         resp.headers.get('Content-Length'))
        return mpu, resp.content

    def _get_singleton_mpu(self):
        # Get or create a single unique mpu in a unique container.
        # Don't modify this mpu.
        cls = self.__class__
        if not cls.singleton_mpu:
            container = self._create_container()
            name = uuid4().hex
            cls.singleton_mpu, _body = self._make_mpu(
                container, name, num_parts=2)
        return cls.singleton_mpu

    def test_list_mpu_sessions(self):
        # do this is a unique container to make listing predictable
        container = self._create_container()
        created = []
        for obj in ('objy2', 'objx1', 'objy2'):
            resp = MPUClient(container, obj).create()
            self.assertEqual(202, resp.status, obj)
            created.append((obj, resp.headers.get('X-Upload-Id')))
        mpu = MPUClient(container, 'objx4')
        resp = mpu.create()
        self.assertEqual(202, resp.status)
        resp = mpu.abort()
        self.assertEqual(204, resp.status)

        resp = tf.retry(_make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json')
        self.assertEqual(200, resp.status)
        # expect ordered by (name, created time)
        expected = [created[1], created[0], created[2]]
        actual = [(item['name'], item['upload_id'])
                  for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # marker
        resp = tf.retry(_make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&marker=objy2')
        self.assertEqual(200, resp.status)
        expected = [created[0], created[2]]
        actual = [(item['name'], item['upload_id'])
                  for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # end_marker
        resp = tf.retry(_make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&end_marker=objy')
        self.assertEqual(200, resp.status)
        expected = [created[1]]
        actual = [(item['name'], item['upload_id'])
                  for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # prefix
        resp = tf.retry(_make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&prefix=objx')
        self.assertEqual(200, resp.status)
        expected = [created[1]]
        actual = [(item['name'], item['upload_id'])
                  for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

        # limit
        resp = tf.retry(_make_request, method='GET',
                        container=container,
                        query_string='uploads&format=json&limit=2')
        self.assertEqual(200, resp.status)
        expected = [created[1], created[0]]
        actual = [(item['name'], item['upload_id'])
                  for item in json.loads(resp.content)]
        self.assertEqual(expected, actual)

    def test_create_upload_complete_read_mpu(self):
        if not self.default_policy:
            self.skipTest('default policy is not known')
        # create upload
        user_headers = {
            'Content-Disposition': 'attachment',
            'Content-Encoding': 'none',
            'Content-Language': 'en-US',
            'Cache-Control': 'no-cache',
            'Expires': 'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        mpu = MPUClient(self.user_cont, self.mpu_name)
        resp = mpu.create(extra_create_headers=user_headers)
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
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(404, resp.status)

        # upload parts
        part_resp_etags = []
        part_bodies = [b'a' * self.part_size, b'b' * self.part_size]
        part_etags = [md5(part_body, usedforsecurity=False).hexdigest()
                      for part_body in part_bodies]
        resp = mpu.upload_part(1, part_bodies[0])
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
        resp = mpu.upload_part(2, part_bodies[1])
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
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name,
                        query_string='upload-id=%s' % upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/json; charset=utf-8',
             'Content-Length': str(len(resp.content)),
             'X-Storage-Policy': self.default_policy,
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
        resp = mpu.complete(part_resp_etags)
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
        resp = tf.retry(_make_request, method='GET',
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
            'Content-Length': str(2 * self.part_size),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
        }
        exp_headers.update(user_headers)
        self.assertEqual(exp_headers, resp.headers)
        self.assertEqual(2 * self.part_size, len(resp.content))
        self.assertEqual(part_bodies[0][-1], resp.content[self.part_size - 1])
        self.assertEqual(part_bodies[1][0], resp.content[self.part_size])

    def test_create_upload_complete_mpu_retry_complete(self):
        # create upload
        resp = _create_mpu(self.user_cont, self.mpu_name)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

        # upload 3 parts
        part_resp_etags = []
        for i, x in enumerate([b'a', b'b', b'c']):
            part_body = x * self.part_size
            resp = _upload_part(
                self.user_cont, self.mpu_name, upload_id, i + 1, part_body)
            self.assertEqual(201, resp.status)
            part_resp_etags.append(resp.getheader('Etag'))

        etag_hasher = MPUEtagHasher()
        for part_etag in part_resp_etags[:2]:
            etag_hasher.update(part_etag)
            exp_mpu_etag = etag_hasher.etag

        # complete mpu using only 2 parts
        resp = _complete_mpu(
            self.user_cont, self.mpu_name, upload_id, part_resp_etags[:2])
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'), body)
        self.assertEqual(exp_mpu_etag, body.get('Etag'), body)
        self.assertEqual([], body['Errors'], body)

        # retry complete mpu with same manifest
        resp = _complete_mpu(
            self.user_cont, self.mpu_name, upload_id, part_resp_etags[:2])
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'),
                         '%s %s' % (resp.headers['x-trans-id'], body))
        self.assertEqual(exp_mpu_etag, body.get('Etag'))
        self.assertEqual([], body['Errors'], body)

        # retry complete mpu with different manifest
        resp = _complete_mpu(
            self.user_cont, self.mpu_name, upload_id, part_resp_etags[1:])
        self.assertEqual(404, resp.status, resp.content)

        # retry complete on a different object path
        resp = _complete_mpu(
            self.user_cont, self.mpu_name + '-not', upload_id,
            part_resp_etags[:2])
        self.assertEqual(400, resp.status)
        self.assertIn(b'Invalid upload-id', resp.content)

        # GET the user object - check
        resp = tf.retry(_make_request, method='GET',
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
             'Content-Length': str(2 * self.part_size),
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        self.assertEqual(2 * self.part_size, len(resp.content))

    def test_make_mpu_auto_detect_content_type(self):
        # verify that content-type is detected and set on the manifest
        mpu_name = self.mpu_name + '.html'
        mpu, content = self._make_mpu(
            self.user_cont, mpu_name,
            extra_create_headers={'content-type': None})

        # GET the user object
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual('text/html', resp.getheader('Content-Type'))

        # GET the user container
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont,
                        query_string='format=json')
        self.assertEqual(200, resp.status)
        listing = json.loads(resp.content)
        expected = {
            'name': mpu_name,
            'hash': '%s' % mpu.mpu_etag,
            'bytes': mpu.size,
            'content_type': 'text/html',
            'last_modified': mock.ANY,
        }
        actual = [item for item in listing if item['name'] == mpu_name]
        self.assertEqual([expected], actual)

    def test_create_mpu_via_container_acl(self):
        # other account cannot create mpu without acl
        resp = _create_mpu(self.user_cont, self.mpu_name,
                           use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot create mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = _create_mpu(self.user_cont, self.mpu_name,
                           use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can create mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = _create_mpu(self.user_cont, self.mpu_name,
                           use_account=2, url_account=1)
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)

    def test_create_upload_abort_retry_abort(self):
        # create upload
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # upload part
        part_body = b'a' * self.part_size
        resp = mpu.upload_part(1, part_body)
        self.assertEqual(201, resp.status)

        # abort
        resp = mpu.abort()
        self.assertEqual(204, resp.status)

        # user object was not created
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(404, resp.status)

        # retry abort
        resp = mpu.abort()
        self.assertEqual(204, resp.status)

        # retry abort on a different object path
        resp = _abort_mpu(mpu.container, mpu.name + '-not', mpu.upload_id)
        self.assertEqual(400, resp.status)
        self.assertIn(b'Invalid upload-id', resp.content)

    def test_upload_part_bad_etag(self):
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)
        part_body = b'a' * self.part_size
        headers = {'Etag': 'you must be mistaken'}
        resp = mpu.upload_part(1, part_body, headers=headers)
        self.assertEqual(422, resp.status)

    def test_upload_part_after_failed_complete(self):
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        part_body = b'a' * self.part_size
        resp = mpu.upload_part(1, part_body)
        self.assertEqual(201, resp.status)
        part_resp_etag_1 = resp.getheader('Etag')

        # try to complete mpu using 2 parts but second part doesn't exist
        resp = mpu.complete([part_resp_etag_1] * 2)
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('400 Bad Request', body.get('Response Status'), body)
        self.assertEqual([["000002", "404 Not Found"]], body['Errors'], body)

        # upload second part
        part_body = b'b' * self.part_size
        resp = mpu.upload_part(2, part_body)
        self.assertEqual(201, resp.status)
        part_resp_etag_2 = resp.getheader('Etag')

        etag_hasher = MPUEtagHasher()
        for part_etag in (part_resp_etag_1, part_resp_etag_2):
            etag_hasher.update(part_etag)
            exp_mpu_etag = etag_hasher.etag

        # try to complete mpu again now second part exists
        resp = mpu.complete([part_resp_etag_1, part_resp_etag_2])
        self.assertEqual(202, resp.status, resp.content)
        body = json.loads(resp.content)
        self.assertEqual('201 Created', body.get('Response Status'), body)
        self.assertEqual(exp_mpu_etag, body.get('Etag'), body)
        self.assertEqual([], body['Errors'], body)

    def test_upload_part_after_abort(self):
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)
        resp = mpu.abort()
        self.assertEqual(204, resp.status)

        part_body = b'a' * self.part_size
        resp = mpu.upload_part(1, part_body)
        self.assertEqual(404, resp.status)
        self.assertEqual(b'No such upload-id', resp.content)

    def test_upload_part_copy_using_PUT(self):
        src = self._get_singleton_src_obj()
        # create mpu session
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # copy to part from source object
        headers = {'x-copy-from': src.rel_path}
        resp = mpu.upload_part(1, b'', headers=headers)
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(src.rel_path, resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = mpu.complete([part_etag])
        self.assertEqual(202, resp.status)

        # verify mpu content
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(src.size, int(resp.headers.get('Content-Length')))

    def test_upload_part_copy_using_COPY(self):
        src = self._get_singleton_src_obj()
        # create mpu session
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()

        # copy to part from source object
        resp = tf.retry(
            _make_request,
            method='COPY',
            container=src.container,
            obj=src.name,
            query_string='upload-id=%s&part-number=%d' % (mpu.upload_id, 1),
            headers={'destination': mpu.rel_path}
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(src.rel_path, resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = mpu.complete([part_etag])
        self.assertEqual(202, resp.status)

        # verify mpu content
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(src.size, int(resp.headers.get('Content-Length')))

    def test_upload_part_copy_with_range_using_PUT(self):
        # upload source object
        src = self._get_singleton_src_obj()
        # create mpu session
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # copy to part from source object
        headers = {'x-copy-from': src.rel_path, 'range': 'bytes=501-1500'}
        resp = mpu.upload_part(1, b'', headers=headers)
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(src.rel_path, resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = mpu.complete([part_etag])
        self.assertEqual(202, resp.status)

        # verify mpu content
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1000, int(resp.headers.get('Content-Length')))
        self.assertEqual(src.body[501:1501], resp.content)

    def test_upload_part_copy_with_range_using_COPY(self):
        src = self._get_singleton_src_obj()
        # create mpu session
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # copy to part from source object
        resp = tf.retry(
            _make_request,
            method='COPY',
            container=src.container,
            obj=src.name,
            query_string='upload-id=%s&part-number=%d' % (mpu.upload_id, 1),
            headers={'destination': mpu.rel_path, 'range': 'bytes=501-1500'}
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(src.rel_path, resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = mpu.complete([part_etag])
        self.assertEqual(202, resp.status)

        # verify mpu content
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1000, int(resp.headers.get('Content-Length')))
        self.assertEqual(src.body[501:1501], resp.content)

    def test_upload_part_copy_from_other_mpu_using_PUT(self):
        src = self._get_singleton_mpu()
        self._skip_if_max_file_size_too_small(src.size)
        # create mpu session
        # note: the part-number applies to the target mpu session, not the
        # source object
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # copy new part from source object
        # note: the part-number applies to the target mpu session, not the
        # source object
        resp = mpu.upload_part(part_num=1, part_body=b'',
                               headers={'x-copy-from': src.rel_path})
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(src.rel_path, resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = mpu.complete([part_etag])
        self.assertEqual(202, resp.status)

        # verify new mpu content - one part copied from entire other mpu
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1, int(resp.headers.get('x-parts-count')))
        self.assertEqual(src.size, int(resp.headers.get('Content-Length')))

    def test_upload_part_copy_from_other_mpu_using_COPY(self):
        # upload source mpu object
        src = self._get_singleton_mpu()
        self._skip_if_max_file_size_too_small(src.size)
        # create mpu session
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # copy new part from source object
        # note: the part-number applies to the target mpu session, not the
        # source object
        resp = tf.retry(
            _make_request,
            method='COPY',
            container=src.container,
            obj=src.name,
            query_string='upload-id=%s&part-number=%d' % (mpu.upload_id, 1),
            headers={'destination': mpu.rel_path}
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(src.rel_path, resp.headers.get('X-Copied-From'))
        part_etag = resp.headers['Etag']

        # complete session
        resp = mpu.complete([part_etag])
        self.assertEqual(202, resp.status)

        # verify mpu content - one part copied from entire other mpu
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual(1, int(resp.headers.get('x-parts-count')))
        self.assertEqual(
            2 * self.part_size, int(resp.headers.get('Content-Length')))

    def test_upload_part_via_container_acl(self):
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)

        # other account cannot upload part without acl
        responses, _ = mpu.upload_parts(num_parts=1, part_size=self.part_size,
                                        use_account=2, url_account=1)
        self.assertEqual([403], [resp.status for resp in responses])

        # other account cannot upload part with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses, _ = mpu.upload_parts(num_parts=1, part_size=self.part_size,
                                        use_account=2, url_account=1)
        self.assertEqual([403], [resp.status for resp in responses])

        # other account can upload part with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        responses, _ = mpu.upload_parts(num_parts=1, part_size=self.part_size,
                                        use_account=2, url_account=1)
        self.assertEqual([201], [resp.status for resp in responses])

    def test_list_parts(self):
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)
        part_etags = []
        for i, part_body in enumerate([b'a' * self.part_size,
                                       b'b' * self.part_size]):
            resp = mpu.upload_part(i + 1, part_body)
            self.assertEqual(201, resp.status)
            part_etags.append(normalize_etag(resp.headers.get('Etag')))

        expected = [{'name': '%s/%s/%06d'
                             % (self.mpu_name, mpu.upload_id, i + 1),
                     'hash': part_etag,
                     'bytes': self.part_size,
                     'content_type': 'application/octet-stream',
                     'last_modified': mock.ANY}
                    for i, part_etag in enumerate(part_etags)]

        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name,
                        query_string='upload-id=%s' % mpu.upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(expected, json.loads(resp.content))

        # with limit
        resp = tf.retry(
            _make_request, method='GET',
            container=self.user_cont, obj=self.mpu_name,
            query_string='upload-id=%s&limit=1' % mpu.upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(expected[:1], json.loads(resp.content))

        # with part-number-marker
        resp = tf.retry(
            _make_request, method='GET',
            container=self.user_cont, obj=self.mpu_name,
            query_string='upload-id=%s&part-number-marker=1' % mpu.upload_id)
        self.assertEqual(200, resp.status)
        self.assertEqual(expected[1:], json.loads(resp.content))

    def test_complete_mpu_via_container_acl(self):
        # create an in-progress mpu
        mpu = MPUClient(self.user_cont, self.mpu_name)
        mpu.create()
        self.assertIsNotNone(mpu.upload_id)
        responses, _ = mpu.upload_parts(num_parts=1, part_size=self.part_size)
        self.assertEqual([201], [resp.status for resp in responses])
        etags = [resp.headers['Etag'] for resp in responses]

        # other account cannot complete mpu without acl
        resp = mpu.complete(etags, use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot complete mpu with read acl
        self._post_acl(read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = mpu.complete(etags, use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can complete mpu with write acl
        self._post_acl(write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = mpu.complete(etags, use_account=2, url_account=1)
        self.assertEqual(202, resp.status)

        # sanity check - creating account can read completed mpu
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)

    def test_post_mpu(self):
        create_headers = {'content-type': 'application/test1',
                          'x-object-meta-test': 'one'}
        mpu, _body = self._make_mpu(
            self.user_cont, self.mpu_name, extra_create_headers=create_headers)
        resp = tf.retry(_make_request, method='HEAD',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self.assertEqual({
            'Content-Type': 'application/test1',
            'Etag': '"%s"' % mpu.mpu_etag,
            'X-Upload-Id': mpu.upload_id,
            'X-Parts-Count': str(mpu.num_parts),
            'Last-Modified': mock.ANY,
            'X-Timestamp': mock.ANY,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(mpu.size),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
            'X-Object-Meta-Test': 'one',
        }, resp.headers)

        # update content-type and metadata
        post_headers = {'content-type': 'application/test2',
                        'x-object-meta-test-2': 'two'}
        resp = tf.retry(_make_request, method='POST',
                        container=self.user_cont, obj=self.mpu_name,
                        headers=post_headers)
        self.assertEqual(202, resp.status)
        resp = tf.retry(_make_request, method='HEAD',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self._assert_dict_equal_piecemeal({
            'Content-Type': 'application/test2',
            'Etag': '"%s"' % mpu.mpu_etag,
            'X-Upload-Id': mpu.upload_id,
            'X-Parts-Count': str(mpu.num_parts),
            'Last-Modified': mock.ANY,
            'X-Timestamp': mock.ANY,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(mpu.size),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
            'X-Object-Meta-Test-2': 'two',
        }, resp.headers)

        # update only metadata
        post_headers = {'x-object-meta-test-3': 'three'}
        resp = tf.retry(_make_request, method='POST',
                        container=self.user_cont, obj=self.mpu_name,
                        headers=post_headers)
        self.assertEqual(202, resp.status)
        resp = tf.retry(_make_request, method='HEAD',
                        container=self.user_cont, obj=self.mpu_name)
        self.assertEqual(200, resp.status)
        self._assert_dict_equal_piecemeal({
            'Content-Type': 'application/test2',
            'Etag': '"%s"' % mpu.mpu_etag,
            'X-Upload-Id': mpu.upload_id,
            'X-Parts-Count': str(mpu.num_parts),
            'Last-Modified': mock.ANY,
            'X-Timestamp': mock.ANY,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(mpu.size),
            'X-Trans-Id': mock.ANY,
            'X-Openstack-Request-Id': mock.ANY,
            'Date': mock.ANY,
            'X-Object-Meta-Test-3': 'three',
        }, resp.headers)

    def test_container_head_with_mpu(self):
        mpu = self._get_singleton_mpu()
        resp = tf.retry(_make_request, method='HEAD',
                        container=mpu.container,
                        query_string='format=json')
        self.assertEqual(204, resp.status)
        self.assertEqual(b'', resp.content)
        self.assertEqual('1', resp.headers['X-Container-Object-Count'])
        self.assertEqual(str(mpu.size),
                         resp.headers['X-Container-Bytes-Used'])

    def test_container_get_with_mpu(self):
        mpu = self._get_singleton_mpu()
        # GET the user container
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container,
                        query_string='format=json')
        self.assertEqual(200, resp.status)
        self.assertEqual('1', resp.headers['X-Container-Object-Count'])
        self.assertEqual(str(2 * self.part_size),
                         resp.headers['X-Container-Bytes-Used'])
        listing = json.loads(resp.content)
        self.assertEqual(1, len(listing))
        expected = {
            'name': mpu.name,
            'hash': mpu.mpu_etag,
            'bytes': mpu.size,
            'content_type': 'application/test',
            'last_modified': mock.ANY,
        }
        self.assertEqual(expected, listing[0])

    def test_container_get_with_mpu_via_container_acl(self):
        mpu = self._get_singleton_mpu()
        # other account cannot get listing without acl
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot get listing with write acl
        self._post_acl(container=mpu.container,
                       write_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can get listing with read acl
        self._post_acl(container=mpu.container,
                       read_acl=tf.swift_test_perm[1])  # acl for account '2'
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container,
                        query_string='format=json',
                        use_account=2, url_account=1)
        self.assertEqual(200, resp.status)
        self.assertEqual(200, resp.status)
        self.assertEqual('1', resp.headers['X-Container-Object-Count'])
        self.assertEqual(str(mpu.size),
                         resp.headers['X-Container-Bytes-Used'])
        listing = json.loads(resp.content)
        self.assertEqual(1, len(listing))
        expected = {
            'name': mpu.name,
            'hash': mpu.mpu_etag,
            'bytes': mpu.size,
            'content_type': 'application/test',
            'last_modified': mock.ANY,
        }
        self.assertEqual(expected, listing[0])

    def test_read_mpu_via_container_acl(self):
        mpu = self._get_singleton_mpu()
        # same account can read mpu without acl
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container, obj=mpu.name)
        self.assertEqual(200, resp.status)

        # other account cannot read mpu without acl
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container, obj=mpu.name,
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account cannot read mpu with write acl
        # acl for account '2'
        self._post_acl(container=mpu.container,
                       write_acl=tf.swift_test_perm[1])
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container, obj=mpu.name,
                        use_account=2, url_account=1)
        self.assertEqual(403, resp.status)

        # other account can read mpu with read acl
        # acl for account '2'
        self._post_acl(container=mpu.container,
                       read_acl=tf.swift_test_perm[1])
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container, obj=mpu.name,
                        use_account=2, url_account=1)
        self.assertEqual(200, resp.status)

    def _do_test_get_head_mpu(self, method, conditional=False, headers=None):
        mpu = self._get_singleton_mpu()
        headers = headers or {}
        if conditional:
            headers['If-Match'] = mpu.mpu_etag
        resp = tf.retry(_make_request, method=method,
                        container=mpu.container, obj=mpu.name,
                        headers=headers)
        self.assertEqual(200, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu.mpu_etag,
             'X-Upload-Id': mpu.upload_id,
             'X-Parts-Count': str(mpu.num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': str(mpu.size),
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        return resp

    def test_get_mpu(self):
        resp = self._do_test_get_head_mpu('GET')
        self.assertEqual(self.singleton_mpu.size, len(resp.content))

    def test_head_mpu(self):
        resp = self._do_test_get_head_mpu('HEAD')
        self.assertEqual(b'', resp.content)

    def test_get_mpu_if_match(self):
        resp = self._do_test_get_head_mpu('GET', conditional=True)
        self.assertEqual(self.singleton_mpu.size, len(resp.content))

    def test_head_mpu_if_match(self):
        resp = self._do_test_get_head_mpu('HEAD', conditional=True)
        self.assertEqual(b'', resp.content)

    def test_get_mpu_if_none_match_does_not_match(self):
        resp = self._do_test_get_head_mpu('GET',
                                          headers={'If-None-Match': 'foo'})
        self.assertEqual(self.singleton_mpu.size, len(resp.content))

    def test_head_mpu_if_none_match_does_not_match(self):
        resp = self._do_test_get_head_mpu('HEAD',
                                          headers={'If-None-Match': 'foo'})
        self.assertEqual(b'', resp.content)

    def _do_test_get_head_mpu_if_match_does_not_match(self, method):
        mpu = self._get_singleton_mpu()
        headers = {'If-Match': 'foo'}
        resp = tf.retry(_make_request, method=method,
                        container=mpu.container, obj=mpu.name,
                        headers=headers)
        self.assertEqual(412, resp.status)
        self.assertEqual(b'', resp.content)
        self.assertEqual(
            # TODO: seems odd that content-type is not what was set
            {'Content-Type': 'text/html; charset=UTF-8',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu.mpu_etag,
             'X-Upload-Id': mpu.upload_id,
             'X-Parts-Count': str(mpu.num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )

    def test_get_mpu_if_match_does_not_match_412(self):
        self._do_test_get_head_mpu_if_match_does_not_match('GET')

    def test_head_mpu_if_match_does_not_match_412(self):
        self._do_test_get_head_mpu_if_match_does_not_match('HEAD')

    def _do_test_get_head_mpu_if_none_match_does_match(self, method):
        mpu = self._get_singleton_mpu()
        headers = {'If-None-Match': mpu.mpu_etag}
        resp = tf.retry(_make_request, method=method,
                        container=mpu.container, obj=mpu.name,
                        headers=headers)
        self.assertEqual(304, resp.status)
        self.assertEqual(b'', resp.content)
        self.assertEqual(
            {'Content-Type': mock.ANY,  # TODO: EC GET returns text/html !?
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu.mpu_etag,
             'X-Upload-Id': mpu.upload_id,
             'X-Parts-Count': str(mpu.num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': '0',
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers,
        )

    def test_get_mpu_if_none_match_does_match_304(self):
        self._do_test_get_head_mpu_if_none_match_does_match('GET')

    def test_head_mpu_if_none_match_does_match_304(self):
        self._do_test_get_head_mpu_if_none_match_does_match('HEAD')

    def test_get_mpu_with_part_number(self):
        mpu = self._get_singleton_mpu()
        resp = tf.retry(_make_request, method='GET',
                        container=mpu.container, obj=mpu.name,
                        query_string='part-number=1')
        self.assertEqual(206, resp.status)
        self.assertEqual(
            {'Content-Type': 'application/test',
             # NB: part etags are always quoted by MPU middleware
             'Etag': '"%s"' % mpu.mpu_etag,
             'X-Upload-Id': mpu.upload_id,
             'X-Parts-Count': str(mpu.num_parts),
             'Last-Modified': mock.ANY,
             'X-Timestamp': mock.ANY,
             'Accept-Ranges': 'bytes',
             'Content-Length': str(self.part_size),
             'Content-Range': 'bytes 0-%s/%s'
                              % (self.part_size - 1, 2 * self.part_size),
             'X-Trans-Id': mock.ANY,
             'X-Openstack-Request-Id': mock.ANY,
             'Date': mock.ANY},
            resp.headers
        )
        self.assertEqual(self.part_size, len(resp.content))

    def test_copy_mpu(self):
        src = self._get_singleton_mpu()
        self._skip_if_max_file_size_too_small(src.size)
        copy_name = uuid4().hex
        resp = tf.retry(
            _make_request,
            method='COPY',
            container=src.container,
            obj=src.name,
            headers={
                'destination': '/'.join([self.user_cont, copy_name]),
            },
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(
            wsgi_quote(str_to_wsgi('%s/%s' % (src.container, src.name))),
            resp.headers.get('X-Copied-From'))

        # GET the copy
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=copy_name)
        self.assertEqual(200, resp.status)
        self.assertNotIn('X-Parts-Count', resp.headers)
        # verify copy content
        self.assertEqual(
            src.size, int(resp.headers.get('Content-Length')))
        self.assertEqual(src.part_bodies[1][0], resp.content[1])
        self.assertEqual(src.part_bodies[2][-1], resp.content[-1])

    def test_copy_mpu_with_part_number(self):
        mpu = self._get_singleton_mpu()
        copy_name = uuid4().hex
        resp = tf.retry(
            _make_request,
            method='COPY',
            container=mpu.container,
            obj=mpu.name,
            headers={
                'destination': '/'.join([self.user_cont, copy_name]),
            },
            query_string='part-number=1',
        )
        self.assertEqual(201, resp.status, resp.content)
        self.assertEqual(
            wsgi_quote(str_to_wsgi('%s/%s' % (mpu.container, mpu.name))),
            resp.headers.get('X-Copied-From'))

        # GET the copy
        resp = tf.retry(_make_request, method='GET',
                        container=self.user_cont, obj=copy_name)
        self.assertNotIn('X-Parts-Count', resp.headers)
        self.assertEqual(200, resp.status)
        # verify copy content
        self.assertEqual(
            self.part_size, int(resp.headers.get('Content-Length')))
        self.assertEqual(mpu.part_bodies[1][0], resp.content[1])
        self.assertEqual(mpu.part_bodies[1][-1], resp.content[-1])


class TestMPUUTF8(TestMPU):

    def setUp(self):
        super(TestMPUUTF8, self).setUp()
        self.mpu_name = uuid4().hex + u'\N{SNOWMAN}'
