# Copyright (c) 2023 Nvidia
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
import collections
import json
import unittest
import uuid
from tempfile import mkdtemp
import os.path
import shutil
import random

from swift.common.manager import Manager
from swift.common.swob import normalize_etag
from swift.common.utils import quote, md5, MD5_OF_EMPTY_STRING
from swift.container.auditor import ContainerAuditor
from swiftclient import client as swiftclient, ClientException

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES

from boto3.s3.transfer import TransferConfig
from test.s3api import get_s3_client

import mock


def status_from_response(resp):
    return resp['ResponseMetadata']['HTTPStatusCode']


def calc_slo_etag(chunk_etags):
    slo_etag_hasher = md5(usedforsecurity=False)
    for chunk_etag in chunk_etags:
        slo_etag_hasher.update(chunk_etag.encode())
    return slo_etag_hasher.hexdigest()


def calc_s3mpu_etag(chunk_etags):
    etag_hasher = md5(usedforsecurity=False)
    for chunk_etag in chunk_etags:
        etag_hasher.update(binascii.a2b_hex(normalize_etag(chunk_etag)))
    return etag_hasher.hexdigest() + '-%d' % len(chunk_etags)


class BaseTestMPU(ReplProbeTest):
    def setUp(self):
        self.tempdir = mkdtemp()
        super(BaseTestMPU, self).setUp()
        self.bucket_name = 'bucket-%s' % uuid.uuid4()
        self.mpu_name = 'mpu-%s' % uuid.uuid4()

    def make_file(self, chunksize, num_chunks):
        filename = os.path.join(self.tempdir, 'big.file')
        md5_hasher = md5(usedforsecurity=False)
        chunk_etags = []
        with open(filename, 'wb') as f:
            c = 'a'
            for i in range(num_chunks):
                c = chr(ord(c) + i)
                chunk = c.encode() * chunksize
                f.write(chunk)
                md5_hasher.update(chunk)
                chunk_etags.append(
                    md5(chunk, usedforsecurity=False).hexdigest())
        return filename, md5_hasher.hexdigest(), chunk_etags

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(BaseTestMPU, self).tearDown()


class BaseTestS3MPU(BaseTestMPU):
    def setUp(self):
        super(BaseTestS3MPU, self).setUp()
        s3api_info = self.cluster_info.get('s3api', {})
        if not s3api_info:
            raise unittest.SkipTest('s3api not enabled')

        self.s3 = get_s3_client(1)

        self.segment_bucket_name = self.bucket_name + '+segments'
        self.bucket_brain = BrainSplitter(self.url, self.token,
                                          self.bucket_name)
        self.segments_brain = BrainSplitter(self.url, self.token,
                                            self.segment_bucket_name)
        self.maxDiff = None


class TestS3MPU(BaseTestS3MPU):

    @unittest.skipIf(len(ENABLED_POLICIES) < 2, "Need more than one policy")
    def setUp(self):
        super(TestS3MPU, self).setUp()
        self.other_policy = random.choice([p for p in ENABLED_POLICIES
                                           if p != self.policy])
        # I think boto has a minimum chunksize that matches AWS, when I do this
        # too small I get less chunks in the SLO than I expect
        self.chunksize = 5 * 2 ** 20
        self.transfer_config = TransferConfig(
            multipart_threshold=self.chunksize,
            multipart_chunksize=self.chunksize)

    def _assert_container_storage_policy(self, container_name,
                                         expected_policy):
        headers = swiftclient.head_container(self.url, self.token,
                                             container_name)
        self.assertEqual(headers['x-storage-policy'], expected_policy.name)

    def test_mixed_policy_upload(self):
        # TODO: this isn't testing mixed policy because the segments bucket is
        #   no longer relevant to native mpu and native mpu doesn't *yet*
        #   support mixed policy parts bucket
        self.skipTest('Needs updating for native mpu')
        # Old swift had a cross policy contamination bug
        # (https://bugs.launchpad.net/swift/+bug/2038459) that created
        # the SLO manifest with the wrong x-backend-storage-policy-index:
        # during the CompleteMultipartUpload it read the upload-id-marker from
        # +segments, and applied that policy index to the manifest PUT, so the
        # manifest object was stored in the wrong policy and requests for it
        # would 404.
        self.s3.create_bucket(Bucket=self.bucket_name)
        self._assert_container_storage_policy(self.bucket_name, self.policy)
        # create segments container in another policy
        self.segments_brain.put_container(policy_index=int(self.other_policy))
        self._assert_container_storage_policy(self.segment_bucket_name,
                                              self.other_policy)
        num_chunks = 3
        data_filename, md5_hash, chunk_etags = self.make_file(self.chunksize,
                                                              num_chunks)
        expected_size = self.chunksize * num_chunks
        exp_etag = calc_s3mpu_etag(chunk_etags)

        self.s3.upload_file(data_filename, self.bucket_name, self.mpu_name,
                            Config=self.transfer_config)

        # s3 mpu request succeeds
        s3_get_resp = self.s3.get_object(Bucket=self.bucket_name,
                                         Key=self.mpu_name)
        self.assertEqual(expected_size, int(s3_get_resp['ContentLength']))
        self.assertEqual('"%s"' % exp_etag, s3_get_resp['ETag'])
        hasher = md5(usedforsecurity=False)
        for chunk in s3_get_resp['Body']:
            hasher.update(chunk)
        self.assertEqual(md5_hash, hasher.hexdigest())

        # swift response is the same
        swift_obj_headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            resp_chunk_size=65536)
        self.assertEqual(expected_size,
                         int(swift_obj_headers['content-length']))
        self.assertEqual('"%s"' % exp_etag, swift_obj_headers['etag'])
        hasher = md5(usedforsecurity=False)
        for chunk in body:
            hasher.update(chunk)
        self.assertEqual(md5_hash, hasher.hexdigest())

        # s3 listing has correct bytes
        resp = self.s3.list_objects(Bucket=self.bucket_name)
        # note: with PY2 the args order (expected, actual) is significant for
        # mock.ANY == datetime(...) to be true
        self.assertEqual([{
            u'ETag': '"%s"' % exp_etag,
            u'Key': self.mpu_name,
            u'LastModified': mock.ANY,
            u'Size': expected_size,
            u'Owner': {u'DisplayName': 'test:tester', u'ID': 'test:tester'},
            u'StorageClass': 'STANDARD',
        }], resp['Contents'])

        # swift listing is the same
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name)
        self.assertEqual(stat['x-storage-policy'], self.policy.name)
        self.assertEqual(listing, [{
            'bytes': expected_size,
            'content_type': 'application/octet-stream',
            'hash': '%s' % exp_etag,
            'last_modified': mock.ANY,
            'name': self.mpu_name,
        }])

    def _create_slo_mpu(self, num_parts):
        self.s3.create_bucket(Bucket=self.bucket_name)
        data_filename, md5_hash, chunk_etags = self.make_file(self.chunksize,
                                                              num_parts)
        expected_size = self.chunksize * num_parts

        # upload an mpu
        self.s3.upload_file(data_filename, self.bucket_name, self.mpu_name,
                            Config=self.transfer_config)
        # check the mpu
        s3_head_resp = self.s3.head_object(Bucket=self.bucket_name,
                                           Key=self.mpu_name)
        self.assertEqual(expected_size, int(s3_head_resp['ContentLength']))
        self.assertEqual(num_parts, int(
            s3_head_resp['ETag'].strip('"').rsplit('-')[-1]))

        # check the parts are in the segments bucket
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.segment_bucket_name)
        part_names = [item['name'] for item in listing]
        split_part_names = [name.split('/') for name in part_names]
        # we don't know the upload ID...
        self.assertEqual(
            [[self.mpu_name, mock.ANY, str(i + 1)] for i in range(num_parts)],
            split_part_names)
        # ...but we can assert it is same for all parts
        self.assertEqual(1, len(set([s[1] for s in split_part_names])))

    def test_mpu_overwrite_async_cleanup(self):
        # TODO: fixme
        self.skipTest('Needs updating for native mpu')
        num_parts = 2
        self._create_slo_mpu(num_parts)
        # overwrite the mpu with a tiny file
        data_filename, md5_hash, chunk_etags = self.make_file(1, 1)
        self.s3.upload_file(data_filename, self.bucket_name,
                            self.mpu_name, Config=self.transfer_config)
        # check the tiny file
        s3_head_resp = self.s3.head_object(Bucket=self.bucket_name,
                                           Key=self.mpu_name)
        self.assertEqual(1, int(s3_head_resp['ContentLength']))
        self.assertNotIn('-', s3_head_resp['ETag'])

        # the parts still exist in the segments bucket, plus a marker
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.segment_bucket_name)
        part_names = [item['name'] for item in listing]
        split_part_names = [name.split('/') for name in part_names]
        self.assertEqual(
            [[self.mpu_name, mock.ANY, str(i + 1)] for i in range(num_parts)]
            + [[self.mpu_name, mock.ANY, 'marker-deleted']],
            split_part_names)
        self.assertEqual(1, len(set([s[1] for s in split_part_names])))

        # run the auditor
        Manager(['container-auditor']).once()

        # parts have been deleted
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.segment_bucket_name)
        part_names = [item['name'] for item in listing]
        self.assertFalse(part_names, part_names)

    def test_mpu_delete_async_cleanup(self):
        # TODO: fixme
        self.skipTest('Needs updating for native mpu')
        num_parts = 2
        self._create_slo_mpu(num_parts)
        # delete the mpu
        self.s3.delete_object(Bucket=self.bucket_name, Key=self.mpu_name)

        # the parts still exist in the segments bucket, plus a marker
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.segment_bucket_name)
        part_names = [item['name'] for item in listing]
        split_part_names = [name.split('/') for name in part_names]
        self.assertEqual(
            [[self.mpu_name, mock.ANY, str(i + 1)] for i in range(num_parts)]
            + [[self.mpu_name, mock.ANY, 'marker-deleted']],
            split_part_names)
        self.assertEqual(1, len(set([s[1] for s in split_part_names])))

        # run the auditor
        Manager(['container-auditor']).once()

        # parts have been deleted
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.segment_bucket_name)
        part_names = [item['name'] for item in listing]
        self.assertFalse(part_names, part_names)


class BaseTestNativeMPU(BaseTestMPU):
    def setUp(self):
        super(BaseTestNativeMPU, self).setUp()
        mpu_info = self.cluster_info.get('mpu', {})
        if not mpu_info:
            raise unittest.SkipTest('mpu not enabled')

        self.internal_client = self.make_internal_client()
        # TODO: revert to superclass uuid name
        self.mpu_name = 'tempname'

    def post_object(self, container, obj, headers=None, query_string='',
                    body=b''):
        # swiftclient post_object doesn't accept query_string and quotes it if
        # included with obj name :(
        parsed, conn = swiftclient.http_connection(self.url)
        path = '%s/%s/%s' % (parsed.path, quote(container), quote(obj))
        if query_string:
            path = '?'.join([path, query_string.lstrip('?')])
        req_headers = {'X-Auth-Token': self.token}
        if headers:
            req_headers.update(headers)
        conn.request('POST', path, headers=req_headers, data=body)
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        return resp, body

    def get_container(self, container, headers=None, query_string=''):
        parsed, conn = swiftclient.http_connection(self.url)
        path = '%s/%s' % (parsed.path, quote(container))
        if query_string:
            path = '?'.join([path, query_string.lstrip('?')])
        req_headers = {'X-Auth-Token': self.token}
        if headers:
            req_headers.update(headers)
        conn.request('GET', path, headers=req_headers)
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        return resp, body

    def get_object_versions_from_listing(self, container):
        resp, body = self.get_container(
            container, query_string='versions=true')
        listing = json.loads(body)
        name_to_versions = collections.defaultdict(list)
        for item in listing:
            if item['content_type'].startswith('application/x-deleted'):
                continue
            name_to_versions[item['name']].append(item['version_id'])
        return name_to_versions

    def get_mpu_resources(self, container, mpu_name=None, upload_id=None):
        prefix = ''
        if mpu_name:
            prefix += '\x00' + mpu_name
            if upload_id:
                prefix += '/' + str(upload_id)

        def filter_listing(listing):
            return [item for item in listing
                    if (not mpu_name or item['name'].startswith(prefix))]

        sessions = self.internal_client.iter_objects(
            self.account, '\x00mpu_sessions\x00%s' % self.bucket_name)

        manifests = self.internal_client.iter_objects(
            self.account, '\x00mpu_manifests\x00%s' % container)

        parts = self.internal_client.iter_objects(
            self.account, '\x00mpu_parts\x00%s' % container)

        return (filter_listing(sessions),
                filter_listing(manifests),
                filter_listing(parts))

    def _make_mpu(self, part_size):
        # create
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        self.assertEqual(202, resp.status)
        # upload part
        part_file, hash_, chunk_etags = self.make_file(part_size, 1)
        with open(part_file, 'rb') as fd:
            part_etag = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd, content_type='ignored',
                query_string='upload-id=%s&part-number=1' % upload_id)
        self.assertEqual(hash_, part_etag)
        # complete
        manifest = [{'part_number': 1, 'etag': part_etag}]
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='upload-id=%s' % upload_id,
                                      body=json.dumps(manifest).encode(
                                          'ascii'))
        self.assertEqual(202, resp.status)
        self.assertNotIn('X-Static-Large-Object', resp.headers)
        body_dict = json.loads(body)
        self.assertEqual('201 Created', body_dict['Response Status'])
        mpu_etag = calc_s3mpu_etag(chunk_etags)
        # head mpu
        headers = swiftclient.head_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(part_size), headers.get('content-length'))
        self.assertEqual('"%s"' % mpu_etag, headers.get('etag'))
        return upload_id, mpu_etag


class TestNativeMPU(BaseTestNativeMPU):
    def test_native_mpu(self):
        self.maxDiff = None
        # create
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        containers = self.internal_client.iter_containers(self.account)
        self.assertEqual(['\x00mpu_manifests\x00%s' % self.bucket_name,
                          '\x00mpu_parts\x00%s' % self.bucket_name,
                          '\x00mpu_sessions\x00%s' % self.bucket_name,
                          self.bucket_name],
                         [c['name'] for c in containers])

        # list sessions internal
        sessions = self.internal_client.iter_objects(
            self.account, '\x00mpu_sessions\x00%s' % self.bucket_name)
        self.assertEqual(
            [{'name': '\x00%s/%s' % (self.mpu_name, upload_id),
              'content_type': 'application/x-mpu-session-created',
              'bytes': 0,
              'hash': MD5_OF_EMPTY_STRING,
              'last_modified': mock.ANY}],
            list(sessions))

        # list mpus via API
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertEqual([(self.mpu_name, upload_id)],
                         [(o['name'], o['upload_id']) for o in listing])

        # upload part
        part_size = 5 * 2 ** 20
        part_file, hash_, chunk_etags = self.make_file(part_size, 1)
        with open(part_file, 'rb') as fd:
            part_etag = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd, content_type='ignored',
                query_string='upload-id=%s&part-number=1' % upload_id)
        # TODO: check resp content-length header
        # swiftclient strips the "" from etag!
        self.assertEqual(hash_, part_etag)

        # list parts internal
        parts = self.internal_client.iter_objects(
            self.account, '\x00mpu_parts\x00%s' % self.bucket_name)
        self.assertEqual(
            [{'name': '\x00%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            [part for part in parts])

        # list parts via mpu API
        resp_hdrs, resp_body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='upload-id=%s' % upload_id)
        self.assertEqual(
            [{'name': '%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': part_etag,
              'bytes': part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            json.loads(resp_body))

        # complete
        manifest = [{'part_number': 1, 'etag': part_etag}]
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='upload-id=%s' % upload_id,
                                      body=json.dumps(manifest).encode(
                                          'ascii'))
        self.assertEqual(202, resp.status)
        self.assertNotIn('X-Static-Large-Object', resp.headers)
        body_dict = json.loads(body)
        self.assertEqual('201 Created', body_dict['Response Status'])

        # check manifest exists
        manifests = self.internal_client.iter_objects(
            self.account, '\x00mpu_manifests\x00%s' % self.bucket_name)
        self.assertEqual(['\x00%s/%s' % (self.mpu_name, upload_id)],
                         [o['name'] for o in manifests])
        manifest_meta = self.internal_client.get_object_metadata(
            self.account, '\x00mpu_manifests\x00%s' % self.bucket_name,
            '\x00%s/%s' % (self.mpu_name, upload_id))
        self.assertEqual(upload_id,
                         manifest_meta.get('x-object-sysmeta-mpu-upload-id'),
                         manifest_meta)

        # check mpu sysmeta
        mpu_meta = self.internal_client.get_object_metadata(
            self.account, self.bucket_name, self.mpu_name)
        self.assertEqual(upload_id,
                         mpu_meta.get('x-object-sysmeta-mpu-upload-id'),
                         mpu_meta)

        # check user container listing
        resp_hdrs, user_objs = swiftclient.get_container(
            self.url, self.token, self.bucket_name)
        # TODO: assert etag, bytes etc
        self.assertEqual([self.mpu_name], [o['name'] for o in user_objs])

        # head mpu
        headers = swiftclient.head_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(part_size), headers.get('content-length'))
        self.assertEqual('"%s"' % calc_s3mpu_etag(chunk_etags),
                         headers.get('etag'))
        # download mpu
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(part_size), headers.get('content-length'))
        self.assertEqual('"%s"' % calc_s3mpu_etag(chunk_etags),
                         headers.get('etag'))

        # manifest cannot be downloaded
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='multipart-manifest=get')
        self.assertEqual(400, cm.exception.http_status)

        # session still exists but is marked completed
        sessions = self.internal_client.iter_objects(
            self.account, '\x00mpu_sessions\x00%s' % self.bucket_name)
        self.assertEqual(
            [{'name': '\x00%s/%s' % (self.mpu_name, upload_id),
              'content_type': 'application/x-mpu-session-completed',
              'bytes': 0,
              'hash': MD5_OF_EMPTY_STRING,
              'last_modified': mock.ANY}],
            list(sessions))

        # list mpu sessions via API - empty list
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertFalse(listing)

        # list parts via mpu API -> 404
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='upload-id=%s' % upload_id)
        self.assertEqual(404, cm.exception.http_status)

        # delete the mpu
        swiftclient.delete_object(self.url, self.token, self.bucket_name,
                                  self.mpu_name)
        # check the mpu cannot be downloaded
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(404, cm.exception.http_status)

        # check we still have manifest, and also an audit marker for it
        manifests = self.internal_client.iter_objects(
            self.account, '\x00mpu_manifests\x00%s' % self.bucket_name)
        self.assertEqual(
            ['\x00%s/%s' % (self.mpu_name, upload_id),
             '\x00%s/%s/marker-deleted' % (self.mpu_name, upload_id)],
            [o['name'] for o in manifests])

        # check we still have the parts
        # list parts internal
        parts = self.internal_client.iter_objects(
            self.account, '\x00mpu_parts\x00%s' % self.bucket_name)
        self.assertEqual(
            [{'name': '\x00%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            [part for part in parts])

        # async cleanup: once to process sessions...
        Manager(['container-auditor']).once()
        # ...once to process manifest markers...
        Manager(['container-auditor']).once()
        # ...once more to process any parts markers...
        Manager(['container-auditor']).once()

        # session, manifest and parts have gone :)
        self.assertEqual(([], [], []),
                         self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_abort(self):
        # create
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')

        # list mpus
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertEqual([(self.mpu_name, upload_id)],
                         [(o['name'], o['upload_id']) for o in listing])
        self.assertEqual(['application/x-mpu-session-created'],
                         [o['content_type'] for o in listing])
        # upload part
        part_size = 5 * 2 ** 20
        part_file, hash_, chunk_etags = self.make_file(part_size, 1)
        with open(part_file, 'rb') as fd:
            part_etag = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd,
                query_string='upload-id=%s&part-number=1' % upload_id)

        # list parts via mpu API
        resp_hdrs, resp_body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='upload-id=%s' % upload_id)
        self.assertEqual(
            [{'name': '%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': part_etag,
              'bytes': part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            json.loads(resp_body))

        # abort upload
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='upload-id=%s' % upload_id)

        # try (but fail) to upload another part
        with open(part_file, 'rb') as fd:
            with self.assertRaises(ClientException) as cm:
                swiftclient.put_object(
                    self.url, self.token, self.bucket_name, self.mpu_name,
                    contents=fd,
                    query_string='upload-id=%s&part-number=2' % upload_id)
            self.assertEqual(404, cm.exception.http_status)

        # try (but fail) to list parts via mpu API
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='upload-id=%s' % upload_id)
            self.assertEqual(404, cm.exception.http_status)

        # try (but fail) to complete the upload
        manifest = [{'part_number': 1, 'etag': part_etag}]
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='upload-id=%s' % upload_id,
                                      body=json.dumps(manifest).encode(
                                          'ascii'))
        self.assertEqual(404, resp.status)

        # list mpus via API - empty list
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertFalse(listing)

        # check we still have the parts via internal client
        exp_parts_internal = [
            {'name': '\x00%s/%s/000001' % (self.mpu_name, upload_id),
             'hash': mock.ANY,  # might be encrypted
             'bytes': part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        parts = self.internal_client.iter_objects(
            self.account, '\x00mpu_parts\x00%s' % self.bucket_name)
        self.assertEqual(exp_parts_internal, [part for part in parts])

        # session still exists but is marked aborted
        exp_aborted_sessions = [
            {'name': '\x00%s/%s' % (self.mpu_name, upload_id),
             'content_type': 'application/x-mpu-session-aborted',
             'bytes': 0,
             'hash': MD5_OF_EMPTY_STRING,
             'last_modified': mock.ANY}
        ]
        sessions = self.internal_client.iter_objects(
            self.account, '\x00mpu_sessions\x00%s' % self.bucket_name)
        self.assertEqual(exp_aborted_sessions, list(sessions))

        # immediate audit(s) will pass over the aborted session
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        # ... so session still exists
        sessions = self.internal_client.iter_objects(
            self.account, '\x00mpu_sessions\x00%s' % self.bucket_name)
        self.assertEqual(exp_aborted_sessions, list(sessions))

        # and the parts still exist
        parts = self.internal_client.iter_objects(
            self.account, '\x00mpu_parts\x00%s' % self.bucket_name)
        self.assertEqual(exp_parts_internal, [part for part in parts])

        # a custom audit with zero purge delay will clean up the session
        custom_conf = {'mpu_aborted_purge_delay': '0'}
        for conf_index in self.configs['container-auditor'].keys():
            self.run_custom_daemon(
                ContainerAuditor, 'container-auditor',
                conf_index, custom_conf=custom_conf)

        # now the session is gone
        sessions = self.internal_client.iter_objects(
            self.account, '\x00mpu_sessions\x00%s' % self.bucket_name)
        self.assertFalse(list(sessions))

        # async cleanup, once to process the manifests container markers...
        Manager(['container-auditor']).once()
        # ...and again to ensure generated parts markers are processed
        Manager(['container-auditor']).once()

        # manifest and parts have gone :)
        self.assertEqual(([], [], []),
                         self.get_mpu_resources(self.bucket_name))


class TestNativeMPUWithVersioning(BaseTestNativeMPU):
    def _make_symlink(self, name):
        # create a symlink pointing to an object in another container
        other_bucket = self.bucket_name + '+other'
        swiftclient.put_container(self.url, self.token, other_bucket)
        tgt_obj_name = name + '-target'
        swiftclient.put_object(self.url, self.token, other_bucket,
                               tgt_obj_name, contents='target object')
        symlink_tgt = '%s/%s' % (other_bucket, tgt_obj_name)
        obj_headers = {'X-Symlink-Target': symlink_tgt}
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               name, headers=obj_headers)

    def test_native_mpu_delete_during_versioning(self):
        # verify that mpu behaves same as a plain symlink w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * DELETE objects - a version is retained
        #   * DELETE object version - nothing retained

        # put an mpu
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        part_size = 5 * 2 ** 20
        self._make_mpu(part_size)

        # put a symlink to an object in another bucket, as a control item
        obj_name = 'non-mpu-obj'
        self._make_symlink(obj_name)

        # enable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})

        # delete both objects
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name)
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual(2, len(obj_versions))
        self.assertEqual(1, len(obj_versions[obj_name]))
        self.assertEqual(1, len(obj_versions[self.mpu_name]))

        # run auditor - nothing should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        # objects still exist
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, obj_name,
            query_string='version-id=%s' % obj_versions[obj_name][0])
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][0])

        # delete one version of each object
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name,
            query_string='version-id=%s' % obj_versions[obj_name][0])
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][0])

        # objects cannot be read
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, obj_name,
                query_string='version-id=%s' % obj_versions[obj_name][0])
            self.assertEqual(404, cm.exception.http_status)
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='version-id=%s' % obj_versions[self.mpu_name][0])
            self.assertEqual(404, cm.exception.http_status)

        # run auditor
        for i in range(2):
            Manager(['container-auditor']).once()

        self.assertEqual(([], [], []),
                         self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_no_overwrites_during_versioning(self):
        # verify that mpu behaves same as a plain symlink w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * disable versioning
        #   * DELETE objects - no version is retained

        # put an mpu
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        part_size = 5 * 2 ** 20
        self._make_mpu(part_size)

        # put a symlink to an object in another bucket, as a control item
        obj_name = 'non-mpu-obj'
        self._make_symlink(obj_name)

        # enable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual(2, len(obj_versions))

        # disable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'false'})

        # delete both objects
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name)
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertFalse(obj_versions)

        # objects cannot be read
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, obj_name)
            self.assertEqual(404, cm.exception.http_status)
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name)
            self.assertEqual(404, cm.exception.http_status)

        # run auditor
        for i in range(2):
            Manager(['container-auditor']).once()

        self.assertEqual(([], [], []),
                         self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_overwrite_during_and_delete_after_versioning(self):
        # verify that mpu behaves same as a plain symlink w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * overwrite objects
        #   * disable versioning
        #   * DELETE objects - 2 versions retained

        # put an mpu
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        part_size = 5 * 2 ** 20
        self._make_mpu(part_size)

        # put a symlink to an object in another bucket, as a control item
        obj_name = 'non-mpu-obj'
        self._make_symlink(obj_name)

        # enable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})

        # put objects again
        self._make_mpu(part_size)
        self._make_symlink(obj_name)

        # disable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})

        # delete both objects
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name)
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual(2, len(obj_versions))
        self.assertEqual(2, len(obj_versions[obj_name]))
        self.assertEqual(2, len(obj_versions[self.mpu_name]))

        # run auditor - nothing should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        # objects still exist
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, obj_name,
            query_string='version-id=%s' % obj_versions[obj_name][0])
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, obj_name,
            query_string='version-id=%s' % obj_versions[obj_name][1])
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][0])
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][1])

    def test_native_mpu_delete_specific_version(self):
        #   * PUT objects
        #   * enable versioning
        #   * PUT objects
        #   * DELETE object version - nothing retained for that version

        # put an mpu
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        part_size = 5 * 2 ** 20
        upload_id_0, _ = self._make_mpu(part_size)

        # enable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})

        # put an mpu again
        swiftclient.put_container(self.url, self.token, self.bucket_name)
        part_size = 5 * 2 ** 20
        upload_id_1, _ = self._make_mpu(part_size)

        # run auditor - nothing should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        _, manifests, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_0)
        self.assertEqual(1, len(manifests))
        self.assertEqual(1, len(parts))
        _, manifests, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_1)
        self.assertEqual(1, len(manifests))
        self.assertEqual(1, len(parts))

        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual(1, len(obj_versions))
        self.assertEqual(2, len(obj_versions[self.mpu_name]))

        # objects exist
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][0])
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][1])

        # delete version [1]
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][1])

        # undeleted version still exists
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][0])

        # deleted version cannot be read
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='version-id=%s' % obj_versions[self.mpu_name][1])
            self.assertEqual(404, cm.exception.http_status)

        _, manifests, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_0)
        # delete-marker has been written in manifests container...
        self.assertEqual(2, len(manifests), manifests)
        self.assertEqual(1, len(parts), parts)
        _, manifests, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_1)
        self.assertEqual(1, len(manifests))
        self.assertEqual(1, len(parts))

        # run auditor - deleted version should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        _, manifests, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_0)
        self.assertFalse(manifests)
        self.assertFalse(parts)
        _, manifests, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_1)
        self.assertEqual(1, len(manifests))
        self.assertEqual(1, len(parts))
