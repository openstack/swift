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
import copy
import json
import unittest
import uuid
from io import BytesIO
from tempfile import mkdtemp
import os.path
import shutil
import random

from swift.common.manager import Manager
from swift.common.middleware.mpu import MPUSessionHandler
from swift.common.storage_policy import POLICIES
from swift.common.swob import normalize_etag
from swift.common.utils import quote, md5, MD5_OF_EMPTY_STRING, Timestamp
from swift.container.auditor import ContainerAuditor
from swiftclient import client as swiftclient, ClientException

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES, \
    DEFAULT_INTERNAL_CLIENT_CONF

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
        self.mpu_internal_client = self.make_mpu_internal_client()
        # TODO: revert to superclass uuid name
        self.mpu_name = 'tempname'
        self.sessions_container_name = \
            '\x00mpu_sessions\x00%s' % self.bucket_name
        self.parts_container_name = '\x00mpu_parts\x00%s' % self.bucket_name
        self.part_size = 5 * 2 ** 20
        swiftclient.put_container(self.url, self.token, self.bucket_name)

    def make_mpu_internal_client(self):
        conf = copy.deepcopy(DEFAULT_INTERNAL_CLIENT_CONF)
        conf['DEFAULT']['log_name'] = 'mpu-internal-client'
        conf['DEFAULT']['log_level'] = 'DEBUG'
        conf['filter:mpu'] = {'use': 'egg:swift#mpu'}
        conf['filter:slo'] = {'use': 'egg:swift#slo'}
        conf['filter:proxy-logging'] = {'use': 'egg:swift#proxy_logging'}
        pipeline = conf['pipeline:main']['pipeline']
        pipeline = pipeline.split(' ')
        pipeline.insert(pipeline.index('proxy-server'), 'slo')
        pipeline.insert(pipeline.index('slo'), 'mpu')
        pipeline.insert(pipeline.index('catch_errors') + 1, 'proxy-logging')
        conf['pipeline:main']['pipeline'] = ' '.join(pipeline)
        return self.make_internal_client(conf=conf)

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
            self.account, self.sessions_container_name)

        parts = self.internal_client.iter_objects(
            self.account, self.parts_container_name)

        return filter_listing(sessions), filter_listing(parts)

    def _make_mpu(self, part_size):
        # create
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        self.assertEqual(202, resp.status)
        # upload part
        part_file, hash_, chunk_etags = self.make_file(self.part_size, 1)
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
        self.assertEqual(str(self.part_size), headers.get('content-length'))
        self.assertEqual('"%s"' % mpu_etag, headers.get('etag'))
        return upload_id, mpu_etag


class TestNativeMPU(BaseTestNativeMPU):
    def test_native_mpu(self):
        self.maxDiff = None
        # create
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        containers = self.internal_client.iter_containers(self.account)
        self.assertEqual([self.parts_container_name,
                          self.sessions_container_name,
                          self.bucket_name],
                         [c['name'] for c in containers])

        # list sessions internal
        sessions = self.internal_client.iter_objects(
            self.account, self.sessions_container_name)
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
        part_file, hash_, chunk_etags = self.make_file(self.part_size, 1)
        with open(part_file, 'rb') as fd:
            part_etag = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd, content_type='ignored',
                query_string='upload-id=%s&part-number=1' % upload_id)
        # TODO: check resp content-length header
        # swiftclient strips the "" from etag!
        self.assertEqual(hash_, part_etag)

        # list parts internal
        parts = [part for part in self.internal_client.iter_objects(
            self.account, self.parts_container_name)]
        self.assertEqual(
            [{'name': '\x00%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': self.part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            parts)

        # list parts via mpu API
        resp_hdrs, resp_body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='upload-id=%s' % upload_id)
        self.assertEqual(
            [{'name': '%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': part_etag,
              'bytes': self.part_size,
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
        self.assertEqual(str(self.part_size), headers.get('content-length'))
        self.assertEqual('"%s"' % calc_s3mpu_etag(chunk_etags),
                         headers.get('etag'))
        # download mpu
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(self.part_size), headers.get('content-length'))
        self.assertEqual('"%s"' % calc_s3mpu_etag(chunk_etags),
                         headers.get('etag'))

        # manifest cannot be downloaded by users
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='multipart-manifest=get')
        self.assertEqual(400, cm.exception.http_status)
        # but can be downloaded by an internal client with no SLO
        status, headers, resp_iter = self.internal_client.get_object(
            self.account, self.bucket_name, self.mpu_name,
            headers={'X-Object-Sysmeta-Allow-Reserved-Names': 'true'})
        self.assertEqual(200, status)
        exp_manifest = [dict(part,
                             name='/\x00mpu_parts\x00%s/%s'
                                  % (self.bucket_name, part['name']),
                             last_modified=mock.ANY)
                        for part in parts]
        manifest = json.loads(b''.join(resp_iter))
        self.assertEqual(exp_manifest, manifest)

        # session still exists but is marked completed
        sessions = self.internal_client.iter_objects(
            self.account, self.sessions_container_name)
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

        # check we still have the parts, and also an audit marker for it
        # list parts internal
        parts = self.internal_client.iter_objects(
            self.account, self.parts_container_name)
        self.assertEqual(
            [{'name': '\x00%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': self.part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY},
             {'name': '\x00%s/%s/marker-deleted' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': 0,
              'content_type': 'application/x-mpu-marker',
              'last_modified': mock.ANY}],
            [part for part in parts])

        # async cleanup: once to process sessions...
        Manager(['container-auditor']).once()
        # ...once more to process any parts markers...
        Manager(['container-auditor']).once()

        # session, manifest and parts have gone :)
        self.assertEqual(([], []), self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_abort(self):
        # create
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
        part_file, hash_, chunk_etags = self.make_file(self.part_size, 1)
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
              'bytes': self.part_size,
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
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        parts = self.internal_client.iter_objects(
            self.account, self.parts_container_name)
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
            self.account, self.sessions_container_name)
        self.assertEqual(exp_aborted_sessions, list(sessions))

        # immediate audit(s) will pass over the aborted session
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        # ... so session still exists
        sessions, parts = self.get_mpu_resources(self.bucket_name)
        self.assertEqual(exp_aborted_sessions, list(sessions))
        # and the parts still exist
        self.assertEqual(exp_parts_internal, [part for part in parts])

        # a custom audit with zero purge delay will clean up the session
        custom_conf = {'mpu_aborted_purge_delay': '0'}
        for conf_index in self.configs['container-auditor'].keys():
            self.run_custom_daemon(
                ContainerAuditor, 'container-auditor',
                conf_index, custom_conf=custom_conf)

        # now the session is gone
        sessions, parts = self.get_mpu_resources(self.bucket_name)
        self.assertFalse(list(sessions))

        # async cleanup to ensure generated parts markers are processed
        Manager(['container-auditor']).once()

        # parts have gone :)
        self.assertEqual(([], []), self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_concurrent_complete_on_handoff(self):
        # verify that if two concurrent completeUploads both succeed in writing
        # different manifests on different nodes then, after replication, the
        # most recent completeUpload is retained
        obj_brain = BrainSplitter(
            self.url, self.token, container_name=self.bucket_name,
            object_name=self.mpu_name, server_type='object',
            policy=POLICIES.default)
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        self.assertEqual(202, resp.status)
        upload_id = resp.headers.get('X-Upload-Id')

        # upload part
        part_file, hash_, chunk_etags_1 = self.make_file(self.part_size, 1)
        with open(part_file, 'rb') as fd:
            part_etag_1 = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd,
                query_string='upload-id=%s&part-number=1' % upload_id)
        part_file, hash_, chunk_etags_2 = self.make_file(99, 1)
        with open(part_file, 'rb') as fd:
            part_etag_2 = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd,
                query_string='upload-id=%s&part-number=2' % upload_id)

        # concurrent completes
        mpu_path = self.mpu_internal_client.make_path(
            self.account, self.bucket_name, self.mpu_name)

        orig_post_session_completed = MPUSessionHandler._post_session_completed
        orig_put_manifest = MPUSessionHandler._put_manifest
        completes = 0
        downloads = []

        def mock_put_manifest(*args, **kwargs):
            # each completeUpload writes manifest to a different set of nodes
            if completes == 0:
                obj_brain.stop_handoff_half()
            else:
                obj_brain.stop_primary_half()
            result = orig_put_manifest(*args, **kwargs)
            if completes == 0:
                obj_brain.start_handoff_half()
            else:
                obj_brain.start_primary_half()
            return result

        def mock_post_session_completed(*args, **kwargs):
            downloads.append(swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name))
            nonlocal completes
            completes += 1
            if completes == 1:
                # send another completeUpload before the first updates the
                # session object, with a different manifest
                manifest = [{'part_number': 2, 'etag': part_etag_2}]
                resp = self.mpu_internal_client.make_request(
                    'POST', mpu_path, headers={}, acceptable_statuses=[202],
                    body_file=BytesIO(json.dumps(manifest).encode('ascii')),
                    params={'upload-id': upload_id})
                body_dict = json.loads(resp.body)
                self.assertEqual('201 Created', body_dict['Response Status'])
            return orig_post_session_completed(*args, **kwargs)

        with mock.patch(
                'swift.common.middleware.mpu.MPUSessionHandler.'
                '_put_manifest', mock_put_manifest), \
                mock.patch(
                    'swift.common.middleware.mpu.MPUSessionHandler.'
                    '_post_session_completed', mock_post_session_completed):
            manifest = [{'part_number': 1, 'etag': part_etag_1}]
            resp = self.mpu_internal_client.make_request(
                'POST', mpu_path, headers={}, acceptable_statuses=[202],
                body_file=BytesIO(json.dumps(manifest).encode('ascii')),
                params={'upload-id': upload_id})
            body_dict = json.loads(resp.body)
            self.assertEqual('201 Created', body_dict['Response Status'])

        # check the downloads...
        exp_etags = ['"%s"' % calc_s3mpu_etag(chunk_etags_1),
                     '"%s"' % calc_s3mpu_etag(chunk_etags_2)]
        self.assertEqual(
            [str(self.part_size), '99'],
            [headers.get('content-length') for headers, body in downloads])
        self.assertEqual(
            exp_etags, [headers.get('etag') for headers, body in downloads])

        # list mpus to check the session is completed
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertFalse(listing)

        # run daemons to check that the mpu parts are *not* cleaned up
        Manager(['object-replicator']).once()
        Manager(['object-replicator']).once()
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        sessions, parts = self.get_mpu_resources(self.bucket_name)
        self.assertFalse(sessions)
        self.assertEqual(
            [{'name': '\x00%s/%s/000001' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': self.part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY},
             {'name': '\x00%s/%s/000002' % (self.mpu_name, upload_id),
              'hash': mock.ANY,  # might be encrypted
              'bytes': 99,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            [part for part in parts])

        # check mpu is still intact
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual('99', headers.get('content-length'))
        self.assertEqual(exp_etags[1], headers.get('etag'))


class TestNativeMPUWithVersioning(BaseTestNativeMPU):
    def test_native_mpu_delete_during_versioning(self):
        # verify that mpu behaves same as a plain object w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * DELETE objects - a version is retained
        #   * DELETE object version - nothing retained

        # put an mpu
        self._make_mpu(self.part_size)

        # put an object as a control item
        obj_name = 'non-mpu-obj'
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               obj_name)

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

        self.assertEqual(([], []), self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_no_overwrites_during_versioning(self):
        # verify that mpu behaves same as a plain object w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * disable versioning
        #   * DELETE objects - no version is retained

        # put an mpu
        self._make_mpu(self.part_size)

        # put an object as a control item
        obj_name = 'non-mpu-obj'
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               obj_name)

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

        self.assertEqual(([], []), self.get_mpu_resources(self.bucket_name))

    def test_native_mpu_overwrite_during_and_delete_after_versioning(self):
        # verify that mpu behaves same as a plain object w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * overwrite objects
        #   * disable versioning
        #   * DELETE objects - 2 versions retained
        brain = BrainSplitter(self.url, self.token, self.bucket_name,
                              self.mpu_name, server_type='object',
                              policy=self.policy)

        # put an mpu
        self._make_mpu(self.part_size)

        # stash the timestamp
        headers = swiftclient.head_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        ts_data = Timestamp(headers.get('x-timestamp'))

        # post to the mpu so it's x-timestamp is advanced
        obj_metadata = {'x-object-meta-test': 'foo'}
        swiftclient.post_object(self.url, self.token, self.bucket_name,
                                self.mpu_name, headers=obj_metadata)
        headers = swiftclient.head_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual('foo', headers.get('x-object-meta-test'))
        ts_meta = Timestamp(headers.get('x-timestamp'))
        self.assertGreater(float(ts_meta), float(ts_data))

        # enable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})

        # overwrite the mpu
        brain.stop_handoff_half()
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               self.mpu_name, contents='version two')
        brain.start_handoff_half()

        # disable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'false'})

        # get listing with versions
        Manager(['container-replicator']).once()
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual(1, len(obj_versions))
        self.assertEqual(2, len(obj_versions[self.mpu_name]))

        # delete the obj
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        # versions still exists
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][0])
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % obj_versions[self.mpu_name][1])

        # run auditor - nothing should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        # versions still exists
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
        upload_id_0, _ = self._make_mpu(self.part_size)

        # enable versioning
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers={'x-versions-enabled': 'true'})

        # put an mpu again
        upload_id_1, _ = self._make_mpu(self.part_size)

        # run auditor - nothing should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        _, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_0)
        self.assertEqual(1, len(parts))
        _, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_1)
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

        _, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_0)
        # delete-marker has been written in parts container...
        self.assertEqual(2, len(parts), parts)
        _, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_1)
        self.assertEqual(1, len(parts))

        # run auditor - deleted version should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        _, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_0)
        self.assertFalse(parts)
        _, parts = self.get_mpu_resources(
            self.bucket_name, self.mpu_name, upload_id_1)
        self.assertEqual(1, len(parts))
