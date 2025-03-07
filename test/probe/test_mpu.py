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
import unittest
import uuid
from tempfile import mkdtemp
import os.path
import shutil
import random
from hashlib import md5

from swiftclient import client as swiftclient

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES

from boto3.s3.transfer import TransferConfig
from test.s3api import get_s3_client

from unittest import mock


class TestMixedPolicyMPU(ReplProbeTest):

    @unittest.skipIf(len(ENABLED_POLICIES) < 2, "Need more than one policy")
    def setUp(self):
        self.tempdir = mkdtemp()
        super(TestMixedPolicyMPU, self).setUp()
        s3api_info = self.cluster_info.get('s3api', {})
        if not s3api_info:
            raise unittest.SkipTest('s3api not enabled')

        self.s3 = get_s3_client(1)

        self.bucket_name = 'bucket-%s' % uuid.uuid4()
        self.mpu_name = 'mpu-%s' % uuid.uuid4()
        self.segment_bucket_name = self.bucket_name + '+segments'
        self.bucket_brain = BrainSplitter(self.url, self.token,
                                          self.bucket_name)
        self.segments_brain = BrainSplitter(self.url, self.token,
                                            self.segment_bucket_name)
        self.other_policy = random.choice([p for p in ENABLED_POLICIES
                                           if p != self.policy])

    def make_large_file(self, chunksize, num_chunks):
        filename = os.path.join(self.tempdir, 'big.file')
        md5_hasher = md5()
        slo_etag_hasher = md5()
        with open(filename, 'wb') as f:
            c = 'a'
            for i in range(num_chunks):
                c = chr(ord(c) + i)
                chunk = c.encode() * chunksize
                f.write(chunk)
                md5_hasher.update(chunk)
                chunk_etag = md5(chunk).hexdigest()
                slo_etag_hasher.update(chunk_etag.encode())
        return filename, md5_hasher.hexdigest(), slo_etag_hasher.hexdigest()

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        super(TestMixedPolicyMPU, self).tearDown()

    def _assert_container_storage_policy(self, container_name,
                                         expected_policy):
        headers = swiftclient.head_container(self.url, self.token,
                                             container_name)
        self.assertEqual(headers['x-storage-policy'], expected_policy.name)

    def test_mixed_policy_upload(self):
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
        # I think boto has a minimum chunksize that matches AWS, when I do this
        # too small I get less chunks in the SLO than I expect
        chunksize = 5 * 2 ** 20
        config = TransferConfig(multipart_threshold=chunksize,
                                multipart_chunksize=chunksize)
        num_chunks = 3
        data_filename, md5_hash, slo_etag = self.make_large_file(chunksize,
                                                                 num_chunks)
        expected_size = chunksize * num_chunks

        self.s3.upload_file(data_filename, self.bucket_name, self.mpu_name,
                            Config=config)
        # s3 mpu request succeeds
        s3_head_resp = self.s3.head_object(Bucket=self.bucket_name,
                                           Key=self.mpu_name)
        self.assertEqual(expected_size, int(s3_head_resp['ContentLength']))
        self.assertEqual(num_chunks, int(
            s3_head_resp['ETag'].strip('"').rsplit('-')[-1]))
        # swift response is the same
        swift_obj_headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            resp_chunk_size=65536)
        self.assertEqual(expected_size,
                         int(swift_obj_headers['content-length']))
        self.assertEqual(slo_etag, swift_obj_headers['etag'].strip('"'))
        hasher = md5()
        for chunk in body:
            hasher.update(chunk)
        self.assertEqual(md5_hash, hasher.hexdigest())

        # s3 listing has correct bytes
        resp = self.s3.list_objects(Bucket=self.bucket_name)
        # note: with PY2 the args order (expected, actual) is significant for
        # mock.ANY == datetime(...) to be true
        self.assertEqual([{
            u'ETag': s3_head_resp['ETag'],
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
            'hash': swift_obj_headers['x-manifest-etag'],
            'last_modified': mock.ANY,
            'name': self.mpu_name,
            's3_etag': s3_head_resp['ETag'],
            'slo_etag': swift_obj_headers['etag'],
        }])
        # check segments
        stat, listing = swiftclient.get_container(
            self.url, self.token, self.segment_bucket_name)
        self.assertEqual(stat['x-storage-policy'], self.other_policy.name)
        self.assertEqual([item['name'].split('/')[0] for item in listing],
                         [self.mpu_name] * 3)
