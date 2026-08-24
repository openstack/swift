#!/usr/bin/python -u
# Copyright (c) 2026 NVIDIA
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

from swiftclient import client

from test.probe.common import ReplProbeTest
from test.s3api import get_s3_client


class TestCopySysmeta(ReplProbeTest):
    """Exercise native and S3 copies of native and S3 MPU SLOs."""

    def setUp(self):
        super(TestCopySysmeta, self).setUp()
        self.container = self._make_name('s3api-copy-sysmeta-').decode()

        client.put_container(self.url, self.token, self.container)

        self.s3 = get_s3_client()
        self.internal_client = self.make_internal_client()

    def _make_swift_slo(self, source):
        segment_container = self._make_name(
            'swift-slo-segments-').decode()
        client.put_container(self.url, self.token, segment_container)
        segment = 'segment'
        body = b'a probe SLO segment'
        segment_etag = client.put_object(
            self.url, self.token, segment_container, segment, body)
        manifest = [{
            'path': '/%s/%s' % (segment_container, segment),
            'etag': segment_etag,
            'size_bytes': len(body),
        }]
        client.put_object(
            self.url, self.token, self.container, source,
            json.dumps(manifest), query_string='multipart-manifest=put')

    def _make_s3_mpu(self, source):
        upload_id = self.s3.create_multipart_upload(
            Bucket=self.container, Key=source)['UploadId']
        part = b'a probe S3 multipart upload part'
        part_etag = self.s3.upload_part(
            Bucket=self.container, Key=source, PartNumber=1,
            UploadId=upload_id, Body=part)['ETag']
        self.s3.complete_multipart_upload(
            Bucket=self.container, Key=source, UploadId=upload_id,
            MultipartUpload={'Parts': [{
                'ETag': part_etag,
                'PartNumber': 1,
            }]})

    def _swift_copy(self, source, destination, copy_manifest=False):
        parsed, conn = client.http_connection(self.url)
        try:
            source_path = '%s/%s/%s' % (
                parsed.path.rstrip('/'), self.container, source)
            if copy_manifest:
                source_path += '?multipart-manifest=get'
            conn.request('COPY', source_path, b'', {
                'Destination': '/%s/%s' % (self.container, destination),
                'X-Auth-Token': self.token,
            })
            resp = conn.getresponse()
            body = resp.read()
            self.assertEqual(201, resp.status, body)
        finally:
            conn.close()

    def _s3_copy(self, source, destination):
        self.s3.copy_object(
            Bucket=self.container, Key=destination,
            CopySource={'Bucket': self.container, 'Key': source})

    def _assert_source_has_slo_sysmeta(self, source):
        source_headers = self.internal_client.get_object_metadata(
            self.account, self.container, source)
        for key in ('x-object-sysmeta-slo-etag',
                    'x-object-sysmeta-slo-size'):
            self.assertIn(key, source_headers)
        return source_headers

    def _assert_source_is_s3_mpu(self, source):
        source_headers = self._assert_source_has_slo_sysmeta(
            source)
        self.assertIn('x-object-sysmeta-s3api-etag', source_headers)
        return source_headers

    def test_swift_copy_slo_as_slo(self):
        source = self._make_name('swift-slo-source-').decode()
        destination = self._make_name('swift-slo-copy-').decode()
        self._make_swift_slo(source)
        source_headers = self._assert_source_has_slo_sysmeta(
            source)
        self._swift_copy(source, destination, copy_manifest=True)
        swift_copy_headers = self.internal_client.get_object_metadata(
            self.account, self.container, destination)

        # multipart-manifest=get changes the destination PUT to
        # multipart-manifest=put. SLO generates these values for the new SLO;
        # the identical manifest therefore produces identical values.
        # N.B. The s3api copy hook may strip source SLO sysmeta: SLO
        # unconditionally creates these headers for multipart-manifest=put.
        self.assertEqual(source_headers['x-object-sysmeta-slo-etag'],
                         swift_copy_headers['x-object-sysmeta-slo-etag'])
        self.assertEqual(source_headers['x-object-sysmeta-slo-size'],
                         swift_copy_headers['x-object-sysmeta-slo-size'])

    def test_swift_copy_slo_as_regular_object(self):
        source = self._make_name('swift-slo-source-').decode()
        destination = self._make_name('swift-slo-copy-').decode()
        self._make_swift_slo(source)
        source_headers = self._assert_source_has_slo_sysmeta(
            source)
        self._swift_copy(source, destination)
        swift_copy_headers = self.internal_client.get_object_metadata(
            self.account, self.container, destination)
        # XXX: Should we REALLY be preserving SLO sysmeta in this case!?
        self.assertEqual(source_headers['x-object-sysmeta-slo-etag'],
                         swift_copy_headers['x-object-sysmeta-slo-etag'])
        self.assertEqual(source_headers['x-object-sysmeta-slo-size'],
                         swift_copy_headers['x-object-sysmeta-slo-size'])

    def test_s3_copy_slo_as_regular_object(self):
        source = self._make_name('swift-slo-source-').decode()
        destination = self._make_name('swift-s3-copy-').decode()
        self._make_swift_slo(source)
        source_headers = self._assert_source_has_slo_sysmeta(source)
        self._s3_copy(source, destination)
        s3_copy_headers = self.internal_client.get_object_metadata(
            self.account, self.container, destination)
        # XXX: Should we REALLY be preserving SLO sysmeta in this case!?
        self.assertEqual(source_headers['x-object-sysmeta-slo-etag'],
                         s3_copy_headers['x-object-sysmeta-slo-etag'])
        self.assertEqual(source_headers['x-object-sysmeta-slo-size'],
                         s3_copy_headers['x-object-sysmeta-slo-size'])

    def test_swift_copy_mpu_as_slo(self):
        source = self._make_name('mpu-source-').decode()
        destination = self._make_name('mpu-copy-').decode()
        self._make_s3_mpu(source)
        self._assert_source_is_s3_mpu(source)
        self._swift_copy(source, destination, copy_manifest=True)
        copy_headers = self.internal_client.get_object_metadata(
            self.account, self.container, destination)
        self.assertNotIn('x-object-sysmeta-s3api-etag', copy_headers)
        # N.B. The s3api copy hook may strip source SLO sysmeta: SLO
        # unconditionally creates these headers for multipart-manifest=put.
        self.assertIn('x-object-sysmeta-slo-etag', copy_headers)
        self.assertIn('x-object-sysmeta-slo-size', copy_headers)

    def test_swift_copy_mpu_as_regular_object(self):
        source = self._make_name('mpu-source-').decode()
        destination = self._make_name('mpu-copy-').decode()
        self._make_s3_mpu(source)
        self._swift_copy(source, destination)
        copy_headers = self.internal_client.get_object_metadata(
            self.account, self.container, destination)
        self.assertNotIn('x-object-sysmeta-s3api-etag', copy_headers)
        self.assertNotIn('x-object-sysmeta-slo-etag', copy_headers)
        self.assertNotIn('x-object-sysmeta-slo-size', copy_headers)

    def test_s3_copy_mpu_as_regular_object(self):
        source = self._make_name('mpu-source-').decode()
        destination = self._make_name('mpu-s3-copy-').decode()
        self._make_s3_mpu(source)
        self._assert_source_is_s3_mpu(source)
        self._s3_copy(source, destination)
        copy_headers = self.internal_client.get_object_metadata(
            self.account, self.container, destination)
        self.assertNotIn('x-object-sysmeta-s3api-etag', copy_headers)
        self.assertNotIn('x-object-sysmeta-slo-etag', copy_headers)
        self.assertNotIn('x-object-sysmeta-slo-size', copy_headers)
