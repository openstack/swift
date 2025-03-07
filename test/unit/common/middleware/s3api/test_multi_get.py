# Copyright (c) 2023 NVIDIA
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
import string
import json
from unittest import mock

from swift.common import swob, utils
from swift.common.request_helpers import get_reserved_name
from swift.common.middleware import symlink
from swift.common.middleware.versioned_writes import object_versioning as ov

from test.unit import make_timestamp_iter
from test.unit.common.middleware.test_slo import slo, md5hex
from test.unit.common.middleware.s3api import (
    S3ApiTestCase, S3ApiTestCaseAcl, _gen_test_headers)


def _prepare_mpu(swift, ts_iter, upload_id, num_segments,
                 segment_bucket='bucket+segments', segment_key='mpu'):
    manifest = []
    for i, letter in enumerate(string.ascii_lowercase):
        if len(manifest) >= num_segments:
            break
        size = (i + 1) * 5
        body = letter * size
        etag = md5hex(body)
        path = '/%s/%s/%s/%s' % (segment_bucket, segment_key, upload_id, i + 1)
        swift.register('GET', '/v1/AUTH_test' + path, swob.HTTPOk, {
            'Content-Length': len(body),
            'Etag': etag,
        }, body)
        manifest.append({
            "name": path,
            "bytes": size,
            "hash": etag,
            "content_type": "application/octet-stream",
            "last_modified": next(ts_iter).isoformat,
        })
    slo_etag = md5hex(''.join(s['hash'] for s in manifest))
    s3_hash = md5hex(binascii.a2b_hex(''.join(
        s['hash'] for s in manifest)))
    s3_etag = "%s-%s" % (s3_hash, len(manifest))
    manifest_json = json.dumps(manifest)
    json_md5 = md5hex(manifest_json)
    manifest_headers = {
        'Content-Length': str(len(manifest_json)),
        'X-Static-Large-Object': 'true',
        'Etag': json_md5,
        'Content-Type': 'application/octet-stream',
        'X-Object-Sysmeta-Slo-Etag': slo_etag,
        'X-Object-Sysmeta-Slo-Size': str(sum(
            s['bytes'] for s in manifest)),
        'X-Object-Sysmeta-S3Api-Etag': s3_etag,
        'X-Object-Sysmeta-S3Api-Upload-Id': upload_id,
        'X-Object-Sysmeta-Container-Update-Override-Etag':
        '%s; s3_etag=%s; slo_etag=%s' % (json_md5, s3_etag, slo_etag),
    }
    return manifest_headers, manifest_json


class TestMpuGETorHEAD(S3ApiTestCase):

    def _wrap_app(self, app):
        self.slo = slo.filter_factory({'rate_limit_under_size': '0'})(app)
        return super(TestMpuGETorHEAD, self)._wrap_app(self.slo)

    def setUp(self):
        # this will call our _wrap_app
        super(TestMpuGETorHEAD, self).setUp()
        self.ts = make_timestamp_iter()
        manifest_headers, manifest_json = _prepare_mpu(
            self.swift, self.ts, 'X', 3)
        self.s3_etag = manifest_headers['X-Object-Sysmeta-S3Api-Etag']
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket/mpu',
            swob.HTTPOk, manifest_headers, manifest_json.encode('ascii'))
        self.s3_acl = False

    def test_mpu_GET(self):
        req = swob.Request.blank('/bucket/mpu', headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(body, b'aaaaabbbbbbbbbbccccccccccccccc')
        expected_calls = [
            ('GET', '/v1/AUTH_test/bucket/mpu'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X/1'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X/2'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X/3'
             '?multipart-manifest=get'),
        ]
        if self.s3_acl:
            # pre-flight object ACL check
            expected_calls.insert(0, ('HEAD', '/v1/AUTH_test/bucket/mpu'))
        self.assertEqual(self.swift.calls, expected_calls)
        self.assertEqual(headers['Content-Length'], '30')
        self.assertEqual(headers['Etag'], '"%s"' % self.s3_etag)
        self.assertNotIn('X-Amz-Mp-Parts-Count', headers)

    def test_mpu_GET_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': '2',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '206')
        self.assertEqual(body, b'bbbbbbbbbb')
        expected_calls = [
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=2'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X/2'
             '?multipart-manifest=get'),
        ]
        if self.s3_acl:
            expected_calls.insert(0, ('HEAD', '/v1/AUTH_test/bucket/mpu'))
        self.assertEqual(self.swift.calls, expected_calls)
        self.assertEqual(headers['Content-Length'], '10')
        self.assertEqual(headers['Content-Range'], 'bytes 5-14/30')
        self.assertEqual(headers['Etag'], '"%s"' % self.s3_etag)
        self.assertEqual(headers['X-Amz-Mp-Parts-Count'], '3')

    def test_mpu_GET_invalid_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': 'foo',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        self.assertEqual(self.swift.calls, [])

    def test_mpu_GET_zero_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': '0',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        self.assertEqual(self.swift.calls, [])

    def _do_test_mpu_GET_out_of_range_part_num(self, part_number):
        self.swift.clear_calls()
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': str(part_number),
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '416')
        self.assertEqual(self._get_error_code(body), 'InvalidPartNumber')
        expected_calls = [
            # s3api.controller.obj doesn't know yet if it's SLO, we delegate
            # param validation
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=%s' % part_number),
        ]
        if self.s3_acl:
            expected_calls.insert(0, ('HEAD', '/v1/AUTH_test/bucket/mpu'))
        self.assertEqual(self.swift.calls, expected_calls)

    def test_mpu_GET_out_of_range_part_num(self):
        self._do_test_mpu_GET_out_of_range_part_num(4)
        self._do_test_mpu_GET_out_of_range_part_num(10000)

    def test_existing_part_number_greater_than_max_parts_allowed(self):
        part_number = 3
        max_parts = 2
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': str(part_number),
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        bad_req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': str(part_number + 1),
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        with mock.patch.object(self.s3api.conf,
                               'max_upload_part_num', max_parts):
            # num_parts >= part number > max parts
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '206')
            # part number > num parts > max parts
            status, headers, body = self.call_s3api(bad_req)
            self.assertEqual(status.split()[0], '400')
            self.assertIn('must be an integer between 1 and 3, inclusive',
                          self._get_error_message(body))

        max_parts = part_number + 1
        with mock.patch.object(self.s3api.conf,
                               'max_upload_part_num', max_parts):
            # max_parts > num_parts >= part number
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '206')
            # max_parts >= part number > num parts
            status, headers, body = self.call_s3api(bad_req)
            self.assertEqual(status.split()[0], '416')
            self.assertIn('The requested partnumber is not satisfiable',
                          self._get_error_message(body))
            # part number > max_parts > num parts
            bad_req.params = {'partNumber': str(max_parts + 1)}
            status, headers, body = self.call_s3api(bad_req)
            self.assertEqual(status.split()[0], '400')
            self.assertIn('must be an integer between 1 and 4, inclusive',
                          self._get_error_message(body))

    def test_mpu_GET_huge_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': '10001',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        expected_calls = [
            # XXX is this value configurable?  do we need the SLO request?
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=10001'),
        ]
        if self.s3_acl:
            expected_calls.insert(0, ('HEAD', '/v1/AUTH_test/bucket/mpu'))
        self.assertEqual(self.swift.calls, expected_calls)

    def test_mpu_HEAD_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': '1',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        }, method='HEAD')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '206')
        self.assertEqual(body, b'')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket/mpu?part-number=1'),
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=1'),
        ])
        self.assertEqual(headers['Content-Length'], '5')
        self.assertEqual(headers['Content-Range'], 'bytes 0-4/30')
        self.assertEqual(headers['Etag'], '"%s"' % self.s3_etag)
        self.assertEqual(headers['X-Amz-Mp-Parts-Count'], '3')

    def test_mpu_HEAD_invalid_part_num(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'partNumber': 'foo',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, _ = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self.swift.calls, [])

    def test_mpu_HEAD_zero_part_num(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'partNumber': '0',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, _ = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self.swift.calls, [])

    def _do_test_mpu_HEAD_out_of_range_part_num(self, part_number):
        self.swift.clear_calls()
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'partNumber': str(part_number),
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, _ = self.call_s3api(req)
        self.assertEqual(status.split()[0], '416')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket/mpu?part-number=%s' % part_number),
            # SLO has to refetch to *see* if it's out-of-bounds
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=%s' % part_number),
        ])

    def test_mpu_HEAD_out_of_range_part_num(self):
        self._do_test_mpu_HEAD_out_of_range_part_num(4)
        self._do_test_mpu_HEAD_out_of_range_part_num(10000)

    def test_mpu_HEAD_huge_part_num(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'partNumber': '10001',
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket/mpu?part-number=10001'),
            # XXX were two requests worth it to 400?
            # how big can you configure SLO?
            # do such manifests *exist*?
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=10001'),
        ])


class TestMpuGETorHEADAcl(TestMpuGETorHEAD, S3ApiTestCaseAcl):

    def setUp(self):
        super(TestMpuGETorHEADAcl, self).setUp()
        object_headers = _gen_test_headers(
            self.default_owner, self.grants, 'object')
        self.swift.update_sticky_response_headers(
            '/v1/AUTH_test/bucket/mpu', object_headers)
        # this is used to flag insertion of expected HEAD pre-flight request of
        # object ACLs
        self.s3_acl = True


class TestVersionedMpuGETorHEAD(S3ApiTestCase):

    def _wrap_app(self, app):
        self.sym = symlink.filter_factory({})(app)
        self.sym.logger = self.swift.logger
        self.ov = ov.ObjectVersioningMiddleware(self.sym, {})
        self.ov.logger = self.swift.logger
        self.slo = slo.filter_factory({'rate_limit_under_size': '0'})(self.ov)
        self.slo.logger = self.swift.logger
        return super(TestVersionedMpuGETorHEAD, self)._wrap_app(self.slo)

    def setUp(self):
        # this will call our _wrap_app
        super(TestVersionedMpuGETorHEAD, self).setUp()
        self.ts = make_timestamp_iter()
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments',
                            swob.HTTPNoContent, {}, None)
        versions_container = get_reserved_name('versions', 'bucket')
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent, {
                ov.SYSMETA_VERSIONS_CONT: versions_container,
                ov.SYSMETA_VERSIONS_ENABLED: True,
            }, None)
        self.swift.register('HEAD', '/v1/AUTH_test/%s' % versions_container,
                            swob.HTTPNoContent, {}, None)
        num_versions = 3
        self.version_ids = []
        for v in range(num_versions):
            upload_id = 'X%s' % v
            num_segments = 3 + v
            manifest_headers, manifest_json = _prepare_mpu(
                self.swift, self.ts, upload_id, num_segments)
            version_ts = next(self.ts)
            # add in a little user-meta to keep versions stright
            manifest_version_headers = dict(manifest_headers, **{
                'x-object-meta-user-notes': 'version%s' % v,
                'x-backend-timestamp': version_ts.internal,
            })
            self.version_ids.append(version_ts.normal)
            obj_version_path = get_reserved_name('mpu', (~version_ts).normal)
            self.swift.register(
                'GET', '/v1/AUTH_test/%s/%s' % (
                    versions_container, obj_version_path),
                swob.HTTPOk, manifest_version_headers,
                manifest_json.encode('ascii'))
        # TODO: make a current version symlink
        symlink_target = '%s/%s' % (versions_container, obj_version_path)
        slo_etag = manifest_headers['X-Object-Sysmeta-Slo-Etag']
        s3_etag = manifest_headers['X-Object-Sysmeta-S3Api-Etag']
        symlink_target_etag = json_md5 = manifest_headers['Etag']
        symlink_target_bytes = manifest_headers['X-Object-Sysmeta-Slo-Size']
        manifest_symlink_headers = dict(manifest_headers, **{
            'X-Object-Sysmeta-Container-Update-Override-Etag':
            '%s; s3_etag=%s; slo_etag=%s; symlink_target=%s; '
            'symlink_target_etag=%s; symlink_target_bytes=%s' % (
                json_md5, s3_etag, slo_etag, symlink_target,
                symlink_target_etag, symlink_target_bytes),
            'X-Object-Sysmeta-Allow-Reserved-Names': 'true',
            'X-Object-Sysmeta-Symlink-Target': symlink_target,
            'X-Object-Sysmeta-Symlink-Target-Bytes': str(symlink_target_bytes),
            'X-Object-Sysmeta-Symlink-Target-Etag': symlink_target_etag,
            'X-Object-Sysmeta-Symloop-Extend': 'true',
            'X-Object-Sysmeta-Versions-Symlink': 'true',
        })
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket/mpu', swob.HTTPOk,
            manifest_symlink_headers, '')
        self.s3_acl = False

    def test_mpu_GET_version(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'versionId': self.version_ids[0],
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version0')
        self.assertEqual(headers['Content-Length'], '30')
        self.assertEqual(body, b'aaaaabbbbbbbbbbccccccccccccccc')
        expected_calls = [
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('GET', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?version-id=%s' % (
                 (~utils.Timestamp(self.version_ids[0])).normal,
                 self.version_ids[0])),
            ('HEAD', '/v1/AUTH_test/bucket+segments'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X0/1'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X0/2'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X0/3'
             '?multipart-manifest=get')
        ]
        if self.s3_acl:
            expected_calls.insert(3, (
                'HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
                '?version-id=%s' % (
                    (~utils.Timestamp(self.version_ids[0])).normal,
                    self.version_ids[0])
            ))
        self.assertEqual(self.swift.calls, expected_calls)

    def test_mpu_GET_last_version(self):
        req = swob.Request.blank('/bucket/mpu', headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version2')
        self.assertEqual(headers['Content-Length'], '75')
        expected_calls = [
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('GET', '/v1/AUTH_test/bucket/mpu'),
            ('GET', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s' % (
                ~utils.Timestamp(self.version_ids[2])).normal),
            ('HEAD', '/v1/AUTH_test/bucket+segments'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/1'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/2'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/3'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/4'
             '?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/5'
             '?multipart-manifest=get'),
        ]
        if self.s3_acl:
            # the pre-flight head on version marker get's symlinked; but I
            # think maybe symlink makes metadata addative?
            expected_calls = expected_calls[:3] + [
                ('HEAD', '/v1/AUTH_test/bucket/mpu'),
                ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00'
                 '%s' % (~utils.Timestamp(self.version_ids[2])).normal),
            ] + expected_calls[3:]
        self.assertEqual(expected_calls, self.swift.calls)

    def test_mpu_HEAD_last_version(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version2')
        self.assertEqual(headers['Content-Length'], '75')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('HEAD', '/v1/AUTH_test/bucket/mpu'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s' % (
                ~utils.Timestamp(self.version_ids[2])).normal),
        ], self.swift.calls)

    def test_mpu_HEAD_version(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'versionId': self.version_ids[1],
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version1')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(body, b'')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?version-id=%s' % (
                 (~utils.Timestamp(self.version_ids[1])).normal,
                 self.version_ids[1])),
        ], self.swift.calls)

    def test_mpu_GET_version_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'versionId': self.version_ids[2],
            'partNumber': 5,
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version2')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(body, b'e' * 25)
        expected_calls = [
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('GET', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?part-number=5&version-id=%s' % (
                 (~utils.Timestamp(self.version_ids[2])).normal,
                 self.version_ids[2])),
            ('HEAD', '/v1/AUTH_test/bucket+segments'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/5'
             '?multipart-manifest=get'),
        ]
        if self.s3_acl:
            expected_calls.insert(3, (
                'HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
                '?version-id=%s' % (
                    (~utils.Timestamp(self.version_ids[2])).normal,
                    self.version_ids[2])
            ))
        self.assertEqual(expected_calls, self.swift.calls)

    def test_mpu_HEAD_version_part_num(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'versionId': self.version_ids[2],
            'partNumber': 3,
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version2')
        self.assertEqual(headers['Content-Length'], '15')
        self.assertEqual(body, b'')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?part-number=3&version-id=%s' % (
                 (~utils.Timestamp(self.version_ids[2])).normal,
                 self.version_ids[2])),
            ('GET', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?part-number=3&version-id=%s' % (
                 (~utils.Timestamp(self.version_ids[2])).normal,
                 self.version_ids[2])),
        ])

    def test_mpu_GET_last_version_part_num(self):
        req = swob.Request.blank('/bucket/mpu', params={
            'partNumber': 4,
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version2')
        self.assertEqual(headers['Content-Length'], '20')
        self.assertEqual(body, b'd' * 20)
        expected_calls = [
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=4'),
            ('GET', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?part-number=4' % (
                 ~utils.Timestamp(self.version_ids[2])).normal),
            ('HEAD', '/v1/AUTH_test/bucket+segments'),
            ('GET', '/v1/AUTH_test/bucket+segments/mpu/X2/4'
             '?multipart-manifest=get'),
        ]
        if self.s3_acl:
            expected_calls = expected_calls[:3] + [
                ('HEAD', '/v1/AUTH_test/bucket/mpu'),
                ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00'
                 '%s' % (~utils.Timestamp(self.version_ids[2])).normal),
            ] + expected_calls[3:]
        self.assertEqual(expected_calls, self.swift.calls)

    def test_mpu_HEAD_last_version_part_num(self):
        req = swob.Request.blank('/bucket/mpu', method='HEAD', params={
            'partNumber': 5,
        }, headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()
        })
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['x-amz-meta-user-notes'], 'version2')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket'),
            ('HEAD', '/v1/AUTH_test/bucket/mpu?part-number=5'),
            ('HEAD', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?part-number=5' % (
                 ~utils.Timestamp(self.version_ids[2])).normal),
            ('GET', '/v1/AUTH_test/bucket/mpu?part-number=5'),
            ('GET', '/v1/AUTH_test/\x00versions\x00bucket/\x00mpu\x00%s'
             '?part-number=5' % (
                 ~utils.Timestamp(self.version_ids[2])).normal),
        ])


class TestVersionedMpuGETorHEADAcl(TestVersionedMpuGETorHEAD,
                                   S3ApiTestCaseAcl):

    def setUp(self):
        super(TestVersionedMpuGETorHEADAcl, self).setUp()
        object_headers = _gen_test_headers(
            self.default_owner, self.grants, 'object')
        for version_id in self.version_ids:
            # s3acl would add the default object ACL on PUT to each version
            version_path = '/v1/AUTH_test/\x00versions\x00bucket/' \
                '\x00mpu\x00%s' % (~utils.Timestamp(version_id)).normal
            self.swift.update_sticky_response_headers(
                version_path, object_headers)
        # this is used to flag insertion of expected HEAD pre-flight request of
        # object ACLs
        self.s3_acl = True
