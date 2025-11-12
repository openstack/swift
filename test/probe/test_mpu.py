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
from io import BytesIO
from tempfile import mkdtemp
import os.path
import shutil
import random

from swift.common.constraints import AUTO_CREATE_ACCOUNT_PREFIX
from swift.common.manager import Manager
from swift.common.middleware.mpu import MPUSessionHandler, \
    externalize_upload_id, internalize_upload_id
from swift.common.object_ref import ObjectRef
from swift.common.storage_policy import POLICIES
from swift.common.swob import normalize_etag
from swift.common.utils import quote, md5, MD5_OF_EMPTY_STRING, Timestamp, \
    readconf
from swift.container.auditor import ContainerAuditor
from swiftclient import client as swiftclient, ClientException

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES

from boto3.s3.transfer import TransferConfig
from test.s3api import get_s3_client

from unittest import mock


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
        super().setUp()
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
        super().tearDown()


class BaseTestS3MPU(BaseTestMPU):
    def setUp(self):
        super().setUp()
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
        super().setUp()
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
        super().setUp()
        mpu_info = self.cluster_info.get('mpu', {})
        if not mpu_info:
            raise unittest.SkipTest('mpu not enabled')

        self.internal_client = self.make_internal_client()
        self.mpu_internal_client = self.make_mpu_internal_client()
        self.hidden_account = AUTO_CREATE_ACCOUNT_PREFIX + self.account
        # TODO: revert to superclass uuid name
        self.mpu_name = 'tempname'
        self.sessions_container_name = \
            '\x00mpu_sessions\x00%s' % self.bucket_name
        self.parts_container_name = '\x00mpu_parts\x00%s' % self.bucket_name
        self.history_container_name = '\x00history\x00%s' % self.bucket_name
        self.versions_container_name = '\x00versions\x00%s' % self.bucket_name
        self.part_size = 5 * 2 ** 20

        swiftclient.put_container(self.url, self.token, self.bucket_name)
        self.maxDiff = None

    def make_mpu_internal_client(self):
        # make an internal client that has mpu in the pipeline; internal client
        # does not support allow_modify_pipeline so we have to hack the
        # pipeline conf before loading rather than mock the server's
        # required_filters for modification during loading.
        def insert_filter(pipeline, filter, after):
            if filter in pipeline:
                return
            index = 0
            for other in after:
                if (other in pipeline):
                    index = max(index, pipeline.index(other) + 1)
            pipeline.insert(index, filter)

        conf = readconf('/etc/swift/internal-client.conf')
        del conf['log_name']
        del conf['__file__']
        conf.setdefault('DEFAULT', {})
        conf['DEFAULT']['swift_dir'] = '/etc/swift'
        pipeline = conf['pipeline:main']['pipeline'].split(' ')

        # TODO: remove slo when no longer required
        conf.setdefault('filter:slo',
                        {'use': 'egg:swift#slo'})
        insert_filter(pipeline, 'slo',
                      ['copy', 'staticweb', 'tempauth', 'keystoneauth',
                       'catch_errors', 'gatekeeper', 'cache', 'proxy-logging'])

        conf.setdefault('filter:mpu',
                        {'use': 'egg:swift#mpu',
                         'log_name': 'internal-client-mpu',
                         'log_level': 'DEBUG'})
        insert_filter(pipeline, 'mpu',
                      ['copy', 'staticweb', 'tempauth', 'keystoneauth',
                       'catch_errors', 'gatekeeper', 'cache', 'proxy-logging'])

        conf.setdefault('filter:proxy-logging',
                        {'use': 'egg:swift#proxy_logging'})
        conf['filter:proxy-logging'].update(
            {'log_name': 'internal-client-proxy',
             'log_level': 'DEBUG'})
        insert_filter(pipeline, 'proxy-logging',
                      ['catch_errors', 'gatekeeper'])

        conf['pipeline:main']['pipeline'] = ' '.join(pipeline)
        print('internal client pipeline: %s'
              % conf['pipeline:main']['pipeline'])

        return self.make_custom_internal_client(conf=conf)

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

    def list_versions(self, container):
        resp, body = self.get_container(
            container, query_string='versions=true')
        return json.loads(body)

    def get_object_versions_from_listing(self, container,
                                         include_delete_markers=True):
        resp, body = self.get_container(
            container, query_string='versions=true')
        listing = json.loads(body)
        name_to_versions = collections.defaultdict(list)
        for item in listing:
            deleted = item['content_type'].startswith('application/x-deleted')
            if deleted and not include_delete_markers:
                continue
            name_to_versions[item['name']].append(
                (item['version_id'], deleted))
        return dict(name_to_versions)

    def internalize_upload_id(self, upload_id):
        # TODO: get signing key from conf file
        parsed, conn = swiftclient.http_connection(self.url)
        path = '%s/%s/%s' % (parsed.path,
                             quote(self.bucket_name),
                             quote(self.mpu_name))
        return internalize_upload_id(b'', path, upload_id)

    def externalize_upload_id(self, upload_id):
        # TODO: get signing key from conf file
        parsed, conn = swiftclient.http_connection(self.url)
        path = '%s/%s/%s' % (parsed.path,
                             quote(self.bucket_name),
                             quote(self.mpu_name))
        return externalize_upload_id(b'', path, upload_id)

    def _get_listing(self, account, container, include_state=None):
        if include_state:
            headers = {'X-Backend-Include-State': str(include_state)}
        else:
            headers = {}
        return list(self.internal_client.iter_objects(
            account, container, headers=headers))

    def get_versions(self, include_state=None):
        return self._get_listing(self.account,
                                 self.versions_container_name,
                                 include_state=include_state)

    def get_mpu_parts(self):
        return self._get_listing(self.account, self.parts_container_name)

    def get_mpu_sessions(self):
        return self._get_listing(
            self.hidden_account, self.sessions_container_name)

    def get_history(self, include_state=None):
        return self._get_listing(
            self.account, self.bucket_name, include_state=include_state)

    def get_mpu_resources(self):
        return (self.get_mpu_sessions(),
                self.get_mpu_parts(),
                self.get_history())

    def _make_mpu(self, num_parts=1):
        # create
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        upload_id = resp.headers.get('X-Upload-Id')
        self.assertIsNotNone(upload_id)
        self.assertEqual(202, resp.status)
        # upload parts
        manifest = []
        part_file, hash_, chunk_etags = self.make_file(self.part_size, 1)
        for i in range(num_parts):
            with open(part_file, 'rb') as fd:
                part_etag = swiftclient.put_object(
                    self.url, self.token, self.bucket_name, self.mpu_name,
                    contents=fd, content_type='ignored',
                    query_string='upload-id=%s&part-number=%d'
                                 % (upload_id, i + 1)
                )
            self.assertEqual(hash_, part_etag)
            manifest.append({'part_number': i + 1, 'etag': part_etag})
        # complete
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='upload-id=%s' % upload_id,
                                      body=json.dumps(manifest).encode(
                                          'ascii'))
        self.assertEqual(202, resp.status)
        self.assertNotIn('X-Static-Large-Object', resp.headers)
        body_dict = json.loads(body)
        self.assertEqual('201 Created', body_dict['Response Status'], body)
        mpu_etag = calc_s3mpu_etag(chunk_etags * num_parts)
        self.assert_etag(mpu_etag, body_dict.get('Etag'))
        # get mpu
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(num_parts * self.part_size),
                         headers.get('content-length'))
        self.assert_etag(mpu_etag, headers.get('etag'), rfc_compliant=True)
        return upload_id, mpu_etag


class TestNativeMPU(BaseTestNativeMPU):
    def test_native_mpu(self):
        swiftclient.put_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            contents='junk')
        orig_objs = list(self.internal_client.iter_objects(
            self.account, self.bucket_name))

        # check history
        sessions, parts, history = self.get_mpu_resources()
        history_refs = [ObjectRef.parse(v['name']) for v in history]
        self.assertEqual([self.mpu_name],
                         [v.user_name for v in history_refs])
        self.assertEqual([self.mpu_name],
                         [o['name'] for o in orig_objs])

        # create mpu
        resp, body = self.post_object(self.bucket_name, self.mpu_name,
                                      query_string='uploads=true')
        self.assertEqual(202, resp.status)
        self.assertIn('X-Upload-Id', resp.headers)
        ext_upload_id_str = resp.headers.get('X-Upload-Id')
        int_upload_id = self.internalize_upload_id(ext_upload_id_str)
        # sanity check ...
        self.assertEqual(ext_upload_id_str,
                         self.externalize_upload_id(int_upload_id))
        sess_name = ObjectRef(self.mpu_name, int_upload_id)
        containers = self.internal_client.iter_containers(self.account)
        # TODO: move parts container to hidden account
        self.assertEqual(sorted([self.parts_container_name,
                                 self.bucket_name]),
                         sorted([c['name'] for c in containers]))
        containers = self.internal_client.iter_containers(self.hidden_account)
        self.assertEqual(sorted([self.sessions_container_name,
                                 self.history_container_name]),
                         sorted([c['name'] for c in containers]))

        # list sessions internal
        sessions, parts, history = self.get_mpu_resources()
        self.assertEqual(
            [{'name': sess_name.serialize(),
              'content_type': 'application/x-mpu-session-created',
              'bytes': 0,
              'hash': MD5_OF_EMPTY_STRING,
              'last_modified': mock.ANY}],
            sessions)

        # list sessions via API
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name,
            query_string='uploads&limit=10')
        self.assertEqual(
            [(self.mpu_name, ext_upload_id_str)],
            [(o['name'], o['upload_id']) for o in listing])

        # upload part
        part_file, hash_, chunk_etags = self.make_file(self.part_size, 1)
        with open(part_file, 'rb') as fd:
            part_etag = swiftclient.put_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                contents=fd, content_type='ignored',
                query_string='upload-id=%s&part-number=1' % ext_upload_id_str)
        # TODO: check resp content-length header
        # swiftclient strips the "" from etag!
        self.assertEqual(hash_, part_etag)

        # list parts internal
        parts = list(self.internal_client.iter_objects(
            self.account, self.parts_container_name))
        part_base = '%s/%s/' % (self.mpu_name, int_upload_id)
        exp_parts_items = [
            {'name': part_base,
             'hash': mock.ANY,  # might be encrypted
             'bytes': 0,
             'content_type': mock.ANY,  # TODO: tighten assertion
             'last_modified': mock.ANY},
            {'name': part_base + '000001',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        self.assertEqual(exp_parts_items, parts)

        # list parts via mpu API
        resp_hdrs, resp_body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='upload-id=%s' % ext_upload_id_str
        )
        self.assertEqual(
            [{'name': '%s/%s/000001' % (self.mpu_name, ext_upload_id_str),
              'hash': part_etag,
              'bytes': self.part_size,
              'content_type': 'application/octet-stream',
              'last_modified': mock.ANY}],
            json.loads(resp_body))

        # complete
        manifest = [{'part_number': 1, 'etag': part_etag}]
        resp, body = self.post_object(
            self.bucket_name, self.mpu_name,
            query_string='upload-id=%s' % ext_upload_id_str,
            body=json.dumps(manifest).encode('ascii'))
        self.assertEqual(202, resp.status)
        self.assertNotIn('X-Static-Large-Object', resp.headers)
        body_dict = json.loads(body)
        self.assertEqual('201 Created', body_dict['Response Status'],
                         body_dict)

        # check mpu sysmeta
        mpu_meta = self.internal_client.get_object_metadata(
            self.account, self.bucket_name, self.mpu_name)
        self.assertEqual(int_upload_id.serialize(),
                         mpu_meta.get('x-object-sysmeta-mpu-upload-id'),
                         mpu_meta)

        # check user container listing
        resp_hdrs, user_objs = swiftclient.get_container(
            self.url, self.token, self.bucket_name)
        exp_etag = calc_s3mpu_etag(chunk_etags)
        self.assertEqual([exp_etag], [o['hash'] for o in user_objs])
        self.assertEqual([{'name': self.mpu_name,
                           'bytes': self.part_size,
                           'hash': exp_etag,
                           'content_type': 'application/octet-stream',
                           'last_modified': mock.ANY}],
                         user_objs)

        sessions, parts, history = self.get_mpu_resources()
        self.assertIn(self.mpu_name, sessions[0]['name'])
        self.assertEqual(exp_parts_items, parts)
        self.assertIn(self.mpu_name, history[0]['name'])

        # head mpu
        headers = swiftclient.head_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(self.part_size), headers.get('content-length'))
        self.assert_etag(calc_s3mpu_etag(chunk_etags), headers.get('etag'),
                         rfc_compliant=True)
        # download mpu
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(self.part_size), headers.get('content-length'))
        self.assert_etag(calc_s3mpu_etag(chunk_etags), headers.get('etag'),
                         rfc_compliant=True)

        # session still exists but is marked completed
        sessions = self.get_mpu_sessions()
        self.assertEqual(
            [{'name': sess_name.serialize(),
              'content_type': 'application/x-mpu-session-completed',
              'bytes': 0,
              'hash': MD5_OF_EMPTY_STRING,
              'last_modified': mock.ANY}],
            sessions)

        # list mpu sessions via API - empty list
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertFalse(listing)

        # list parts via mpu API -> 404
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='upload-id=%s' % ext_upload_id_str)
        self.assertEqual(404, cm.exception.http_status)

        # list history versions
        history = self.get_history()
        history_refs = [ObjectRef.parse(v['name']) for v in history]
        self.assertEqual([self.mpu_name],
                         [v.user_name for v in history_refs])
        # latest version is the mpu
        exp_history_ref = ObjectRef(self.mpu_name, reserved=False)
        self.assertEqual(exp_history_ref.serialize(),
                         history_refs[0].serialize())

        # list versions via api
        versions = self.list_versions(self.bucket_name)
        self.assertEqual(1, len(versions))
        self.assertEqual(self.mpu_name, versions[0]['name'])
        self.assertEqual('null', versions[0]['version_id'])

        # check account stats
        Manager(['container-updater']).once()
        account_hdrs, account_listing = swiftclient.get_account(
            self.url, self.token)
        # TODO: move parts to hidden account but add bytes to user account
        self.assertEqual('2', account_hdrs.get('X-Account-Container-Count'))
        # user + part + lifeline
        self.assertEqual('3', account_hdrs.get('X-Account-Object-Count'))
        # account only reports the sum of part size, not manifest
        # TODO: fix - we're currently double counting parts bytes
        # self.assertEqual(str(self.part_size),
        #                  account_hdrs.get('X-Account-Bytes-Used'))

        # delete the mpu
        swiftclient.delete_object(self.url, self.token, self.bucket_name,
                                  self.mpu_name)

        # history is empty...
        self.assertFalse(self.get_history())
        # ...except for a tombstone and a relic
        history = self.get_history(include_state=1)
        self.assertEqual([exp_history_ref.serialize()],
                         [item['name'] for item in history])
        exp_relic_ref = '%s\x00%s' % (self.mpu_name, int_upload_id)
        history = self.get_history(include_state=2)
        self.assertEqual([exp_relic_ref],
                         [item['name'] for item in history])

        # check the mpu cannot be downloaded
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(404, cm.exception.http_status)

        # check we still have the parts
        self.assertEqual(exp_parts_items, self.get_mpu_parts())

        # async cleanup: once to process sessions...
        Manager(['container-auditor']).once()
        # ...once more to process any parts markers...
        Manager(['container-auditor']).once()

        # session, parts and history have gone :)
        self.assertEqual(([], [], []), self.get_mpu_resources())

        # check the account stats
        Manager(['container-updater']).once()
        account_hdrs, account_listing = swiftclient.get_account(
            self.url, self.token)
        self.assertEqual('2', account_hdrs.get('X-Account-Container-Count'))
        self.assertEqual('0', account_hdrs.get('X-Account-Object-Count'))
        self.assertEqual('0', account_hdrs.get('X-Account-Bytes-Used'))

    def test_mpu_overwritten_by_mpu(self):
        upload_id, etag = self._make_mpu(num_parts=1)
        int_upload_id = self.internalize_upload_id(upload_id)
        part_base = '%s/%s/' % (self.mpu_name, int_upload_id)
        exp_parts_items_1 = [
            {'name': part_base,
             'hash': mock.ANY,  # might be encrypted
             'bytes': 0,
             'content_type': mock.ANY,  # TODO: tighten assertion
             'last_modified': mock.ANY},
            {'name': part_base + '000001',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        # overwrite with another mpu
        upload_id, etag = self._make_mpu(num_parts=2)
        int_upload_id = self.internalize_upload_id(upload_id)
        part_base = '%s/%s/' % (self.mpu_name, int_upload_id)
        exp_parts_items_2 = [
            {'name': part_base,
             'hash': mock.ANY,  # might be encrypted
             'bytes': 0,
             'content_type': mock.ANY,  # TODO: tighten assertion
             'last_modified': mock.ANY},
            {'name': part_base + '000001',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY},
            {'name': part_base + '000002',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]

        # check user container listing
        resp_hdrs, user_objs = swiftclient.get_container(
            self.url, self.token, self.bucket_name)
        self.assertEqual([{'name': self.mpu_name,
                           'bytes': 2 * self.part_size,
                           'hash': etag,
                           'content_type': 'application/octet-stream',
                           'last_modified': mock.ANY}],
                         user_objs)

        # get user object
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(2 * self.part_size),
                         headers.get('content-length'))
        self.assert_etag(etag, headers.get('etag'), rfc_compliant=True)
        self.assertEqual(2 * self.part_size, len(body))

        # list history versions
        history = self.get_history()
        history_refs = [ObjectRef.parse(v['name']) for v in history]
        self.assertEqual([self.mpu_name],
                         [v.user_name for v in history_refs])

        # check we still have the parts for both mpu
        parts = list(self.internal_client.iter_objects(
            self.account, self.parts_container_name))
        self.assertEqual(exp_parts_items_1 + exp_parts_items_2, parts)

        # async cleanup: once for sessions and history, once for parts markers
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        # sessions and obsolete parts have gone :)
        sessions, parts, history = self.get_mpu_resources()
        self.assertFalse(sessions)
        self.assertEqual(exp_parts_items_2, parts)
        self.assertEqual(1, len(history))

        # get user object
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual(str(2 * self.part_size),
                         headers.get('content-length'))
        self.assert_etag(etag, headers.get('etag'), rfc_compliant=True)
        self.assertEqual(2 * self.part_size, len(body))

    def test_mpu_overwritten_by_non_mpu(self):
        upload_id, etag = self._make_mpu(num_parts=1)
        int_upload_id = self.internalize_upload_id(upload_id)
        part_base = '%s/%s/' % (self.mpu_name, int_upload_id)
        exp_parts_items_1 = [
            {'name': part_base,
             'hash': mock.ANY,  # might be encrypted
             'bytes': 0,
             'content_type': mock.ANY,  # TODO: tighten assertion
             'last_modified': mock.ANY},
            {'name': part_base + '000001',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        # overwrite with empty object
        swiftclient.put_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            content_type='text/plain')

        # check user container listing
        resp_hdrs, user_objs = swiftclient.get_container(
            self.url, self.token, self.bucket_name)
        self.assertEqual([{'name': self.mpu_name,
                           'bytes': 0,
                           'hash': MD5_OF_EMPTY_STRING,
                           'content_type': 'text/plain',
                           'last_modified': mock.ANY}],
                         user_objs)

        # get user object
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual('0', headers.get('content-length'))
        self.assert_etag(MD5_OF_EMPTY_STRING, headers.get('etag'))
        self.assertEqual(b'', body)

        # list history versions
        history = self.get_history()
        history_refs = [ObjectRef.parse(v['name']) for v in history]
        self.assertEqual([self.mpu_name],
                         [v.user_name for v in history_refs])

        # check we still have the parts for mpu
        parts = list(self.internal_client.iter_objects(
            self.account, self.parts_container_name))
        self.assertEqual(exp_parts_items_1, parts)

        # async cleanup: once for sessions and history, once for parts markers
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        # sessions and obsolete parts have gone :)
        sessions, parts, history = self.get_mpu_resources()
        self.assertFalse(sessions)
        self.assertFalse(parts)
        self.assertEqual(1, len(history))

        # get user object
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual('0', headers.get('content-length'))
        self.assert_etag(MD5_OF_EMPTY_STRING, headers.get('etag'))
        self.assertEqual(b'', body)

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
        int_upload_id = self.internalize_upload_id(upload_id)
        part_base = '%s/%s/' % (self.mpu_name, int_upload_id)
        exp_parts_items = [
            {'name': part_base,
             'hash': mock.ANY,  # might be encrypted
             'bytes': 0,
             'content_type': mock.ANY,  # TODO: tighten assertion
             'last_modified': mock.ANY},
            {'name': part_base + '000001',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        parts = list(self.internal_client.iter_objects(
            self.account, self.parts_container_name))
        self.assertEqual(exp_parts_items, parts)

        # session still exists but is marked aborted; session are named in
        # chronological order
        mpu_ref = ObjectRef(self.mpu_name, int_upload_id)
        exp_aborted_sessions = [
            {'name': '%s' % mpu_ref.serialize(),
             'content_type': 'application/x-mpu-session-aborted',
             'bytes': 0,
             'hash': MD5_OF_EMPTY_STRING,
             'last_modified': mock.ANY}
        ]
        sessions = self.internal_client.iter_objects(
            self.hidden_account, self.sessions_container_name)
        self.assertEqual(exp_aborted_sessions, list(sessions))

        # immediate audit(s) will pass over the aborted session
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        # ... so session still exists
        sessions, parts, history = self.get_mpu_resources()
        self.assertEqual(exp_aborted_sessions, list(sessions))
        # and the parts still exist
        self.assertEqual(exp_parts_items, [part for part in parts])

        # a custom audit with zero purge delay will clean up the session
        custom_conf = {'mpu_aborted_purge_delay': '0'}
        for conf_index in self.configs['container-auditor'].keys():
            self.run_custom_daemon(
                ContainerAuditor, 'container-auditor',
                conf_index, custom_conf=custom_conf)

        # now the session is gone
        sessions, parts, history = self.get_mpu_resources()
        self.assertFalse(list(sessions))

        # async cleanup to ensure generated parts markers are processed
        Manager(['container-auditor']).once()

        # parts have gone :)
        sessions, parts, history = self.get_mpu_resources()
        self.assertEqual(([], []), (sessions, parts))

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
            downloads.append(swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name))
            if completes == 0:
                obj_brain.start_handoff_half()
            else:
                obj_brain.start_primary_half()
            return result

        def mock_post_session_completed(*args, **kwargs):
            nonlocal completes
            completes += 1
            if completes == 1:
                # send another completeUpload before the first updates the
                # session object, with a different manifest
                manifest = [{'part_number': 2, 'etag': part_etag_2}]
                body = json.dumps(manifest).encode('ascii')
                resp = self.mpu_internal_client.make_request(
                    'POST', mpu_path,
                    headers={'Content-Length': str(len(body))},
                    acceptable_statuses=[202],
                    body_file=BytesIO(body),
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
            body = json.dumps(manifest).encode('ascii')
            resp = self.mpu_internal_client.make_request(
                'POST', mpu_path,
                headers={'Content-Length': str(len(body))},
                acceptable_statuses=[202],
                body_file=BytesIO(body),
                params={'upload-id': upload_id})
            body_dict = json.loads(resp.body)
            self.assertEqual('201 Created', body_dict['Response Status'],
                             body_dict)

        # check the downloads...
        exp_etags = ['"%s"' % calc_s3mpu_etag(chunk_etags_1),
                     '"%s"' % calc_s3mpu_etag(chunk_etags_2)]
        self.assertEqual(
            [str(self.part_size), '99'],
            [headers.get('content-length') for headers, body in downloads],
            [headers for headers, body in downloads])
        self.assert_etag(calc_s3mpu_etag(chunk_etags_1),
                         downloads[0][0].get('etag'), rfc_compliant=True)
        self.assert_etag(calc_s3mpu_etag(chunk_etags_2),
                         downloads[1][0].get('etag'), rfc_compliant=True)

        # list mpus to check the session is completed
        resp_hdrs, listing = swiftclient.get_container(
            self.url, self.token, self.bucket_name, query_string='uploads')
        self.assertFalse(listing)

        # run daemons to check that the mpu parts are *not* cleaned up
        Manager(['object-replicator']).once()
        Manager(['object-replicator']).once()
        Manager(['container-auditor']).once()
        Manager(['container-auditor']).once()

        sessions, parts, history = self.get_mpu_resources()
        self.assertFalse(sessions)
        int_upload_id = self.internalize_upload_id(upload_id)
        part_base = '%s/%s/' % (self.mpu_name, int_upload_id)
        exp_parts_items = [
            {'name': part_base,
             'hash': mock.ANY,  # might be encrypted
             'bytes': 0,
             'content_type': mock.ANY,  # TODO: tighten assertion
             'last_modified': mock.ANY},
            {'name': part_base + '000001',
             'hash': mock.ANY,  # might be encrypted
             'bytes': self.part_size,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY},
            {'name': part_base + '000002',
             'hash': mock.ANY,  # might be encrypted
             'bytes': 99,
             'content_type': 'application/octet-stream',
             'last_modified': mock.ANY}
        ]
        self.assertEqual(exp_parts_items, parts)

        # check mpu is still intact
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name)
        self.assertEqual('99', headers.get('content-length'))
        self.assert_etag(exp_etags[1], headers.get('etag'), rfc_compliant=True)


class TestNativeMPUUTF8(TestNativeMPU):
    def setUp(self):
        super().setUp()
        self.mpu_name = self.mpu_name + '\N{SNOWMAN}'


class TestNativeMPUWithS3CompatVersioning(BaseTestNativeMPU):
    def enable_versioning(self):
        headers = {'x-s3-compatible-versions': 'true',
                   'x-versions-enabled': 'true'}
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers=headers)

    def disable_versioning(self):
        headers = {'x-versions-enabled': 'false'}
        swiftclient.post_container(self.url, self.token, self.bucket_name,
                                   headers=headers)

    def _do_test_delete_version_while_versioning_enabled(
            self, obj_name, mpu_name):
        # verify that mpu behaves same as a plain object w.r.t. versioning
        #   * DELETE objects - a version is retained
        #   * DELETE object version - nothing retained
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name)
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({obj_name: [(mock.ANY, True), (mock.ANY, False)],
                          mpu_name: [(mock.ANY, True), (mock.ANY, False)]},
                         obj_versions)

        # run auditor - nothing should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()

        # previous object versions still exist
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, obj_name,
            query_string='version-id=%s'
                         % quote(obj_versions[obj_name][1][0]))
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, mpu_name,
            query_string='version-id=%s'
                         % quote(obj_versions[mpu_name][1][0]))

        # delete oldest version of each object
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name,
            query_string='version-id=%s'
                         % quote(obj_versions[obj_name][1][0]))
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, mpu_name,
            query_string='version-id=%s'
                         % quote(obj_versions[mpu_name][1][0]))

        obj_versions2 = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({obj_name: obj_versions[obj_name][:1],
                          mpu_name: obj_versions[mpu_name][:1]},
                         obj_versions2)

        # objects cannot be read
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, obj_name,
                query_string='version-id=%s'
                             % quote(obj_versions[obj_name][1][0]))
        self.assertEqual(404, cm.exception.http_status)
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, mpu_name,
                query_string='version-id=%s'
                             % quote(obj_versions[mpu_name][1][0]))
        self.assertEqual(404, cm.exception.http_status)

        sessions, parts, history = self.get_mpu_resources()
        self.assertFalse(sessions)
        # part + lifeline
        self.assertEqual(2, len(parts), parts)
        # XXX
        # self.assertFalse(history)

        # run auditor
        for i in range(2):
            Manager(['container-auditor']).once()

        sessions, parts, history = self.get_mpu_resources()
        self.assertEqual(([], []), (sessions, parts))

    def test_delete_retained_version_while_versioning_enabled(self):
        # enable versioning
        self.enable_versioning()
        # put an mpu
        self._make_mpu()
        # put an object as a control item
        obj_name = 'non-mpu-obj'
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               obj_name)
        # check delete behavior
        self._do_test_delete_version_while_versioning_enabled(
            obj_name, self.mpu_name)

    def test_delete_null_version_while_versioning_enabled(self):
        # put an mpu
        self._make_mpu()
        # put an object as a control item
        obj_name = 'non-mpu-obj'
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               obj_name)
        # enable versioning
        self.enable_versioning()
        # check delete behavior
        self._do_test_delete_version_while_versioning_enabled(
            obj_name, self.mpu_name)

    def test_no_overwrites_during_versioning(self):
        # verify that mpu behaves same as a plain object w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * disable versioning
        #   * DELETE objects - no version is retained, delete markers written

        # put an mpu
        self._make_mpu()

        # put an object as a control item
        obj_name = 'non-mpu-obj'
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               obj_name)

        # enable versioning
        self.enable_versioning()
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [('null', False)],
                          obj_name: [('null', False)]},
                         obj_versions)
        # disable versioning
        self.disable_versioning()

        # delete both objects
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, obj_name)
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name,)
        self.assertEqual({self.mpu_name: [('null', True)],
                          obj_name: [('null', True)]},
                         obj_versions)

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

        sessions, parts, history = self.get_mpu_resources()
        self.assertEqual(([], []), (sessions, parts))

    def test_overwrite_during_and_delete_after_versioning(self):
        # verify that mpu behaves same as a plain object w.r.t. versioning
        #   * PUT objects
        #   * enable versioning
        #   * overwrite objects
        #   * disable versioning
        #   * DELETE objects - 1 version plus null version delete marker
        brain = BrainSplitter(self.url, self.token, self.bucket_name,
                              self.mpu_name, server_type='object',
                              policy=self.policy)

        # put an mpu
        upload_id, etag = self._make_mpu(num_parts=2)
        upload_id = self.internalize_upload_id(upload_id)

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
        self.enable_versioning()

        # overwrite the mpu with a non-mpu
        brain.stop_handoff_half()
        swiftclient.put_object(self.url, self.token, self.bucket_name,
                               self.mpu_name, contents='version two')
        brain.start_handoff_half()

        # disable versioning
        self.disable_versioning()

        # get listing with versions
        Manager(['container-replicator']).once()
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [(mock.ANY, False), ('null', False)]},
                         obj_versions)
        vers = obj_versions[self.mpu_name][0][0]

        # run the auditor - nothing to clean up
        for i in range(2):
            Manager(['container-auditor']).once()
        exp_parts = [
            '%s/%s/%s' % (self.mpu_name, upload_id, tail)
            for tail in ('', '000001', '000002')
        ]
        self.assertEqual(sorted(exp_parts),
                         sorted([p['name'] for p in self.get_mpu_parts()]))

        # both versions exist
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=null')
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % quote(vers))

        # delete the obj - null version delete marker should be written
        swiftclient.delete_object(
            self.url, self.token, self.bucket_name, self.mpu_name)

        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [('null', True), (vers, False)]},
                         obj_versions)

        # retained version still exists
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % quote(vers))
        # but not the deleted null version
        with self.assertRaises(ClientException) as cm:
            swiftclient.get_object(
                self.url, self.token, self.bucket_name, self.mpu_name,
                query_string='version-id=null')
        self.assertEqual(404, cm.exception.http_status)

        # run auditor - original null version parts should be cleaned up
        for i in range(2):
            Manager(['container-auditor']).once()
        self.assertFalse(self.get_mpu_parts())

        # retained version still exists
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % quote(vers))

    def test_null_version_overwritten_while_versioning_suspended(self):
        #   * PUT mpu (null version)
        #   * enable versioning
        #   * PUT mpu (retained version)
        #   * disable versioning
        #   * PUT mpu (new null version)

        # put an mpu
        upload_id, etag1 = self._make_mpu(num_parts=1)
        upload_id1 = self.internalize_upload_id(upload_id)
        # enable versioning
        self.enable_versioning()
        # put an mpu again
        upload_id, etag2 = self._make_mpu(num_parts=2)
        upload_id2 = self.internalize_upload_id(upload_id)
        self.assertNotEqual(upload_id1, upload_id2)
        self.assertNotEqual(etag1, etag2)
        # run auditor - nothing should be cleaned up
        exp_parts1 = [
            '%s/%s/%s' % (self.mpu_name, upload_id1, tail)
            for tail in ('', '000001')
        ]
        exp_parts2 = [
            '%s/%s/%s' % (self.mpu_name, upload_id2, tail)
            for tail in ('', '000001', '000002')
        ]
        self.assertEqual(sorted(exp_parts1 + exp_parts2),
                         sorted([p['name'] for p in self.get_mpu_parts()]))
        for i in range(2):
            Manager(['container-auditor']).once()
        self.assertEqual(sorted(exp_parts1 + exp_parts2),
                         sorted([p['name'] for p in self.get_mpu_parts()]))
        # get listing with versions
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [(mock.ANY, False),
                                          ('null', False)]},
                         obj_versions)
        vers_1 = obj_versions[self.mpu_name][0][0]
        # objects exist
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=null')
        swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % quote(vers_1))

        # suspend versioning
        self.disable_versioning()
        # put an mpu again
        upload_id, etag3 = self._make_mpu(num_parts=3)
        upload_id3 = self.internalize_upload_id(upload_id)
        self.assertNotEqual(upload_id2, upload_id3)
        self.assertNotEqual(etag2, etag3)
        exp_parts3 = [
            '%s/%s/%s' % (self.mpu_name, upload_id3, tail)
            for tail in ('', '000001', '000002', '000003')
        ]
        # the earlier null version is not listed
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [('null', False),
                                          (vers_1, False)]},
                         obj_versions)
        # run auditor and the parts for the earlier null version are cleaned up
        self.assertEqual(sorted(exp_parts1 + exp_parts2 + exp_parts3),
                         sorted([p['name'] for p in self.get_mpu_parts()]))
        for i in range(2):
            Manager(['container-auditor']).once()
        self.assertEqual(sorted(exp_parts2 + exp_parts3),
                         sorted([p['name'] for p in self.get_mpu_parts()]))

        # we can get the latest null version
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=null')
        self.assert_etag(etag3, headers.get('etag'))
        # and the retained version
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=%s' % quote(vers_1))
        self.assert_etag(etag2, headers.get('etag'))

        # rinse and repeat...
        self.enable_versioning()
        upload_id, etag4 = self._make_mpu(num_parts=4)
        upload_id4 = self.internalize_upload_id(upload_id)
        self.assertNotEqual(upload_id3, upload_id4)
        self.assertNotEqual(etag3, etag4)
        exp_parts4 = [
            '%s/%s/%s' % (self.mpu_name, upload_id4, tail)
            for tail in ('', '000001', '000002', '000003', '000004')
        ]
        # the null version is replaced by the latest null version
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [(mock.ANY, False),
                                          ('null', False),
                                          (vers_1, False)]},
                         obj_versions)
        vers_2 = obj_versions[self.mpu_name][0][0]
        # nothing to clean up...
        self.assertEqual(sorted(exp_parts2 + exp_parts3 + exp_parts4),
                         sorted([p['name'] for p in self.get_mpu_parts()]))
        for i in range(2):
            Manager(['container-auditor']).once()
        self.assertEqual(sorted(exp_parts2 + exp_parts3 + exp_parts4),
                         sorted([p['name'] for p in self.get_mpu_parts()]))
        headers, body = swiftclient.get_object(
            self.url, self.token, self.bucket_name, self.mpu_name,
            query_string='version-id=null')
        self.assert_etag(etag3, headers.get('etag'))

        self.disable_versioning()
        upload_id, etag5 = self._make_mpu(num_parts=5)
        upload_id5 = self.internalize_upload_id(upload_id)
        self.assertNotEqual(upload_id4, upload_id5)
        self.assertNotEqual(etag4, etag5)
        exp_parts5 = [
            '%s/%s/%s' % (self.mpu_name, upload_id5, tail)
            for tail in ('', '000001', '000002', '000003', '000004', '000005')
        ]
        # the previous null version is replaced by the latest null version
        obj_versions = self.get_object_versions_from_listing(self.bucket_name)
        self.assertEqual({self.mpu_name: [('null', False),
                                          (vers_2, False),
                                          (vers_1, False)]},
                         obj_versions)
        # the previous null version parts are cleaned up...
        self.assertEqual(
            sorted(exp_parts2 + exp_parts3 + exp_parts4 + exp_parts5),
            sorted([p['name'] for p in self.get_mpu_parts()]))
        for i in range(2):
            Manager(['container-auditor']).once()
        self.assertEqual(
            sorted(exp_parts2 + exp_parts4 + exp_parts5),
            sorted([p['name'] for p in self.get_mpu_parts()]))
