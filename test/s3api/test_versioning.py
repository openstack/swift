# Copyright (c) 2019 SwiftStack, Inc.
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

import time
from collections import defaultdict
from unittest import mock

from botocore.exceptions import ClientError
import io

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import md5
from test.s3api import BaseS3TestCase


def retry(f, timeout=10):
    timelimit = time.time() + timeout
    while True:
        try:
            return f()
        except (ClientError, AssertionError):
            if time.time() > timelimit:
                raise
            continue


class TestObjectVersioning(BaseS3TestCase):

    maxDiff = None

    def _sanitize_obj_listing(self, obj):
        # there's some object listing parameters that are not deterministic
        obj.pop('LastModified')
        obj.pop('Owner', None)
        # there's some object listing parameters that Swift doesn't return,
        obj.pop('ChecksumAlgorithm', None)
        obj.pop('ChecksumType', None)

    def enable_versioning(self):
        resp = self.client.put_bucket_versioning(
            Bucket=self.bucket_name,
            VersioningConfiguration={'Status': 'Enabled'})
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        resp = self.get_versioning_status()
        self.assertEqual('Enabled', resp.get('Status'), resp)

    def disable_versioning(self):
        resp = self.client.put_bucket_versioning(
            Bucket=self.bucket_name,
            VersioningConfiguration={'Status': 'Suspended'})
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        resp = self.get_versioning_status()
        self.assertEqual('Suspended', resp.get('Status'), resp)

    def get_versioning_status(self):
        resp = self.client.get_bucket_versioning(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        return resp

    def get_version_ids(self, obj_name):
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        versions = resp.get('Versions', [])
        version_ids = [version['VersionId']
                       for version in versions
                       if version['Key'] == obj_name]
        versions = resp.get('DeleteMarkers', [])
        marker_ids = [version['VersionId']
                      for version in versions
                      if version['Key'] == obj_name]
        return version_ids, marker_ids

    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('versioning')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        resp = retry(self.get_versioning_status)
        self.assertNotIn('Status', resp)

    def tearDown(self):
        retry(self.disable_versioning)
        self.clear_bucket(self.client, self.bucket_name)
        super(TestObjectVersioning, self).tearDown()

    def assert_no_such_key(self, bucket_name, obj_name):
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=bucket_name, Key=obj_name)
        expected_err = 'An error occurred (NoSuchKey) when calling the ' \
            'GetObject operation: The specified key does not exist.'
        self.assertEqual(expected_err, str(caught.exception))

    def assert_no_such_version(self, bucket_name, obj_name, version_id):
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=bucket_name, Key=obj_name,
                                   VersionId=version_id)
        expected_err = 'An error occurred (NoSuchVersion) when calling the ' \
            'GetObject operation: The specified version does not exist.'
        self.assertEqual(expected_err, str(caught.exception))

    def test_setup(self):
        bucket_name = self.create_name('new-bucket')
        resp = self.client.create_bucket(Bucket=bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        expected_location = '/%s' % bucket_name
        self.assertEqual(expected_location, resp['Location'])
        headers = HeaderKeyDict(resp['ResponseMetadata']['HTTPHeaders'])
        self.assertEqual('0', headers['content-length'])
        self.assertEqual(expected_location, headers['location'])

        # get versioning
        resp = self.client.get_bucket_versioning(Bucket=bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('Status', resp)

        # put versioning
        versioning_config = {
            'Status': 'Enabled',
        }
        resp = self.client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration=versioning_config)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # ... now it's enabled
        def check_status():
            resp = self.client.get_bucket_versioning(Bucket=bucket_name)
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            try:
                self.assertEqual('Enabled', resp['Status'])
            except KeyError:
                self.fail('Status was not in %r' % resp)
        retry(check_status)

        # send over some bogus junk
        versioning_config['Status'] = 'Disabled'
        with self.assertRaises(ClientError) as ctx:
            self.client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration=versioning_config)
        expected_err = 'An error occurred (MalformedXML) when calling the ' \
            'PutBucketVersioning operation: The XML you provided was ' \
            'not well-formed or did not validate against our published schema'
        self.assertEqual(expected_err, str(ctx.exception))

        # disable it
        versioning_config['Status'] = 'Suspended'
        resp = self.client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration=versioning_config)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # ... now it's disabled again
        def check_status():
            resp = self.client.get_bucket_versioning(Bucket=bucket_name)
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            self.assertEqual('Suspended', resp['Status'])
        retry(check_status)

    def test_no_overwrite_while_versioning_enabled(self):
        # verify that the null version prior to versioning being enabled will
        # *not* become a version if it is not overwritten while versioning is
        # enabled.
        obj_name = self.create_name('unversioned-obj')
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)

        self.enable_versioning()
        resp = self.get_versioning_status()
        self.assertEqual('Enabled', resp.get('Status'), resp)

        self.disable_versioning()
        resp = self.get_versioning_status()
        self.assertEqual('Suspended', resp.get('Status'), resp)

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        self.assertEqual(1, len(objs))
        self.assertEqual(obj_name, objs[0]['Key'])

        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        self.assertNotIn('Versions', resp)
        markers = resp.get('DeleteMarkers', [])
        self.assertEqual(1, len(markers))
        self.assertEqual(obj_name, markers[0]['Key'])

    def test_null_versions_replaced(self):
        # verify that there is only ever one null version retained
        obj_name = self.create_name('versioned-obj')
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)

        # there's a 'null' version even before versioning has been enabled
        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(['null'], version_ids)
        self.assertFalse(marker_ids)

        self.enable_versioning()
        # put version
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)
        self.disable_versioning()

        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(2, len(version_ids))
        vers0 = version_ids[0]
        self.assertEqual([vers0, 'null'], version_ids, version_ids)
        self.assertFalse(marker_ids)

        # put null version
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)

        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(['null', vers0], version_ids)
        self.assertFalse(marker_ids)

        # delete
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # null version gone...
        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual([vers0], version_ids)
        self.assertEqual(['null'], marker_ids)

        # put null version
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)
        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertFalse(marker_ids)
        self.assertEqual(['null', vers0], version_ids)

        self.enable_versioning()
        # put version
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)
        self.disable_versioning()

        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertFalse(marker_ids)
        self.assertEqual(3, len(version_ids), version_ids)
        vers1 = version_ids[0]
        self.assertEqual([vers1, 'null', vers0], version_ids)

        # put null version
        self.client.upload_fileobj(io.BytesIO(b'some-data'),
                                   self.bucket_name, obj_name)
        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertFalse(marker_ids)
        self.assertEqual(['null', vers1, vers0], version_ids)

    def test_null_version_listing(self):
        # verify that null version is positioned in listing according to its
        # created time
        obj_name = self.create_name('versioned-obj')
        self.enable_versioning()
        # put version
        self.client.upload_fileobj(io.BytesIO(b'retained-version'),
                                   self.bucket_name, obj_name)
        version_ids_0, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(1, len(version_ids_0), version_ids_0)
        self.assertFalse(marker_ids)

        self.disable_versioning()
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)
        version_ids_1, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(2, len(version_ids_1), version_ids_1)
        self.assertFalse(marker_ids)
        # null version is first in listing
        self.assertEqual(version_ids_0, version_ids_1[1:])

        self.enable_versioning()
        # put version
        self.client.upload_fileobj(io.BytesIO(b'retained-version'),
                                   self.bucket_name, obj_name)
        version_ids_2, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(3, len(version_ids_2), version_ids_2)
        self.assertFalse(marker_ids)
        # null version is middle of listing
        self.assertEqual(version_ids_1, version_ids_2[1:])

        self.disable_versioning()
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)
        version_ids_3, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(3, len(version_ids_3), version_ids_3)
        self.assertFalse(marker_ids)
        # null version is first in listing
        self.assertEqual([version_ids_2[0], version_ids_2[2]],
                         version_ids_3[1:])

    def _do_test_null_version_is_latest_delete(self, obj_name):
        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertEqual(['null'], version_ids)
        self.assertFalse(marker_ids)

        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name,
                                      VersionId='null')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # delete the null version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId='null')
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        version_ids, marker_ids = self.get_version_ids(obj_name)
        self.assertFalse(version_ids)
        self.assertFalse(marker_ids)
        self.assert_no_such_version(self.bucket_name, obj_name, 'null')

    def test_null_version_is_latest_delete_before_versioning_enabled(self):
        obj_name = self.create_name('versioned-obj')
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)

        self._do_test_null_version_is_latest_delete(obj_name)

    def test_null_version_is_latest_delete_while_versioning_enabled(self):
        obj_name = self.create_name('versioned-obj')
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)

        self.enable_versioning()

        self._do_test_null_version_is_latest_delete(obj_name)

    def test_null_version_is_latest_delete_while_versioning_suspended(self):
        obj_name = self.create_name('versioned-obj')
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)

        self.enable_versioning()
        self.disable_versioning()

        self._do_test_null_version_is_latest_delete(obj_name)

    def test_null_version_delete_while_versioning_enabled(self):
        obj_name = self.create_name('versioned-obj')
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)

        self.enable_versioning()
        # get the null version
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name,
                                      VersionId='null')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        # put version
        self.client.upload_fileobj(io.BytesIO(b'retained-version'),
                                   self.bucket_name, obj_name)
        version_ids_0, marker_ids_0 = self.get_version_ids(obj_name)
        self.assertEqual(2, len(version_ids_0), version_ids_0)
        self.assertFalse(marker_ids_0)
        vers1, null_vers1 = version_ids_0

        # get the null version
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name,
                                      VersionId='null')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # delete the null version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=null_vers1)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        version_ids_1, marker_ids_1 = self.get_version_ids(obj_name)
        self.assertEqual(version_ids_0[:1], version_ids_1)

        # check the latest version is intact
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        exp_etag = md5(b'retained-version', usedforsecurity=False).hexdigest()
        self.assertEqual('"%s"' % exp_etag, resp['ETag'])

    def test_null_version_delete_while_versioning_suspended(self):
        obj_name = self.create_name('versioned-obj')
        # put null version
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)

        self.enable_versioning()
        # put version
        self.client.upload_fileobj(io.BytesIO(b'retained-version'),
                                   self.bucket_name, obj_name)
        version_ids_0, marker_ids_0 = self.get_version_ids(obj_name)
        self.assertEqual(2, len(version_ids_0), version_ids_0)
        self.assertFalse(marker_ids_0)
        vers1, null_vers1 = version_ids_0

        self.disable_versioning()
        # delete the null version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=null_vers1)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        version_ids_1, marker_ids_1 = self.get_version_ids(obj_name)
        self.assertEqual(version_ids_0[:1], version_ids_1)

        # check the latest version is intact
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        exp_etag = md5(b'retained-version', usedforsecurity=False).hexdigest()
        self.assertEqual('"%s"' % exp_etag, resp['ETag'])

    def test_null_version_get(self):
        # verify that null version is always valid
        obj_name = self.create_name('versioned-obj')

        # before object exists...
        self.assert_no_such_version(self.bucket_name, obj_name, 'null')

        # before versioning is enabled...
        self.client.upload_fileobj(io.BytesIO(b'null-version'),
                                   self.bucket_name, obj_name)
        self.assertEqual((['null'], []), self.get_version_ids(obj_name))
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name,
                                      VersionId='null')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # while versioning is enabled...
        self.enable_versioning()
        self.assertEqual((['null'], []), self.get_version_ids(obj_name))
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name,
                                      VersionId='null')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # while versioning is suspended...
        self.disable_versioning()
        self.assertEqual((['null'], []), self.get_version_ids(obj_name))
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name,
                                      VersionId='null')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_upload_fileobj_versioned(self):
        retry(self.enable_versioning)
        obj_data = self.create_name('some-data').encode('ascii')
        obj_etag = md5(obj_data, usedforsecurity=False).hexdigest()
        obj_name = self.create_name('versioned-obj')
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        # object is in the listing
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % obj_etag,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # object version listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
            obj.pop('VersionId')
        self.assertEqual([{
            'ETag': '"%s"' % obj_etag,
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # overwrite the object
        new_obj_data = self.create_name('some-new-data').encode('ascii')
        new_obj_etag = md5(new_obj_data, usedforsecurity=False).hexdigest()
        self.client.upload_fileobj(io.BytesIO(new_obj_data),
                                   self.bucket_name, obj_name)

        # new object is in the listing
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % new_obj_etag,
            'Key': obj_name,
            'Size': len(new_obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # both object versions in the versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
            obj.pop('VersionId')
        self.assertEqual([{
            'ETag': '"%s"' % new_obj_etag,
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(new_obj_data),
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"%s"' % obj_etag,
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

    def _do_test_delete_versioned_objects(self, obj_name, obj_data, etags):
        # only one object appears in the listing
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # ...and that's the object that a plain GET will return
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # but everything is layed out in the object versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            self._sanitize_obj_listing(obj)
            versions.append(obj.pop('VersionId'))
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"%s"' % etags[1],
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"%s"' % etags[2],
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)
        self.assertFalse(resp.get('DeleteMarkers'))

        # we can delete a specific version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[1])

        # and that just pulls it out of the versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
            'VersionId': versions[0],
        }, {
            'ETag': '"%s"' % etags[2],
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
            'VersionId': versions[2],
        }], objs)
        self.assertFalse(resp.get('DeleteMarkers'))

        # ... but the current listing is unaffected
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # ...and that's still the object that a plain GET will return
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # OTOH, if you delete specifically the latest version
        # we can delete a specific version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[0])
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # the versions listing has a new IsLatest
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[2],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
            'VersionId': versions[2],
        }], objs)
        self.assertFalse(resp.get('DeleteMarkers'))

        # and the stack pops
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[2],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # ...and the restored version is now what a plain GET will return
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[2], resp['ETag'])

    def test_delete_versioned_objects_while_versioning_enabled(self):
        # verify DELETE?versionId=xxx and restore-on-delete
        retry(self.enable_versioning)
        etags = []
        obj_name = self.create_name('versioned-obj')
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(io.BytesIO(obj_data),
                                       self.bucket_name, obj_name)

        self._do_test_delete_versioned_objects(obj_name, obj_data, etags)

    def test_delete_versioned_objects_while_versioning_suspended(self):
        # verify DELETE?versionId=xxx and restore-on-delete
        retry(self.enable_versioning)
        etags = []
        obj_name = self.create_name('versioned-obj')
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(io.BytesIO(obj_data),
                                       self.bucket_name, obj_name)

        retry(self.disable_versioning)
        self._do_test_delete_versioned_objects(obj_name, obj_data, etags)

    def _test_delete_version_previous_restored(
            self, obj_name, obj_data, etags, expected_versions):
        # only latest version appears in the listing
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # ...and that's the object that a plain GET will return
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # but everything is layed out in the object versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            self._sanitize_obj_listing(obj)
            versions.append(obj.pop('VersionId'))
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"%s"' % etags[1],
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)
        self.assertEqual(expected_versions, versions)
        self.assertFalse(resp.get('DeleteMarkers'))

        # delete the latest version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[0])
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])
        # and that pulls it out of the versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[1],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
            'VersionId': versions[1],
        }], objs)
        self.assertFalse(resp.get('DeleteMarkers'))

        # ...and the previous version is restored
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual([{
            'ETag': '"%s"' % etags[1],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # ...and a plain GET will now return the previous version
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[1], resp['ETag'])

        # delete the current version and it's gone
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[1])
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        self.assertFalse(resp.get('Versions'))
        self.assertFalse(resp.get('DeleteMarkers'))
        self.assert_no_such_version(self.bucket_name, obj_name, versions[1])
        self.assert_no_such_key(self.bucket_name, obj_name)

    def test_delete_version_null_restored_while_versioning_enabled(self):
        # verify DELETE?versionId=xxx and null restore-on-delete
        # create a null version
        obj_name = self.create_name('versioned-obj')
        obj_data = self.create_name('some-data-0').encode('ascii')
        etags = [md5(obj_data, usedforsecurity=False).hexdigest()]
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        retry(self.enable_versioning)

        # create a non-null version
        obj_data = self.create_name('some-data-1').encode('ascii')
        etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        self._test_delete_version_previous_restored(
            obj_name, obj_data, etags, [mock.ANY, 'null'])

    def test_delete_version_null_restored_while_versioning_suspended(self):
        # verify DELETE?versionId=xxx and null restore-on-delete
        # create a null version
        obj_name = self.create_name('versioned-obj')
        obj_data = self.create_name('some-data-0').encode('ascii')
        etags = [md5(obj_data, usedforsecurity=False).hexdigest()]
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        retry(self.enable_versioning)

        # create a non-null version
        obj_data = self.create_name('some-data-1').encode('ascii')
        etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        retry(self.disable_versioning)

        self._test_delete_version_previous_restored(
            obj_name, obj_data, etags, [mock.ANY, 'null'])

    def test_delete_null_version_older_version_restored(self):
        # verify DELETE?versionId=null and restore-on-delete
        retry(self.enable_versioning)

        # create a non-null version
        obj_name = self.create_name('versioned-obj')
        obj_data = self.create_name('some-data-0').encode('ascii')
        etags = [md5(obj_data, usedforsecurity=False).hexdigest()]
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        retry(self.disable_versioning)

        # create a null version
        obj_data = self.create_name('some-data-1').encode('ascii')
        etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        self._test_delete_version_previous_restored(
            obj_name, obj_data, etags, ['null', mock.ANY])

    def _test_delete_anon_does_not_restore(self, obj_name):
        orig_versions = []
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            orig_versions.append(obj.pop('VersionId'))
        self.assertEqual(2, len(orig_versions))
        self.assertFalse(resp.get('DeleteMarkers'))

        # delete without a version id does NOT restore the previous version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assert_no_such_key(self.bucket_name, obj_name)

        # versions remain
        versions = []
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            versions.append(obj.pop('VersionId'))
        self.assertEqual(orig_versions, versions)
        # ... but there's also a delete marker
        markers = resp.get('DeleteMarkers', [])
        for marker in markers:
            self._sanitize_obj_listing(marker)
        self.assertEqual([{
            'Key': obj_name,
            'VersionId': mock.ANY,
            'IsLatest': True,
        }], markers)
        self.assertNotIn(markers[0]['VersionId'], orig_versions)

    def test_delete_anon_no_restore_while_versioning_enabled(self):
        # verify DELETE without versionId does not restore
        retry(self.enable_versioning)

        # create two versions
        obj_name = self.create_name('versioned-obj')
        for i in range(2):
            obj_data = self.create_name('some-data-%d' % i).encode('ascii')
            self.client.upload_fileobj(io.BytesIO(obj_data),
                                       self.bucket_name, obj_name)

        self._test_delete_anon_does_not_restore(obj_name)

    def test_delete_anon_no_restore_while_versioning_suspended(self):
        # verify DELETE without versionId does not restore
        retry(self.enable_versioning)

        # create two versions
        obj_name = self.create_name('versioned-obj')
        for i in range(2):
            obj_data = self.create_name('some-data-%d' % i).encode('ascii')
            self.client.upload_fileobj(io.BytesIO(obj_data),
                                       self.bucket_name, obj_name)

        retry(self.disable_versioning)

        self._test_delete_anon_does_not_restore(obj_name)

    def test_delete_anon_current_is_null_no_restore(self):
        # verify DELETE of null version without versionId does not restore
        retry(self.enable_versioning)

        # create version
        obj_name = self.create_name('versioned-obj')
        obj_data = self.create_name('some-data-0').encode('ascii')
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        retry(self.disable_versioning)

        # create null version
        obj_data = self.create_name('some-data-1').encode('ascii')
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        orig_versions = []
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            orig_versions.append(obj.pop('VersionId'))
        self.assertEqual(['null', mock.ANY], orig_versions)
        self.assertFalse(resp.get('DeleteMarkers'))

        # delete without a version id does NOT restore the previous version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assert_no_such_key(self.bucket_name, obj_name)

        # non-null version remains
        versions = []
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            versions.append(obj.pop('VersionId'))
        self.assertEqual(orig_versions[1:], versions)
        # ... but there's also a delete marker
        markers = resp.get('DeleteMarkers', [])
        for marker in markers:
            self._sanitize_obj_listing(marker)
        self.assertEqual([{
            'Key': obj_name,
            'VersionId': 'null',
            'IsLatest': True,
        }], markers)

    def test_delete_versioned_deletes(self):
        retry(self.enable_versioning)
        etags = []
        obj_name = self.create_name('versioned-obj')
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(io.BytesIO(obj_data),
                                       self.bucket_name, obj_name)
            # and make a delete marker
            self.client.delete_object(Bucket=self.bucket_name, Key=obj_name)

        # current listing is empty
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        self.assertEqual([], objs)

        # but everything is in layed out in the versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            self._sanitize_obj_listing(obj)
            versions.append(obj.pop('VersionId'))
        self.assertEqual([{
            'ETag': '"%s"' % etag,
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        } for etag in etags], objs)
        # ... plus the delete markers
        delete_markers = resp.get('DeleteMarkers', [])
        marker_versions = []
        for marker in delete_markers:
            self._sanitize_obj_listing(marker)
            marker_versions.append(marker.pop('VersionId'))
        self.assertEqual([{
            'Key': obj_name,
            'IsLatest': is_latest,
        } for is_latest in (True, False, False)], delete_markers)

        # delete an old delete markers
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=marker_versions[2])
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # since IsLatest is still a delete marker we'll raise NoSuchKey
        self.assert_no_such_key(self.bucket_name, obj_name)

        # now delete the delete marker (IsLatest)
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=marker_versions[0])
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # most recent version is now restored to be the latest
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # now delete the IsLatest object version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[0])
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # and object is deleted again
        self.assert_no_such_key(self.bucket_name, obj_name)

        # delete marker IsLatest
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        delete_markers = resp.get('DeleteMarkers', [])
        for marker in delete_markers:
            self._sanitize_obj_listing(marker)
        self.assertEqual([{
            'Key': obj_name,
            'IsLatest': True,
            'VersionId': marker_versions[1],
        }], delete_markers)

    def test_multipart_upload(self):
        # enable versioning now...
        retry(self.enable_versioning)
        obj_name = self.create_name('versioned-obj')
        obj_data = b'data'

        mu = self.client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name)
        part_md5 = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=mu['UploadId'],
            PartNumber=1,
            Body=obj_data)['ETag']
        complete_response = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=mu['UploadId'],
            MultipartUpload={'Parts': [
                {'PartNumber': 1, 'ETag': part_md5},
            ]})
        obj_etag = complete_response['ETag']

        delete_response = self.client.delete_object(
            Bucket=self.bucket_name,
            Key=obj_name)
        marker_version_id = delete_response['VersionId']

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            self._sanitize_obj_listing(obj)
            versions.append(obj.pop('VersionId'))
        self.assertEqual([{
            'ETag': obj_etag,
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        markers = resp.get('DeleteMarkers', [])
        for marker in markers:
            marker.pop('LastModified')
            marker.pop('Owner')
        self.assertEqual([{
            'IsLatest': True,
            'Key': obj_name,
            'VersionId': marker_version_id,
        }], markers)

        # Can still get the old version
        resp = self.client.get_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            VersionId=versions[0])
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(obj_etag, resp['ETag'])

        delete_response = self.client.delete_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            VersionId=versions[0])

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        self.assertEqual([], resp.get('Versions', []))

        markers = resp.get('DeleteMarkers', [])
        for marker in markers:
            self._sanitize_obj_listing(marker)
        self.assertEqual([{
            'IsLatest': True,
            'Key': obj_name,
            'VersionId': marker_version_id,
        }], markers)

    def test_get_versioned_object(self):
        retry(self.enable_versioning)
        etags = []
        obj_name = self.create_name('versioned-obj')
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            # TODO: pull etag from response instead
            etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(
                io.BytesIO(obj_data), self.bucket_name, obj_name)

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            self._sanitize_obj_listing(obj)
            versions.append(obj.pop('VersionId'))
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"%s"' % etags[1],
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }, {
            'ETag': '"%s"' % etags[2],
            'IsLatest': False,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # un-versioned get_object returns IsLatest
        resp = self.client.get_object(Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # but you can get any object by version
        for i, version in enumerate(versions):
            resp = self.client.get_object(
                Bucket=self.bucket_name, Key=obj_name, VersionId=version)
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            self.assertEqual('"%s"' % etags[i], resp['ETag'])

        # and head_object works about the same
        resp = self.client.head_object(Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])
        self.assertEqual(versions[0], resp['VersionId'])
        for version, etag in zip(versions, etags):
            resp = self.client.head_object(
                Bucket=self.bucket_name, Key=obj_name, VersionId=version)
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            self.assertEqual(version, resp['VersionId'])
            self.assertEqual('"%s"' % etag, resp['ETag'])

    def test_get_versioned_object_invalid_params(self):
        retry(self.enable_versioning)
        with self.assertRaises(ClientError) as ctx:
            self.client.list_object_versions(Bucket=self.bucket_name,
                                             KeyMarker='',
                                             VersionIdMarker='bogus')
        expected_err = 'An error occurred (InvalidArgument) when calling ' \
            'the ListObjectVersions operation: Invalid version id specified'
        self.assertEqual(expected_err, str(ctx.exception))

        with self.assertRaises(ClientError) as ctx:
            self.client.list_object_versions(
                Bucket=self.bucket_name,
                VersionIdMarker='a' * 32)
        expected_err = 'An error occurred (InvalidArgument) when calling ' \
            'the ListObjectVersions operation: A version-id marker cannot ' \
            'be specified without a key marker.'
        self.assertEqual(expected_err, str(ctx.exception))

    def test_get_versioned_object_key_marker(self):
        retry(self.enable_versioning)
        obj00_name = self.create_name('00-versioned-obj')
        obj01_name = self.create_name('01-versioned-obj')
        names = [obj00_name] * 3 + [obj01_name] * 3
        latest = [True, False, False, True, False, False]
        etags = []
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, '"%s"' % md5(
                obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(
                io.BytesIO(obj_data), self.bucket_name, obj01_name)
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, '"%s"' % md5(
                obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(
                io.BytesIO(obj_data), self.bucket_name, obj00_name)
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        versions = []
        objs = []
        for o in resp.get('Versions', []):
            versions.append(o['VersionId'])
            objs.append({
                'Key': o['Key'],
                'VersionId': o['VersionId'],
                'IsLatest': o['IsLatest'],
                'ETag': o['ETag'],
            })
        expected = [{
            'Key': name,
            'VersionId': version,
            'IsLatest': is_latest,
            'ETag': etag,
        } for name, etag, version, is_latest in zip(
            names, etags, versions, latest)]
        self.assertEqual(expected, objs)

        # on s3 this makes expected[0]['IsLatest'] magicaly change to False?
        # resp = self.client.list_object_versions(Bucket=self.bucket_name,
        #                                         KeyMarker='',
        #                                         VersionIdMarker=versions[0])
        # objs = [{
        #     'Key': o['Key'],
        #     'VersionId': o['VersionId'],
        #     'IsLatest': o['IsLatest'],
        #     'ETag': o['ETag'],
        # } for o in resp.get('Versions', [])]
        # self.assertEqual(expected, objs)

        # KeyMarker skips past that key
        resp = self.client.list_object_versions(Bucket=self.bucket_name,
                                                KeyMarker=obj00_name)
        objs = [{
            'Key': o['Key'],
            'VersionId': o['VersionId'],
            'IsLatest': o['IsLatest'],
            'ETag': o['ETag'],
        } for o in resp.get('Versions', [])]
        self.assertEqual(expected[3:], objs)

        # KeyMarker with VersionIdMarker skips past that version
        resp = self.client.list_object_versions(Bucket=self.bucket_name,
                                                KeyMarker=obj00_name,
                                                VersionIdMarker=versions[0])
        objs = [{
            'Key': o['Key'],
            'VersionId': o['VersionId'],
            'IsLatest': o['IsLatest'],
            'ETag': o['ETag'],
        } for o in resp.get('Versions', [])]
        self.assertEqual(expected[1:], objs)

        # KeyMarker with bogus version skips past that key
        resp = self.client.list_object_versions(
            Bucket=self.bucket_name,
            KeyMarker=obj00_name,
            VersionIdMarker=versions[4])
        objs = [{
            'Key': o['Key'],
            'VersionId': o['VersionId'],
            'IsLatest': o['IsLatest'],
            'ETag': o['ETag'],
        } for o in resp.get('Versions', [])]
        self.assertEqual(expected[3:], objs)

    def test_list_objects(self):
        retry(self.enable_versioning)
        etags = defaultdict(list)
        for i in range(3):
            obj_name = self.create_name('versioned-obj')
            for i in range(3):
                obj_data = self.create_name('some-data-%s' % i).encode('ascii')
                etags[obj_name].insert(0, md5(
                    obj_data, usedforsecurity=False).hexdigest())
                self.client.upload_fileobj(
                    io.BytesIO(obj_data), self.bucket_name, obj_name)

        # both unversioned list_objects responses are similar
        expected = []
        for name, obj_etags in sorted(etags.items()):
            expected.append({
                'ETag': '"%s"' % obj_etags[0],
                'Key': name,
                'Size': len(obj_data),
                'StorageClass': 'STANDARD',
            })
        resp = self.client.list_objects(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            owner = obj.pop('Owner')
            # one difference seems to be the Owner key
            self.check_owner(owner)
            self._sanitize_obj_listing(obj)
        self.assertEqual(expected, objs)
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            self._sanitize_obj_listing(obj)
        self.assertEqual(expected, objs)

        # versioned listings has something for everyone
        expected = []
        for name, obj_etags in sorted(etags.items()):
            is_latest = True
            for etag in obj_etags:
                expected.append({
                    'ETag': '"%s"' % etag,
                    'IsLatest': is_latest,
                    'Key': name,
                    'Size': len(obj_data),
                    'StorageClass': 'STANDARD',
                })
                is_latest = False

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            self._sanitize_obj_listing(obj)
            versions.append(obj.pop('VersionId'))
        self.assertEqual(expected, objs)

    def test_copy_object(self):
        retry(self.enable_versioning)
        etags = []
        obj_name = self.create_name('versioned-obj')
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, md5(
                obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(
                io.BytesIO(obj_data), self.bucket_name, obj_name)

        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            versions.append(obj.pop('VersionId'))

        # CopySource can just be Bucket/Key string
        first_target = self.create_name('target-obj1')
        copy_resp = self.client.copy_object(
            Bucket=self.bucket_name, Key=first_target,
            CopySource='%s/%s' % (self.bucket_name, obj_name))
        self.assertEqual(versions[0], copy_resp['CopySourceVersionId'])

        # and you'll just get the most recent version
        resp = self.client.head_object(Bucket=self.bucket_name,
                                       Key=first_target)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # or you can be more explicit
        explicit_target = self.create_name('target-%s' % versions[0])
        copy_source = {'Bucket': self.bucket_name, 'Key': obj_name,
                       'VersionId': versions[0]}
        copy_resp = self.client.copy_object(
            Bucket=self.bucket_name, Key=explicit_target,
            CopySource=copy_source)
        self.assertEqual(versions[0], copy_resp['CopySourceVersionId'])
        # and you still get the same thing
        resp = self.client.head_object(Bucket=self.bucket_name,
                                       Key=explicit_target)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # but you can also copy from a specific version
        version_target = self.create_name('target-%s' % versions[2])
        copy_source['VersionId'] = versions[2]
        copy_resp = self.client.copy_object(
            Bucket=self.bucket_name, Key=version_target,
            CopySource=copy_source)
        self.assertEqual(versions[2], copy_resp['CopySourceVersionId'])
        resp = self.client.head_object(Bucket=self.bucket_name,
                                       Key=version_target)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[2], resp['ETag'])
