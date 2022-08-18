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

from botocore.exceptions import ClientError
import io

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import md5
from test.s3api import BaseS3TestCase


def retry(f, timeout=10):
    timelimit = time.time() + timeout
    while True:
        try:
            f()
        except (ClientError, AssertionError):
            if time.time() > timelimit:
                raise
            continue
        else:
            break


class TestObjectVersioning(BaseS3TestCase):

    maxDiff = None

    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('versioning')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        def enable_versioning():
            resp = self.client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'})
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        retry(enable_versioning)

    def tearDown(self):
        resp = self.client.put_bucket_versioning(
            Bucket=self.bucket_name,
            VersioningConfiguration={'Status': 'Suspended'})
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.clear_bucket(self.client, self.bucket_name)
        super(TestObjectVersioning, self).tearDown()

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

    def test_upload_fileobj_versioned(self):
        obj_data = self.create_name('some-data').encode('ascii')
        obj_etag = md5(obj_data, usedforsecurity=False).hexdigest()
        obj_name = self.create_name('versioned-obj')
        self.client.upload_fileobj(io.BytesIO(obj_data),
                                   self.bucket_name, obj_name)

        # object is in the listing
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            obj.pop('LastModified')
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
            obj.pop('LastModified')
            obj.pop('Owner')
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
            obj.pop('LastModified')
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
            obj.pop('LastModified')
            obj.pop('Owner')
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

    def test_delete_versioned_objects(self):
        etags = []
        obj_name = self.create_name('versioned-obj')
        for i in range(3):
            obj_data = self.create_name('some-data-%s' % i).encode('ascii')
            etags.insert(0, md5(obj_data, usedforsecurity=False).hexdigest())
            self.client.upload_fileobj(io.BytesIO(obj_data),
                                       self.bucket_name, obj_name)

        # only one object appears in the listing
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            obj.pop('LastModified')
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # but everything is layed out in the object versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        versions = []
        for obj in objs:
            obj.pop('LastModified')
            obj.pop('Owner')
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

        # we can delete a specific version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[1])

        # and that just pulls it out of the versions listing
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            obj.pop('LastModified')
            obj.pop('Owner')
            obj.pop('VersionId')
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'IsLatest': True,
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

        # ... but the current listing is unaffected
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            obj.pop('LastModified')
        self.assertEqual([{
            'ETag': '"%s"' % etags[0],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # OTOH, if you delete specifically the latest version
        # we can delete a specific version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[0])

        # the versions listing has a new IsLatest
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        objs = resp.get('Versions', [])
        for obj in objs:
            obj.pop('LastModified')
            obj.pop('Owner')
            obj.pop('VersionId')
        self.assertEqual([{
            'ETag': '"%s"' % etags[2],
            'IsLatest': True,
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

        # and the stack pops
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            obj.pop('LastModified')
        self.assertEqual([{
            'ETag': '"%s"' % etags[2],
            'Key': obj_name,
            'Size': len(obj_data),
            'StorageClass': 'STANDARD',
        }], objs)

    def test_delete_versioned_deletes(self):
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
            obj.pop('LastModified')
            obj.pop('Owner')
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
            marker.pop('LastModified')
            marker.pop('Owner')
            marker_versions.append(marker.pop('VersionId'))
        self.assertEqual([{
            'Key': obj_name,
            'IsLatest': is_latest,
        } for is_latest in (True, False, False)], delete_markers)

        # delete an old delete markers
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=marker_versions[2])

        # since IsLatest is still marker we'll raise NoSuchKey
        with self.assertRaises(ClientError) as caught:
            resp = self.client.get_object(Bucket=self.bucket_name,
                                          Key=obj_name)
        expected_err = 'An error occurred (NoSuchKey) when calling the ' \
            'GetObject operation: The specified key does not exist.'
        self.assertEqual(expected_err, str(caught.exception))

        # now delete the delete marker (IsLatest)
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=marker_versions[0])

        # most recent version is now latest
        resp = self.client.get_object(Bucket=self.bucket_name,
                                      Key=obj_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('"%s"' % etags[0], resp['ETag'])

        # now delete the IsLatest object version
        resp = self.client.delete_object(Bucket=self.bucket_name,
                                         Key=obj_name,
                                         VersionId=versions[0])

        # and object is deleted again
        with self.assertRaises(ClientError) as caught:
            resp = self.client.get_object(Bucket=self.bucket_name,
                                          Key=obj_name)
        expected_err = 'An error occurred (NoSuchKey) when calling the ' \
            'GetObject operation: The specified key does not exist.'
        self.assertEqual(expected_err, str(caught.exception))

        # delete marker IsLatest
        resp = self.client.list_object_versions(Bucket=self.bucket_name)
        delete_markers = resp.get('DeleteMarkers', [])
        for marker in delete_markers:
            marker.pop('LastModified')
            marker.pop('Owner')
        self.assertEqual([{
            'Key': obj_name,
            'IsLatest': True,
            'VersionId': marker_versions[1],
        }], delete_markers)

    def test_multipart_upload(self):
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
            obj.pop('LastModified')
            obj.pop('Owner')
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
            marker.pop('LastModified')
            marker.pop('Owner')
        self.assertEqual([{
            'IsLatest': True,
            'Key': obj_name,
            'VersionId': marker_version_id,
        }], markers)

    def test_get_versioned_object(self):
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
            obj.pop('LastModified')
            obj.pop('Owner')
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
            obj.pop('LastModified')
            # one difference seems to be the Owner key
            self.assertEqual({'DisplayName', 'ID'},
                             set(obj.pop('Owner').keys()))
        self.assertEqual(expected, objs)
        resp = self.client.list_objects_v2(Bucket=self.bucket_name)
        objs = resp.get('Contents', [])
        for obj in objs:
            obj.pop('LastModified')
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
            obj.pop('LastModified')
            obj.pop('Owner')
            versions.append(obj.pop('VersionId'))
        self.assertEqual(expected, objs)

    def test_copy_object(self):
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
