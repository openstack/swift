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

import logging
import os
import unittest
import uuid
import time

import boto3
from botocore.exceptions import ClientError
import urllib.parse

from swift.common.utils import config_true_value, readconf

from test import get_config

_CONFIG = None


# boto's loggign can get pretty noisy; require opt-in to see it all
if not config_true_value(os.environ.get('BOTO3_DEBUG')):
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)


class ConfigError(Exception):
    '''Error test conf misconfigurations'''


def load_aws_config(conf_file):
    """
    Read user credentials from an AWS CLI style credentials file and translate
    to a swift test config. Currently only supports a single user.

    :param conf_file: path to AWS credentials file
    """
    conf = readconf(conf_file, 'default')
    global _CONFIG
    _CONFIG = {
        'endpoint': conf.get('endpoint', 'https://s3.amazonaws.com'),
        'region': conf.get('region', 'us-east-1'),
        'access_key1': conf.get('aws_access_key_id'),
        'secret_key1': conf.get('aws_secret_access_key'),
        'session_token1': conf.get('aws_session_token')
    }


aws_config_file = os.environ.get('SWIFT_TEST_AWS_CONFIG_FILE')
if aws_config_file:
    load_aws_config(aws_config_file)
    print('Loaded test config from %s' % aws_config_file)


def get_opt_or_error(option):
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = get_config('s3api_test')

    value = _CONFIG.get(option)
    if not value:
        raise ConfigError('must supply [s3api_test]%s' % option)
    return value


def get_opt(option, default=None):
    try:
        return get_opt_or_error(option)
    except ConfigError:
        return default


def get_s3_client(user=1, signature_version='s3v4', addressing_style='path'):
    '''
    Get a boto3 client to talk to an S3 endpoint.

    :param user: user number to use. Should be one of:
        1 -- primary user
        2 -- secondary user
        3 -- unprivileged user
    :param signature_version: S3 signing method. Should be one of:
        s3 -- v2 signatures; produces Authorization headers like
              ``AWS access_key:signature``
        s3-query -- v2 pre-signed URLs; produces query strings like
                    ``?AWSAccessKeyId=access_key&Signature=signature``
        s3v4 -- v4 signatures; produces Authorization headers like
                ``AWS4-HMAC-SHA256
                Credential=access_key/date/region/s3/aws4_request,
                Signature=signature``
        s3v4-query -- v4 pre-signed URLs; produces query strings like
                      ``?X-Amz-Algorithm=AWS4-HMAC-SHA256&
                      X-Amz-Credential=access_key/date/region/s3/aws4_request&
                      X-Amz-Signature=signature``
    :param addressing_style: One of:
        path -- produces URLs like ``http(s)://host.domain/bucket/key``
        virtual -- produces URLs like ``http(s)://bucket.host.domain/key``
    '''
    endpoint = get_opt('endpoint', None)
    if endpoint:
        scheme = urllib.parse.urlsplit(endpoint).scheme
        if scheme not in ('http', 'https'):
            raise ConfigError('unexpected scheme in endpoint: %r; '
                              'expected http or https' % scheme)
    else:
        scheme = None
    region = get_opt('region', 'us-east-1')
    access_key = get_opt_or_error('access_key%d' % user)
    secret_key = get_opt_or_error('secret_key%d' % user)
    session_token = get_opt('session_token%d' % user)

    ca_cert = get_opt('ca_cert')
    if ca_cert is not None:
        try:
            # do a quick check now; it's more expensive to have boto check
            os.stat(ca_cert)
        except OSError as e:
            raise ConfigError(str(e))

    return boto3.client(
        's3',
        endpoint_url=endpoint,
        region_name=region,
        use_ssl=(scheme == 'https'),
        verify=ca_cert,
        config=boto3.session.Config(s3={
            'signature_version': signature_version,
            'addressing_style': addressing_style,
        }),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )


def etag_from_resp(response):
    return response['ETag']


def code_from_error(error):
    return error.response['Error']['Code']


def status_from_error(error):
    return error.response['ResponseMetadata']['HTTPStatusCode']


TEST_PREFIX = 's3api-test-'


class BaseS3Mixin(object):
    # Default to v4 signatures (as aws-cli does), but subclasses can override
    signature_version = 's3v4'

    @classmethod
    def get_s3_client(cls, user):
        return get_s3_client(user, cls.signature_version)

    @classmethod
    def _remove_all_object_versions_from_bucket(cls, client, bucket_name):
        resp = client.list_object_versions(Bucket=bucket_name)
        objs_to_delete = (resp.get('Versions', []) +
                          resp.get('DeleteMarkers', []))
        while objs_to_delete:
            multi_delete_body = {
                'Objects': [
                    {'Key': obj['Key'], 'VersionId': obj['VersionId']}
                    for obj in objs_to_delete
                ],
                'Quiet': False,
            }
            del_resp = client.delete_objects(Bucket=bucket_name,
                                             Delete=multi_delete_body)
            if any(del_resp.get('Errors', [])):
                raise Exception('Unable to delete %r' % del_resp['Errors'])
            if not resp['IsTruncated']:
                break
            key_marker = resp['NextKeyMarker']
            version_id_marker = resp['NextVersionIdMarker']
            resp = client.list_object_versions(
                Bucket=bucket_name, KeyMarker=key_marker,
                VersionIdMarker=version_id_marker)
            objs_to_delete = (resp.get('Versions', []) +
                              resp.get('DeleteMarkers', []))

    @classmethod
    def clear_bucket(cls, client, bucket_name):
        timeout = time.time() + 10
        backoff = 0.1
        cls._remove_all_object_versions_from_bucket(client, bucket_name)
        try:
            client.delete_bucket(Bucket=bucket_name)
        except ClientError as e:
            if 'NoSuchBucket' in str(e):
                return
            if 'BucketNotEmpty' not in str(e):
                raise
            # Something's gone sideways. Try harder
            client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Suspended'})
            while True:
                cls._remove_all_object_versions_from_bucket(
                    client, bucket_name)
                # also try some version-unaware operations...
                for key in client.list_objects(Bucket=bucket_name).get(
                        'Contents', []):
                    client.delete_object(Bucket=bucket_name, Key=key['Key'])

                # *then* try again
                try:
                    client.delete_bucket(Bucket=bucket_name)
                except ClientError as e:
                    if 'NoSuchBucket' in str(e):
                        return
                    if 'BucketNotEmpty' not in str(e):
                        raise
                    if time.time() > timeout:
                        raise Exception('Timeout clearing %r' % bucket_name)
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    break

    @classmethod
    def create_name(cls, slug):
        return '%s%s-%s' % (TEST_PREFIX, slug, uuid.uuid4().hex)

    @classmethod
    def clear_account(cls, client):
        for bucket in client.list_buckets()['Buckets']:
            if not bucket['Name'].startswith(TEST_PREFIX):
                # these tests run against real s3 accounts
                continue
            cls.clear_bucket(client, bucket['Name'])


class BaseS3TestCase(BaseS3Mixin, unittest.TestCase):
    def tearDown(self):
        client = self.get_s3_client(1)
        self.clear_account(client)
        try:
            client = self.get_s3_client(2)
        except ConfigError:
            pass
        else:
            self.clear_account(client)


class BaseS3TestCaseWithBucket(BaseS3Mixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bucket_name = cls.create_name('bucket')
        client = cls.get_s3_client(1)
        client.create_bucket(Bucket=cls.bucket_name)

    @classmethod
    def tearDownClass(cls):
        client = cls.get_s3_client(1)
        cls.clear_account(client)
        try:
            client = cls.get_s3_client(2)
        except ConfigError:
            pass
        else:
            cls.clear_account(client)
