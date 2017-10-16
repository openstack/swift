# Copyright (c) 2015 OpenStack Foundation
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

import os
from boto.s3.connection import S3Connection, OrdinaryCallingFormat, \
    BotoClientError, S3ResponseError

RETRY_COUNT = 3


class Connection(object):
    """
    Connection class used for S3 functional testing.
    """
    def __init__(self, aws_access_key=os.environ.get('TESTER_ACCESS_KEY'),
                 aws_secret_key=os.environ.get('TESTER_SECRET_KEY'),
                 user_id='%s:%s' % (os.environ.get('TESTER_TENANT'),
                                    os.environ.get('TESTER_USER'))):
        """
        Initialize method.

        :param aws_access_key: a string of aws access key
        :param aws_secret_key: a string of aws secret key
        :param user_id: a string consists of TENANT and USER name used for
                        asserting Owner ID (not required S3Connection)

        In default, Connection class will be initialized as tester user
        behaves as:
        user_test_tester = testing .admin

        """
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.user_id = user_id
        swift_host = os.environ.get('SWIFT_HOST').split(':')
        self.host = swift_host[0]
        self.port = int(swift_host[1]) if len(swift_host) == 2 else 80
        self.conn = \
            S3Connection(aws_access_key, aws_secret_key, is_secure=False,
                         host=self.host, port=self.port,
                         calling_format=OrdinaryCallingFormat())
        self.conn.auth_region_name = 'US'

    def reset(self):
        """
        Reset all swift environment to keep clean. As a result by calling this
        method, we can assume the backend swift keeps no containers and no
        objects on this connection's account.
        """
        exceptions = []
        for i in range(RETRY_COUNT):
            try:
                buckets = self.conn.get_all_buckets()
                if not buckets:
                    break

                for bucket in buckets:
                    try:
                        for upload in bucket.list_multipart_uploads():
                            upload.cancel_upload()

                        for obj in bucket.list():
                            bucket.delete_key(obj.name)

                        self.conn.delete_bucket(bucket.name)
                    except S3ResponseError as e:
                        # 404 means NoSuchBucket, NoSuchKey, or NoSuchUpload
                        if e.status != 404:
                            raise
            except (BotoClientError, S3ResponseError) as e:
                exceptions.append(e)
        if exceptions:
            # raise the first exception
            raise exceptions.pop(0)

    def make_request(self, method, bucket='', obj='', headers=None, body='',
                     query=None):
        """
        Wrapper method of S3Connection.make_request.

        :param method: a string of HTTP request method
        :param bucket: a string of bucket name
        :param obj: a string of object name
        :param headers: a dictionary of headers
        :param body: a string of data binary sent to S3 as a request body
        :param query: a string of HTTP query argument

        :returns: a tuple of (int(status_code), headers dict, response body)
        """
        response = \
            self.conn.make_request(method, bucket=bucket, key=obj,
                                   headers=headers, data=body,
                                   query_args=query, sender=None,
                                   override_num_retries=RETRY_COUNT,
                                   retry_handler=None)
        return response.status, dict(response.getheaders()), response.read()

    def generate_url_and_headers(self, method, bucket='', obj='',
                                 expires_in=3600):
        url = self.conn.generate_url(expires_in, method, bucket, obj)
        if os.environ.get('S3_USE_SIGV4') == "True":
            # V4 signatures are known-broken in boto, but we can work around it
            if url.startswith('https://'):
                url = 'http://' + url[8:]
            return url, {'Host': '%(host)s:%(port)d:%(port)d' % {
                'host': self.host, 'port': self.port}}
        return url, {}


def get_admin_connection():
    """
    Return tester connection behaves as:
    user_test_admin = admin .admin
    """
    aws_access_key = os.environ.get('ADMIN_ACCESS_KEY')
    aws_secret_key = os.environ.get('ADMIN_SECRET_KEY')
    user_id = os.environ.get('ADMIN_TENANT') + ':' + \
        os.environ.get('ADMIN_USER')
    return Connection(aws_access_key, aws_secret_key, user_id)


def get_tester2_connection():
    """
    Return tester2 connection behaves as:
    user_test_tester2 = testing2
    """
    aws_access_key = os.environ.get('TESTER2_ACCESS_KEY')
    aws_secret_key = os.environ.get('TESTER2_SECRET_KEY')
    user_id = os.environ.get('TESTER2_TENANT') + ':' + \
        os.environ.get('TESTER2_USER')
    return Connection(aws_access_key, aws_secret_key, user_id)
