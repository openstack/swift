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

import unittest2
import os
import test.functional as tf
from swift.common.middleware.s3api.etree import fromstring, tostring, Element, \
    SubElement

from test.functional.s3api import S3ApiBase
from test.functional.s3api.s3_test_client import Connection
from test.functional.s3api.utils import get_error_code, calculate_md5


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiMultiDelete(S3ApiBase):
    def _prepare_test_delete_multi_objects(self, bucket, objects):
        self.conn.make_request('PUT', bucket)
        for obj in objects:
            self.conn.make_request('PUT', bucket, obj)

    def _gen_multi_delete_xml(self, objects, quiet=None):
        elem = Element('Delete')
        if quiet:
            SubElement(elem, 'Quiet').text = quiet
        for key in objects:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key

        return tostring(elem, use_s3ns=False)

    def _gen_invalid_multi_delete_xml(self, hasObjectTag=False):
        elem = Element('Delete')
        if hasObjectTag:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = ''

        return tostring(elem, use_s3ns=False)

    def test_delete_multi_objects(self):
        bucket = 'bucket'
        put_objects = ['obj%s' % var for var in range(4)]
        self._prepare_test_delete_multi_objects(bucket, put_objects)
        query = 'delete'

        # Delete an object via MultiDelete API
        req_objects = ['obj0']
        xml = self._gen_multi_delete_xml(req_objects)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue(headers['content-type'] is not None)
        self.assertEqual(headers['content-length'], str(len(body)))
        elem = fromstring(body)
        resp_objects = elem.findall('Deleted')
        self.assertEqual(len(resp_objects), len(req_objects))
        for o in resp_objects:
            self.assertTrue(o.find('Key').text in req_objects)

        # Delete 2 objects via MultiDelete API
        req_objects = ['obj1', 'obj2']
        xml = self._gen_multi_delete_xml(req_objects)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(status, 200)
        elem = fromstring(body, 'DeleteResult')
        resp_objects = elem.findall('Deleted')
        self.assertEqual(len(resp_objects), len(req_objects))
        for o in resp_objects:
            self.assertTrue(o.find('Key').text in req_objects)

        # Delete 2 objects via MultiDelete API but one (obj4) doesn't exist.
        req_objects = ['obj3', 'obj4']
        xml = self._gen_multi_delete_xml(req_objects)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(status, 200)
        elem = fromstring(body, 'DeleteResult')
        resp_objects = elem.findall('Deleted')
        # S3 assumes a NoSuchKey object as deleted.
        self.assertEqual(len(resp_objects), len(req_objects))
        for o in resp_objects:
            self.assertTrue(o.find('Key').text in req_objects)

        # Delete 2 objects via MultiDelete API but no objects exist
        req_objects = ['obj4', 'obj5']
        xml = self._gen_multi_delete_xml(req_objects)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(status, 200)
        elem = fromstring(body, 'DeleteResult')
        resp_objects = elem.findall('Deleted')
        self.assertEqual(len(resp_objects), len(req_objects))
        for o in resp_objects:
            self.assertTrue(o.find('Key').text in req_objects)

    def test_delete_multi_objects_error(self):
        bucket = 'bucket'
        put_objects = ['obj']
        self._prepare_test_delete_multi_objects(bucket, put_objects)
        xml = self._gen_multi_delete_xml(put_objects)
        content_md5 = calculate_md5(xml)
        query = 'delete'

        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = \
            auth_error_conn.make_request('POST', bucket, body=xml,
                                         headers={
                                             'Content-MD5': content_md5
                                         },
                                         query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('POST', 'nothing', body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        # without Object tag
        xml = self._gen_invalid_multi_delete_xml()
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(get_error_code(body), 'MalformedXML')

        # without value of Key tag
        xml = self._gen_invalid_multi_delete_xml(hasObjectTag=True)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(get_error_code(body), 'UserKeyMustBeSpecified')

        max_deletes = tf.cluster_info.get('s3api', {}).get(
            'max_multi_delete_objects', 1000)
        # specified number of objects are over max_multi_delete_objects
        # (Default 1000), but xml size is relatively small
        req_objects = ['obj%s' for var in range(max_deletes + 1)]
        xml = self._gen_multi_delete_xml(req_objects)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(get_error_code(body), 'MalformedXML')

        # specified xml size is large, but number of objects are
        # smaller than max_multi_delete_objects.
        obj = 'a' * 102400
        req_objects = [obj + str(var) for var in range(max_deletes - 1)]
        xml = self._gen_multi_delete_xml(req_objects)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(get_error_code(body), 'MalformedXML')

    def test_delete_multi_objects_with_quiet(self):
        bucket = 'bucket'
        put_objects = ['obj']
        query = 'delete'

        # with Quiet true
        quiet = 'true'
        self._prepare_test_delete_multi_objects(bucket, put_objects)
        xml = self._gen_multi_delete_xml(put_objects, quiet)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(status, 200)
        elem = fromstring(body, 'DeleteResult')
        resp_objects = elem.findall('Deleted')
        self.assertEqual(len(resp_objects), 0)

        # with Quiet false
        quiet = 'false'
        self._prepare_test_delete_multi_objects(bucket, put_objects)
        xml = self._gen_multi_delete_xml(put_objects, quiet)
        content_md5 = calculate_md5(xml)
        status, headers, body = \
            self.conn.make_request('POST', bucket, body=xml,
                                   headers={'Content-MD5': content_md5},
                                   query=query)
        self.assertEqual(status, 200)
        elem = fromstring(body, 'DeleteResult')
        resp_objects = elem.findall('Deleted')
        self.assertEqual(len(resp_objects), 1)


class TestS3ApiMultiDeleteSigV4(TestS3ApiMultiDelete):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']

    def setUp(self):
        super(TestS3ApiMultiDeleteSigV4, self).setUp()


if __name__ == '__main__':
    unittest2.main()
