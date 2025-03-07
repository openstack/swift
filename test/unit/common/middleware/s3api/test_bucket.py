# Copyright (c) 2014 OpenStack Foundation
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
from unittest import mock
from hashlib import sha256

from urllib.parse import quote, parse_qsl

from swift.common import swob
from swift.common.middleware.proxy_logging import ProxyLoggingMiddleware
from swift.common.middleware.versioned_writes.object_versioning import \
    DELETE_MARKER_CONTENT_TYPE
from swift.common.swob import Request
from swift.common.utils import json

from swift.common.middleware.s3api.etree import fromstring, tostring, \
    Element, SubElement
from swift.common.middleware.s3api.subresource import Owner, encode_acl, \
    ACLPublicRead
from swift.common.middleware.s3api.s3request import MAX_32BIT_INT

from test.unit.common.middleware.helpers import normalize_path
from test.unit.common.middleware.s3api import S3ApiTestCase, S3ApiTestCaseAcl
from test.unit.common.middleware.s3api.helpers import UnreadableInput

# Example etag from ProxyFS; note that it is already quote-wrapped
PFS_ETAG = '"pfsv2/AUTH_test/01234567/89abcdef-32"'


class BaseS3ApiBucket(object):
    def setup_objects(self):
        self.objects = (('lily', '2011-01-05T02:19:14.275290', '0', '3909'),
                        (u'lily-\u062a', '2011-01-05T02:19:14.275290', 0, 390),
                        ('mu', '2011-01-05T02:19:14.275290',
                         'md5-of-the-manifest; s3_etag=0', '3909'),
                        ('pfs-obj', '2011-01-05T02:19:14.275290',
                         PFS_ETAG, '3909'),
                        ('rose', '2011-01-05T02:19:14.275290', 0, 303),
                        ('slo', '2011-01-05T02:19:14.275290',
                         'md5-of-the-manifest', '3909'),
                        ('viola', '2011-01-05T02:19:14.275290', '0', 3909),
                        ('with space', '2011-01-05T02:19:14.275290', 0, 390),
                        ('with%20space', '2011-01-05T02:19:14.275290', 0, 390))

        self.objects_list = [
            {'name': item[0], 'last_modified': str(item[1]),
             'content_type': 'application/octet-stream',
             'hash': str(item[2]), 'bytes': str(item[3])}
            for item in self.objects]
        self.objects_list[5]['slo_etag'] = '"0"'
        self.versioned_objects = [{
            'name': 'rose',
            'version_id': '2',
            'hash': '0',
            'bytes': '0',
            'last_modified': '2010-03-01T17:09:51.510928',
            'content_type': DELETE_MARKER_CONTENT_TYPE,
            'is_latest': False,
        }, {
            'name': 'rose',
            'version_id': '1',
            'hash': '1234',
            'bytes': '6',
            'last_modified': '2010-03-01T17:09:50.510928',
            'content_type': 'application/octet-stream',
            'is_latest': False,
        }]

        listing_body = json.dumps(self.objects_list)
        self.prefixes = ['rose', 'viola', 'lily']
        object_list_subdir = [{"subdir": p} for p in self.prefixes]

        self.swift.register('DELETE', '/v1/AUTH_test/bucket+segments',
                            swob.HTTPNoContent, {}, json.dumps([]))
        for name, _, _, _ in self.objects:
            self.swift.register(
                'DELETE',
                '/v1/AUTH_test/bucket+segments/' +
                swob.bytes_to_wsgi(name.encode('utf-8')),
                swob.HTTPNoContent, {}, json.dumps([]))
        self.swift.register(
            'GET',
            '/v1/AUTH_test/bucket+segments?format=json&marker=with%2520space',
            swob.HTTPOk,
            {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps([]))
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments?format=json&marker=',
            swob.HTTPOk, {'Content-Type': 'application/json'}, listing_body)
        self.swift.register(
            'HEAD', '/v1/AUTH_test/junk', swob.HTTPNoContent,
            {'X-Backend-Storage-Policy-Index': '3'}, None)
        self.swift.register(
            'HEAD', '/v1/AUTH_test/nojunk', swob.HTTPNotFound, {}, None)
        self.swift.register(
            'HEAD', '/v1/AUTH_test/unavailable', swob.HTTPServiceUnavailable,
            {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/junk', swob.HTTPOk,
            {'Content-Type': 'application/json',
             'X-Backend-Storage-Policy-Index': '3'}, listing_body)
        self.swift.register(
            'GET', '/v1/AUTH_test/junk-subdir', swob.HTTPOk,
            {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(object_list_subdir))
        self.swift.register(
            'GET',
            '/v1/AUTH_test/subdirs?delimiter=/&limit=3',
            swob.HTTPOk, {}, json.dumps([
                {'subdir': 'nothing/'},
                {'subdir': u'but-\u062a/'},
                {'subdir': 'subdirs/'},
            ]))

    def setUp(self):
        super(BaseS3ApiBucket, self).setUp()
        self.setup_objects()

    def _add_versions_request(self, orig_objects=None, versioned_objects=None,
                              bucket='junk'):
        if orig_objects is None:
            orig_objects = self.objects_list
        if versioned_objects is None:
            versioned_objects = self.versioned_objects
        all_versions = versioned_objects + [
            dict(i, version_id='null', is_latest=True)
            for i in orig_objects]
        all_versions.sort(key=lambda o: (
            o['name'], '' if o['version_id'] == 'null' else o['version_id']))
        self.swift.register(
            'GET', '/v1/AUTH_test/%s' % bucket, swob.HTTPOk,
            {'Content-Type': 'application/json'}, json.dumps(all_versions))

    def _assert_delete_markers(self, elem):
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(len(delete_markers), 1)
        self.assertEqual(delete_markers[0].find('./IsLatest').text, 'false')
        self.assertEqual(delete_markers[0].find('./VersionId').text, '2')
        self.assertEqual(delete_markers[0].find('./Key').text, 'rose')

    def _test_bucket_PUT_with_location(self, root_element):
        elem = Element(root_element)
        SubElement(elem, 'LocationConstraint').text = 'us-east-1'
        xml = tostring(elem)

        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def _test_method_error_delete(self, path, sw_resp):
        self.swift.register('HEAD', '/v1/AUTH_test' + path, sw_resp, {}, None)
        return self._test_method_error('DELETE', path, sw_resp)

    def test_bucket_GET_error(self):
        code = self._test_method_error('GET', '/bucket', swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error('GET', '/bucket', swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error('GET', '/bucket', swob.HTTPNotFound)
        self.assertEqual(code, 'NoSuchBucket')
        code = self._test_method_error('GET', '/bucket',
                                       swob.HTTPServiceUnavailable)
        self.assertEqual(code, 'ServiceUnavailable')
        code = self._test_method_error('GET', '/bucket', swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')

    def test_bucket_GET_non_json(self):
        # Suppose some middleware accidentally makes it return txt instead
        resp_body = b'\n'.join([b'obj%d' % i for i in range(100)])
        self.swift.register('GET', '/v1/AUTH_test/bucket', swob.HTTPOk, {},
                            resp_body)
        # When we do our GET...
        req = Request.blank('/bucket',
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        # ...there isn't much choice but to error...
        self.assertEqual(self._get_error_code(body), 'InternalError')
        # ... but we should at least log the body to aid in debugging
        self.assertIn(
            'Got non-JSON response trying to list /bucket: %r'
            % (resp_body[:60] + b'...'),
            self.s3api.logger.get_lines_for_level('error'))

    def test_bucket_PUT_error(self):
        code = self._test_method_error('PUT', '/bucket', swob.HTTPCreated,
                                       headers={'Content-Length': 'a'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket', swob.HTTPCreated,
                                       headers={'Content-Length': '-1'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket', swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error('PUT', '/bucket', swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error('PUT', '/bucket', swob.HTTPAccepted)
        self.assertEqual(code, 'BucketAlreadyOwnedByYou')
        with mock.patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                return_value={'sysmeta': {'s3api-acl': '{"Owner": "nope"}'}}):
            code = self._test_method_error(
                'PUT', '/bucket', swob.HTTPAccepted)
        self.assertEqual(code, 'BucketAlreadyExists')
        code = self._test_method_error('PUT', '/bucket', swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')
        code = self._test_method_error(
            'PUT', '/bucket', swob.HTTPServiceUnavailable)
        self.assertEqual(code, 'ServiceUnavailable')
        code = self._test_method_error(
            'PUT', '/bucket+bucket', swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')
        code = self._test_method_error(
            'PUT', '/192.168.11.1', swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')
        code = self._test_method_error(
            'PUT', '/bucket.-bucket', swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')
        code = self._test_method_error(
            'PUT', '/bucket-.bucket', swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')
        code = self._test_method_error('PUT', '/bucket*', swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')
        code = self._test_method_error('PUT', '/b', swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')
        code = self._test_method_error(
            'PUT', '/%s' % ''.join(['b' for x in range(64)]),
            swob.HTTPCreated)
        self.assertEqual(code, 'InvalidBucketName')

    def test_bucket_PUT_bucket_already_owned_by_you(self):
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket', swob.HTTPAccepted,
            {'X-Container-Object-Count': 0}, None)
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '409 Conflict')
        self.assertIn(b'BucketAlreadyOwnedByYou', body)

    def test_bucket_PUT_first_put_fail(self):
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket',
            swob.HTTPServiceUnavailable,
            {'X-Container-Object-Count': 0}, None)
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '503 Service Unavailable')
        # The last call was PUT not POST for acl set
        self.assertEqual(self.swift.calls, [
            ('PUT', '/v1/AUTH_test/bucket'),
        ])

    def test_bucket_PUT(self):
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(body, b'')
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Location'], '/bucket')

        # Apparently some clients will include a chunked transfer-encoding
        # even with no body
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Transfer-Encoding': 'chunked'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(body, b'')
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Location'], '/bucket')

        with UnreadableInput(self) as fake_input:
            req = Request.blank(
                '/bucket',
                environ={'REQUEST_METHOD': 'PUT',
                         'wsgi.input': fake_input},
                headers={'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()})
            status, headers, body = self.call_s3api(req)
        self.assertEqual(body, b'')
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Location'], '/bucket')

    def test_bucket_PUT_with_location(self):
        self._test_bucket_PUT_with_location('CreateBucketConfiguration')

    def test_bucket_PUT_with_ami_location(self):
        # ec2-ami-tools apparently uses CreateBucketConstraint instead?
        self._test_bucket_PUT_with_location('CreateBucketConstraint')

    def test_bucket_PUT_with_strange_location(self):
        # Even crazier: it doesn't seem to matter
        self._test_bucket_PUT_with_location('foo')

    def test_bucket_PUT_with_location_error(self):
        elem = Element('CreateBucketConfiguration')
        SubElement(elem, 'LocationConstraint').text = 'XXX'
        xml = tostring(elem)

        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body),
                         'InvalidLocationConstraint')

    def test_bucket_PUT_with_location_invalid_xml(self):
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='invalid_xml')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedXML')

    def test_bucket_DELETE_error(self):
        code = self._test_method_error_delete('/bucket', swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error_delete('/bucket', swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error_delete('/bucket', swob.HTTPNotFound)
        self.assertEqual(code, 'NoSuchBucket')
        code = self._test_method_error_delete('/bucket', swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')

        # bucket not empty is now validated at s3api
        self.swift._responses.get(('HEAD', '/v1/AUTH_test/bucket'))
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
                            {'X-Container-Object-Count': '1'}, None)
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, _headers, body = self.call_s3api(req)
        self.assertEqual('409 Conflict', status)
        self.assertEqual('BucketNotEmpty', self._get_error_code(body))
        self.assertNotIn('You must delete all versions in the bucket',
                         self._get_error_message(body))

    def test_bucket_DELETE_error_with_enabled_versioning(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
                            {'X-Container-Object-Count': '1',
                             'X-Container-Sysmeta-Versions-Enabled': 'True'},
                            None)
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, _headers, body = self.call_s3api(req)
        self.assertEqual('409 Conflict', status)
        self.assertEqual('BucketNotEmpty', self._get_error_code(body))
        self.assertIn('You must delete all versions in the bucket',
                      self._get_error_message(body))

    def test_bucket_DELETE_error_with_suspended_versioning(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
                            {'X-Container-Object-Count': '1',
                             'X-Container-Sysmeta-Versions-Enabled': 'False'},
                            None)
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, _headers, body = self.call_s3api(req)
        self.assertEqual('409 Conflict', status)
        self.assertEqual('BucketNotEmpty', self._get_error_code(body))
        self.assertIn('You must delete all versions in the bucket',
                      self._get_error_message(body))

    def test_bucket_DELETE(self):
        # overwrite default HEAD to return x-container-object-count
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
            {'X-Container-Object-Count': 0}, None)

        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

    def test_bucket_DELETE_with_empty_versioning(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+versioning',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket+versioning',
                            swob.HTTPNoContent, {}, None)
        # overwrite default HEAD to return x-container-object-count
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
            {'X-Container-Object-Count': 0}, None)

        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

    def test_bucket_DELETE_error_while_segment_bucket_delete(self):
        # An error occurred while deleting segment objects
        self.swift.register('DELETE', '/v1/AUTH_test/bucket+segments/lily',
                            swob.HTTPServiceUnavailable, {}, json.dumps([]))
        # overwrite default HEAD to return x-container-object-count
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
            {'X-Container-Object-Count': 0}, None)

        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '503')
        called = [(method, path) for method, path, _ in
                  self.swift.calls_with_headers]
        # Don't delete original bucket when error occurred in segment container
        self.assertNotIn(('DELETE', '/v1/AUTH_test/bucket'), called)


class TestS3ApiBucketNoACL(BaseS3ApiBucket, S3ApiTestCase):

    def test_bucket_HEAD(self):
        req = Request.blank('/junk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def _do_test_bucket_HEAD_policy_index_logging(self, bucket_policy_index):
        self.logger.clear()
        self._register_bucket_policy_index_head('junk', bucket_policy_index)
        req = Request.blank('/junk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.s3api = ProxyLoggingMiddleware(self.s3api, {}, logger=self.logger)
        status, headers, body = self.call_s3api(req)
        self._assert_policy_index(req.headers, headers, bucket_policy_index)
        self.assertEqual('/v1/AUTH_test/junk',
                         req.environ['swift.backend_path'])
        access_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(1, len(access_lines))
        parts = access_lines[0].split()
        self.assertEqual(' '.join(parts[3:7]), 'HEAD /junk HTTP/1.0 200')
        self.assertEqual(parts[-1], str(bucket_policy_index))

    def test_bucket_HEAD_policy_index_logging(self):
        self._do_test_bucket_HEAD_policy_index_logging(0)
        self._do_test_bucket_HEAD_policy_index_logging(1)

    def test_bucket_HEAD_error(self):
        req = Request.blank('/nojunk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(body, b'')  # sanity

    def test_bucket_HEAD_503(self):
        req = Request.blank('/unavailable',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '503')
        self.assertEqual(body, b'')  # sanity

    def test_bucket_HEAD_slash(self):
        req = Request.blank('/junk/',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_bucket_HEAD_slash_error(self):
        req = Request.blank('/nojunk/',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')

    def test_bucket_GET(self):
        bucket_name = 'junk'
        req = Request.blank('/%s' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self._assert_policy_index(req.headers, headers, 3)
        self.assertEqual('/v1/AUTH_test/junk',
                         req.environ.get('swift.backend_path'))
        elem = fromstring(body, 'ListBucketResult')
        name = elem.find('./Name').text
        self.assertEqual(name, bucket_name)

        objects = elem.iterchildren('Contents')

        items = []
        for o in objects:
            items.append((o.find('./Key').text, o.find('./ETag').text))
            self.assertEqual('2011-01-05T02:19:15.000Z',
                             o.find('./LastModified').text)
        expected = [
            (i[0],
             PFS_ETAG if i[0] == 'pfs-obj' else
             '"0-N"' if i[0] == 'slo' else '"0"')
            for i in self.objects
        ]
        self.assertEqual(items, expected)

    def test_bucket_GET_last_modified_rounding(self):
        objects_list = [
            {'name': 'a', 'last_modified': '2011-01-05T02:19:59.275290',
             'content_type': 'application/octet-stream',
             'hash': 'ahash', 'bytes': '12345'},
            {'name': 'b', 'last_modified': '2011-01-05T02:19:59.000000',
             'content_type': 'application/octet-stream',
             'hash': 'ahash', 'bytes': '12345'},
        ]
        self.swift.register(
            'GET', '/v1/AUTH_test/junk',
            swob.HTTPOk, {'Content-Type': 'application/json'},
            json.dumps(objects_list))
        req = Request.blank('/junk',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListBucketResult')
        name = elem.find('./Name').text
        self.assertEqual(name, 'junk')
        objects = elem.iterchildren('Contents')
        actual = [(obj.find('./Key').text, obj.find('./LastModified').text)
                  for obj in objects]
        self.assertEqual(
            [('a', '2011-01-05T02:20:00.000Z'),
             ('b', '2011-01-05T02:19:59.000Z')],
            actual)

    def test_bucket_GET_url_encoded(self):
        bucket_name = 'junk'
        req = Request.blank('/%s?encoding-type=url' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListBucketResult')
        name = elem.find('./Name').text
        self.assertEqual(name, bucket_name)

        objects = elem.iterchildren('Contents')

        items = []
        for o in objects:
            items.append((o.find('./Key').text, o.find('./ETag').text))
            self.assertEqual('2011-01-05T02:19:15.000Z',
                             o.find('./LastModified').text)

        self.assertEqual(items, [
            (quote(i[0].encode('utf-8')),
             PFS_ETAG if i[0] == 'pfs-obj' else
             '"0-N"' if i[0] == 'slo' else '"0"')
            for i in self.objects])

    def test_bucket_GET_subdir(self):
        bucket_name = 'junk-subdir'
        req = Request.blank('/%s' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListBucketResult')
        name = elem.find('./Name').text
        self.assertEqual(name, bucket_name)

        prefixes = elem.findall('CommonPrefixes')

        self.assertEqual(len(prefixes), len(self.prefixes))
        for p in prefixes:
            self.assertTrue(p.find('./Prefix').text in self.prefixes)

    def test_bucket_GET_is_truncated(self):
        bucket_name = 'junk'

        req = Request.blank(
            '/%s?max-keys=%d' % (bucket_name, len(self.objects)),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')

        req = Request.blank(
            '/%s?max-keys=%d' % (bucket_name, len(self.objects) - 1),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

        req = Request.blank('/subdirs?delimiter=/&max-keys=2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')
        self.assertEqual(elem.find('./NextMarker').text,
                         u'but-\u062a/')

    def test_bucket_GET_is_truncated_url_encoded(self):
        bucket_name = 'junk'

        req = Request.blank(
            '/%s?encoding-type=url&max-keys=%d' % (
                bucket_name, len(self.objects)),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')

        req = Request.blank(
            '/%s?encoding-type=url&max-keys=%d' % (
                bucket_name, len(self.objects) - 1),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

        req = Request.blank('/subdirs?encoding-type=url&delimiter=/&'
                            'max-keys=2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')
        self.assertEqual(elem.find('./NextMarker').text,
                         quote(u'but-\u062a/'.encode('utf-8')))

    def test_bucket_GET_v2_is_truncated(self):
        bucket_name = 'junk'

        req = Request.blank(
            '/%s?list-type=2&max-keys=%d' % (bucket_name, len(self.objects)),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./KeyCount').text, str(len(self.objects)))
        self.assertEqual(elem.find('./IsTruncated').text, 'false')

        req = Request.blank(
            '/%s?list-type=2&max-keys=%d' % (bucket_name,
                                             len(self.objects) - 1),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertIsNotNone(elem.find('./NextContinuationToken'))
        self.assertEqual(elem.find('./KeyCount').text,
                         str(len(self.objects) - 1))
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

        req = Request.blank('/subdirs?list-type=2&delimiter=/&max-keys=2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertIsNotNone(elem.find('./NextContinuationToken'))
        self.assertEqual(elem.find('./KeyCount').text, '2')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

    def test_bucket_GET_max_keys(self):
        bucket_name = 'junk'

        req = Request.blank('/%s?max-keys=5' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./MaxKeys').text, '5')
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = dict(parse_qsl(query_string))
        self.assertEqual(args['limit'], '6')

        req = Request.blank('/%s?max-keys=5000' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./MaxKeys').text, '5000')
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = dict(parse_qsl(query_string))
        self.assertEqual(args['limit'], '1001')

    def test_bucket_GET_str_max_keys(self):
        bucket_name = 'junk'

        req = Request.blank('/%s?max-keys=invalid' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_bucket_GET_negative_max_keys(self):
        bucket_name = 'junk'

        req = Request.blank('/%s?max-keys=-1' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_bucket_GET_over_32bit_int_max_keys(self):
        bucket_name = 'junk'

        req = Request.blank('/%s?max-keys=%s' %
                            (bucket_name, MAX_32BIT_INT + 1),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_bucket_GET_passthroughs(self):
        bucket_name = 'junk'
        req = Request.blank('/%s?delimiter=a&marker=b&prefix=c' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./Prefix').text, 'c')
        self.assertEqual(elem.find('./Marker').text, 'b')
        self.assertEqual(elem.find('./Delimiter').text, 'a')
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = dict(parse_qsl(query_string))
        self.assertEqual(args['delimiter'], 'a')
        self.assertEqual(args['marker'], 'b')
        self.assertEqual(args['prefix'], 'c')

    def test_bucket_GET_v2_passthroughs(self):
        bucket_name = 'junk'
        req = Request.blank(
            '/%s?list-type=2&delimiter=a&start-after=b&prefix=c' % bucket_name,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./Prefix').text, 'c')
        self.assertEqual(elem.find('./StartAfter').text, 'b')
        self.assertEqual(elem.find('./Delimiter').text, 'a')
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = dict(parse_qsl(query_string))
        self.assertEqual(args['delimiter'], 'a')
        # "start-after" is converted to "marker"
        self.assertEqual(args['marker'], 'b')
        self.assertEqual(args['prefix'], 'c')

    def test_bucket_GET_with_nonascii_queries(self):
        bucket_name = 'junk'
        req = Request.blank(
            '/%s?delimiter=\xef\xbc\xa1&marker=\xef\xbc\xa2&'
            'prefix=\xef\xbc\xa3' % bucket_name,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./Prefix').text,
                         swob.wsgi_to_str('\xef\xbc\xa3'))
        self.assertEqual(elem.find('./Marker').text,
                         swob.wsgi_to_str('\xef\xbc\xa2'))
        self.assertEqual(elem.find('./Delimiter').text,
                         swob.wsgi_to_str('\xef\xbc\xa1'))
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = [part.partition('=')[::2] for part in query_string.split('&')]
        self.assertEqual(sorted(args), [
            ('delimiter', '%EF%BC%A1'),
            ('limit', '1001'),
            ('marker', '%EF%BC%A2'),
            ('prefix', '%EF%BC%A3'),
        ])

        req = Request.blank(
            '/%s?delimiter=\xef\xbc\xa1&marker=\xef\xbc\xa2&'
            'prefix=\xef\xbc\xa3&encoding-type=url' % bucket_name,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./Prefix').text, '%EF%BC%A3')
        self.assertEqual(elem.find('./Marker').text, '%EF%BC%A2')
        self.assertEqual(elem.find('./Delimiter').text, '%EF%BC%A1')
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = [part.partition('=')[::2] for part in query_string.split('&')]
        self.assertEqual(sorted(args), [
            ('delimiter', '%EF%BC%A1'),
            ('limit', '1001'),
            ('marker', '%EF%BC%A2'),
            ('prefix', '%EF%BC%A3'),
        ])

    def test_bucket_GET_v2_with_nonascii_queries(self):
        bucket_name = 'junk'
        req = Request.blank(
            '/%s?list-type=2&delimiter=\xef\xbc\xa1&start-after=\xef\xbc\xa2&'
            'prefix=\xef\xbc\xa3' % bucket_name,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./Prefix').text,
                         swob.wsgi_to_str('\xef\xbc\xa3'))
        self.assertEqual(elem.find('./StartAfter').text,
                         swob.wsgi_to_str('\xef\xbc\xa2'))
        self.assertEqual(elem.find('./Delimiter').text,
                         swob.wsgi_to_str('\xef\xbc\xa1'))
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = [part.partition('=')[::2] for part in query_string.split('&')]
        self.assertEqual(sorted(args), [
            ('delimiter', '%EF%BC%A1'),
            ('limit', '1001'),
            ('marker', '%EF%BC%A2'),
            ('prefix', '%EF%BC%A3'),
        ])

        req = Request.blank(
            '/%s?list-type=2&delimiter=\xef\xbc\xa1&start-after=\xef\xbc\xa2&'
            'prefix=\xef\xbc\xa3&encoding-type=url' % bucket_name,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./Prefix').text, '%EF%BC%A3')
        self.assertEqual(elem.find('./StartAfter').text, '%EF%BC%A2')
        self.assertEqual(elem.find('./Delimiter').text, '%EF%BC%A1')
        _, path = self.swift.calls[-1]
        _, query_string = path.split('?')
        args = [part.partition('=')[::2] for part in query_string.split('&')]
        self.assertEqual(sorted(args), [
            ('delimiter', '%EF%BC%A1'),
            ('limit', '1001'),
            ('marker', '%EF%BC%A2'),
            ('prefix', '%EF%BC%A3'),
        ])

    def test_bucket_GET_with_delimiter_max_keys(self):
        bucket_name = 'junk'
        req = Request.blank('/%s?delimiter=a&max-keys=4' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./NextMarker').text,
                         self.objects_list[3]['name'])
        self.assertEqual(elem.find('./MaxKeys').text, '4')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

    def test_bucket_GET_v2_with_delimiter_max_keys(self):
        bucket_name = 'junk'
        req = Request.blank(
            '/%s?list-type=2&delimiter=a&max-keys=2' % bucket_name,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListBucketResult')
        next_token = elem.find('./NextContinuationToken')
        self.assertIsNotNone(next_token)
        self.assertEqual(elem.find('./MaxKeys').text, '2')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

        req = Request.blank(
            '/%s?list-type=2&delimiter=a&max-keys=2&continuation-token=%s' %
            (bucket_name, next_token.text),
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListBucketResult')
        names = [o.find('./Key').text for o in elem.iterchildren('Contents')]
        self.assertEqual(names[0], 'lily')

    def test_bucket_GET_subdir_with_delimiter_max_keys(self):
        bucket_name = 'junk-subdir'
        req = Request.blank('/%s?delimiter=a&max-keys=1' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListBucketResult')
        self.assertEqual(elem.find('./NextMarker').text, 'rose')
        self.assertEqual(elem.find('./MaxKeys').text, '1')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')

    def test_bucket_GET_v2_fetch_owner(self):
        bucket_name = 'junk'
        req = Request.blank('/%s?list-type=2' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListBucketResult')
        name = elem.find('./Name').text
        self.assertEqual(name, bucket_name)

        objects = elem.iterchildren('Contents')
        for o in objects:
            self.assertIsNone(o.find('./Owner'))

        req = Request.blank('/%s?list-type=2&fetch-owner=true' % bucket_name,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListBucketResult')
        name = elem.find('./Name').text
        self.assertEqual(name, bucket_name)

        objects = elem.iterchildren('Contents')
        for o in objects:
            self.assertIsNotNone(o.find('./Owner'))

    def test_bucket_GET_with_versions_versioning_not_configured(self):
        for obj in self.objects:
            self.swift.register(
                'HEAD', '/v1/AUTH_test/junk/%s' % quote(obj[0].encode('utf8')),
                swob.HTTPOk, {}, None)

        self._add_versions_request(versioned_objects=[])
        req = Request.blank('/junk?versions',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./Name').text, 'junk')
        self.assertIsNone(elem.find('./Prefix').text)
        self.assertIsNone(elem.find('./KeyMarker').text)
        self.assertIsNone(elem.find('./VersionIdMarker').text)
        self.assertEqual(elem.find('./MaxKeys').text, '1000')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        self.assertEqual(elem.findall('./DeleteMarker'), [])
        versions = elem.findall('./Version')
        objects = list(self.objects)
        expected = [v[0] for v in objects]
        self.assertEqual([v.find('./Key').text for v in versions], expected)
        self.assertEqual([v.find('./IsLatest').text for v in versions],
                         ['true' for v in objects])
        self.assertEqual([v.find('./VersionId').text for v in versions],
                         ['null' for v in objects])
        # Last modified in self.objects is 2011-01-05T02:19:14.275290 but
        # the returned value is rounded up to 2011-01-05T02:19:15Z
        self.assertEqual([v.find('./LastModified').text for v in versions],
                         ['2011-01-05T02:19:15.000Z'] * len(objects))
        self.assertEqual([v.find('./ETag').text for v in versions],
                         [PFS_ETAG if v[0] == 'pfs-obj' else
                          '"0-N"' if v[0] == 'slo' else '"0"'
                          for v in objects])
        self.assertEqual([v.find('./Size').text for v in versions],
                         [str(v[3]) for v in objects])
        self.assertEqual([v.find('./Owner/ID').text for v in versions],
                         ['test:tester' for v in objects])
        self.assertEqual([v.find('./Owner/DisplayName').text
                          for v in versions],
                         ['test:tester' for v in objects])
        self.assertEqual([v.find('./StorageClass').text for v in versions],
                         ['STANDARD' for v in objects])

    def test_bucket_GET_with_versions(self):
        self._add_versions_request()
        req = Request.blank('/junk?versions',
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./Name').text, 'junk')
        self._assert_delete_markers(elem)
        versions = elem.findall('./Version')
        self.assertEqual(len(versions), len(self.objects) + 1)

        expected = []
        for o in self.objects_list:
            name = o['name']
            expected.append((name, 'true', 'null'))
            if name == 'rose':
                expected.append((name, 'false', '1'))
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in versions
        ]
        self.assertEqual(expected, discovered)

    def test_bucket_GET_with_versions_with_max_keys(self):
        self._add_versions_request()
        req = Request.blank('/junk?versions&max-keys=7',
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./MaxKeys').text, '7')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')
        self._assert_delete_markers(elem)
        versions = elem.findall('./Version')
        self.assertEqual(len(versions), 6)

        expected = []
        for o in self.objects_list[:5]:
            name = o['name']
            expected.append((name, 'true', 'null'))
            if name == 'rose':
                expected.append((name, 'false', '1'))
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in versions
        ]
        self.assertEqual(expected, discovered)

    def test_bucket_GET_with_versions_with_max_keys_and_key_marker(self):
        self._add_versions_request(orig_objects=self.objects_list[4:])
        req = Request.blank('/junk?versions&max-keys=3&key-marker=ros',
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./MaxKeys').text, '3')
        self.assertEqual(elem.find('./IsTruncated').text, 'true')
        self._assert_delete_markers(elem)
        versions = elem.findall('./Version')
        self.assertEqual(len(versions), 2)

        expected = [
            ('rose', 'true', 'null'),
            ('rose', 'false', '1'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in versions
        ]
        self.assertEqual(expected, discovered)

    def test_bucket_GET_versions_with_key_marker_and_version_id_marker(self):
        container_listing = [{
            "bytes": 8192,
            "content_type": "binary/octet-stream",
            "hash": "221994040b14294bdf7fbc128e66633c",
            "last_modified": "2019-08-16T19:39:53.152780",
            "name": "subdir/foo",
        }]
        versions_listing = [{
            'bytes': 0,
            'content_type': DELETE_MARKER_CONTENT_TYPE,
            'hash': '0',
            "last_modified": "2019-08-19T19:05:33.565940",
            'name': 'subdir/bar',
            "version_id": "1565241533.55320",
            'is_latest': True,
        }, {
            "bytes": 8192,
            "content_type": "binary/octet-stream",
            "hash": "221994040b14294bdf7fbc128e66633c",
            "last_modified": "2019-08-16T19:39:53.508510",
            "name": "subdir/bar",
            "version_id": "1564984393.68962",
            'is_latest': False,
        }, {
            "bytes": 8192,
            "content_type": "binary/octet-stream",
            "hash": "221994040b14294bdf7fbc128e66633c",
            "last_modified": "2019-08-16T19:39:42.673260",
            "name": "subdir/foo",
            "version_id": "1565984382.67326",
            'is_latest': False,
        }]
        self._add_versions_request(container_listing, versions_listing,
                                   bucket='mybucket')
        req = Request.blank(
            '/mybucket?versions&key-marker=subdir/bar&'
            'version-id-marker=1566589611.065522',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(['subdir/bar'], [
            o.find('Key').text for o in delete_markers])
        expected = [
            ('subdir/bar', 'false', '1564984393.68962'),
            ('subdir/foo', 'true', 'null'),
            ('subdir/foo', 'false', '1565984382.67326'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

        self._add_versions_request(container_listing, versions_listing[1:],
                                   bucket='mybucket')
        req = Request.blank(
            '/mybucket?versions&key-marker=subdir/bar&'
            'version-id-marker=1565241533.55320',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(0, len(delete_markers))
        expected = [
            ('subdir/bar', 'false', '1564984393.68962'),
            ('subdir/foo', 'true', 'null'),
            ('subdir/foo', 'false', '1565984382.67326'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

        self._add_versions_request([], versions_listing[-1:],
                                   bucket='mybucket')
        req = Request.blank(
            '/mybucket?versions&key-marker=subdir/foo&'
            'version-id-marker=null',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(0, len(delete_markers))
        expected = [
            ('subdir/foo', 'false', '1565984382.67326'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

    def test_bucket_GET_versions_with_version_id_marker(self):
        self._add_versions_request()
        req = Request.blank(
            '/junk?versions',
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        # sanity
        elem = fromstring(body, 'ListVersionsResult')
        expected = [('rose', 'false', '2')]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./DeleteMarker')
        ]
        self.assertEqual(expected, discovered)
        expected = [
            ('lily', 'true', 'null'),
            ('lily-\u062a', 'true', 'null'),
            ('mu', 'true', 'null'),
            ('pfs-obj', 'true', 'null'),
            ('rose', 'true', 'null'),
            ('rose', 'false', '1'),
            ('slo', 'true', 'null'),
            ('viola', 'true', 'null'),
            ('with space', 'true', 'null'),
            ('with%20space', 'true', 'null'),
        ]

        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

        self._add_versions_request(self.objects_list[5:])
        req = Request.blank(
            '/junk?versions&key-marker=rose&version-id-marker=null',
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(len(delete_markers), 1)

        expected = [
            ('rose', 'false', '1'),
            ('slo', 'true', 'null'),
            ('viola', 'true', 'null'),
            ('with space', 'true', 'null'),
            ('with%20space', 'true', 'null'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

        # N.B. versions are sorted most recent to oldest
        self._add_versions_request(self.objects_list[5:],
                                   self.versioned_objects[1:])
        req = Request.blank(
            '/junk?versions&key-marker=rose&version-id-marker=2',
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(len(delete_markers), 0)

        expected = [
            ('rose', 'false', '1'),
            ('slo', 'true', 'null'),
            ('viola', 'true', 'null'),
            ('with space', 'true', 'null'),
            ('with%20space', 'true', 'null'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

        self._add_versions_request(self.objects_list[5:],
                                   self.versioned_objects[2:])
        req = Request.blank(
            '/junk?versions&key-marker=rose&version-id-marker=1',
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./IsTruncated').text, 'false')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(len(delete_markers), 0)

        expected = [
            ('slo', 'true', 'null'),
            ('viola', 'true', 'null'),
            ('with space', 'true', 'null'),
            ('with%20space', 'true', 'null'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

    def test_bucket_GET_versions_non_existent_version_id_marker(self):
        self._add_versions_request(orig_objects=self.objects_list[5:])
        req = Request.blank(
            '/junk?versions&key-marker=rose&'
            'version-id-marker=null',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200', body)
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./Name').text, 'junk')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(len(delete_markers), 1)

        expected = [
            ('rose', 'false', '1'),
            ('slo', 'true', 'null'),
            ('viola', 'true', 'null'),
            ('with space', 'true', 'null'),
            ('with%20space', 'true', 'null'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)
        self.assertEqual(self.swift.calls, [
            ('GET', normalize_path('/v1/AUTH_test/junk?'
             'limit=1001&marker=rose&version_marker=null&versions=')),
        ])

    def test_bucket_GET_versions_prefix(self):
        container_listing = [{
            "bytes": 8192,
            "content_type": "binary/octet-stream",
            "hash": "221994040b14294bdf7fbc128e66633c",
            "last_modified": "2019-08-16T19:39:53.152780",
            "name": "subdir/foo",
        }]
        versions_listing = [{
            "bytes": 8192,
            "content_type": "binary/octet-stream",
            "hash": "221994040b14294bdf7fbc128e66633c",
            "last_modified": "2019-08-16T19:39:53.508510",
            "name": "subdir/bar",
            "version_id": "1565984393.68962",
            "is_latest": True,
        }, {
            'bytes': 0,
            'content_type': DELETE_MARKER_CONTENT_TYPE,
            'hash': '0',
            "last_modified": "2019-08-19T19:05:33.565940",
            'name': 'subdir/bar',
            'version_id': '1566241533.55320',
            'is_latest': False,
        }, {
            "bytes": 8192,
            "content_type": "binary/octet-stream",
            "hash": "221994040b14294bdf7fbc128e66633c",
            "last_modified": "2019-08-16T19:39:42.673260",
            "name": "subdir/foo",
            "version_id": "1565984382.67326",
            'is_latest': False,
        }]
        self._add_versions_request(container_listing, versions_listing)
        req = Request.blank(
            '/junk?versions&prefix=subdir/',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListVersionsResult')
        self.assertEqual(elem.find('./Name').text, 'junk')
        delete_markers = elem.findall('./DeleteMarker')
        self.assertEqual(len(delete_markers), 1)

        expected = [
            ('subdir/bar', 'true', '1565984393.68962'),
            ('subdir/foo', 'true', 'null'),
            ('subdir/foo', 'false', '1565984382.67326'),
        ]
        discovered = [
            tuple(e.find('./%s' % key).text for key in (
                'Key', 'IsLatest', 'VersionId'))
            for e in elem.findall('./Version')
        ]
        self.assertEqual(expected, discovered)

        self.assertEqual(self.swift.calls, [
            ('GET', normalize_path('/v1/AUTH_test/junk'
             '?limit=1001&prefix=subdir/&versions=')),
        ])

    def test_bucket_PUT_with_mixed_case_location(self):
        self.s3api.conf.location = 'RegionOne'
        elem = Element('CreateBucketConfiguration')
        # We've observed some clients (like aws-sdk-net) shift regions
        # to lower case
        SubElement(elem, 'LocationConstraint').text = 'regionone'
        headers = {
            'Authorization': 'AWS4-HMAC-SHA256 ' + ', '.join([
                'Credential=test:tester/%s/regionone/s3/aws4_request' %
                self.get_v4_amz_date_header().split('T', 1)[0],
                'SignedHeaders=host',
                'Signature=X',
            ]),
            'Date': self.get_date_header(),
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
        }
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers,
                            body=tostring(elem))
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)

    def test_bucket_PUT_v4_with_body(self):
        elem = Element('CreateBucketConfiguration')
        SubElement(elem, 'LocationConstraint').text = self.s3api.conf.location
        req_body = tostring(elem)
        body_sha = sha256(req_body).hexdigest()
        headers = {
            'Authorization': 'AWS4-HMAC-SHA256 ' + ', '.join([
                'Credential=test:tester/%s/us-east-1/s3/aws4_request' %
                self.get_v4_amz_date_header().split('T', 1)[0],
                'SignedHeaders=host',
                'Signature=X',
            ]),
            'Date': self.get_date_header(),
            'x-amz-content-sha256': body_sha,
        }
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers,
                            body=req_body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)

    def test_bucket_PUT_v4_with_body_bad_hash(self):
        elem = Element('CreateBucketConfiguration')
        SubElement(elem, 'LocationConstraint').text = self.s3api.conf.location
        req_body = tostring(elem)
        headers = {
            'Authorization': 'AWS4-HMAC-SHA256 ' + ', '.join([
                'Credential=test:tester/%s/us-east-1/s3/aws4_request' %
                self.get_v4_amz_date_header().split('T', 1)[0],
                'SignedHeaders=host',
                'Signature=X',
            ]),
            'Date': self.get_date_header(),
            'x-amz-content-sha256': '0' * 64,
        }
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers,
                            body=req_body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body),
                         'XAmzContentSHA256Mismatch')
        self.assertIn(b'x-amz-content-sha256', body)
        # we maybe haven't parsed the location/path yet?
        self.assertNotIn('swift.backend_path', req.environ)

    def test_bucket_PUT_with_canned_acl(self):
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-Amz-Acl': 'public-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue('X-Container-Read' in headers)
        self.assertEqual(headers.get('X-Container-Read'), '.r:*,.rlistings')
        self.assertNotIn('X-Container-Sysmeta-S3api-Acl', headers)


class TestS3ApiBucketAcl(BaseS3ApiBucket, S3ApiTestCaseAcl):
    def test_bucket_PUT_with_canned_s3acl(self):
        account = 'test:tester'
        acl = \
            encode_acl('container', ACLPublicRead(Owner(account, account)))
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-Amz-Acl': 'public-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertNotIn('X-Container-Read', headers)
        self.assertIn('X-Container-Sysmeta-S3api-Acl', headers)
        self.assertEqual(headers.get('X-Container-Sysmeta-S3api-Acl'),
                         acl['x-container-sysmeta-s3api-acl'])

    def test_bucket_PUT_error_non_swift_owner(self):
        code = self._test_method_error('PUT', '/bucket', swob.HTTPAccepted,
                                       env={'swift_owner': False})
        self.assertEqual(code, 'AccessDenied')

    def _test_bucket_for_s3acl(self, method, account):
        req = Request.blank('/bucket',
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()})

        return self.call_s3api(req)

    def test_bucket_GET_without_permission(self):
        status, headers, body = self._test_bucket_for_s3acl('GET',
                                                            'test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_bucket_GET_with_read_permission(self):
        status, headers, body = self._test_bucket_for_s3acl('GET',
                                                            'test:read')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_GET_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_bucket_for_s3acl('GET', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_GET_with_owner_permission(self):
        status, headers, body = self._test_bucket_for_s3acl('GET',
                                                            'test:tester')
        self.assertEqual(status.split()[0], '200')

    def _test_bucket_GET_canned_acl(self, bucket):
        req = Request.blank('/%s' % bucket,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        return self.call_s3api(req)

    def test_bucket_GET_authenticated_users(self):
        status, headers, body = \
            self._test_bucket_GET_canned_acl('authenticated')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_GET_all_users(self):
        status, headers, body = self._test_bucket_GET_canned_acl('public')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_DELETE_without_permission(self):
        status, headers, body = self._test_bucket_for_s3acl('DELETE',
                                                            'test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        # Don't delete anything in backend Swift
        called = [method for method, _, _ in self.swift.calls_with_headers]
        self.assertNotIn('DELETE', called)

    def test_bucket_DELETE_with_write_permission(self):
        status, headers, body = self._test_bucket_for_s3acl('DELETE',
                                                            'test:write')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        # Don't delete anything in backend Swift
        called = [method for method, _, _ in self.swift.calls_with_headers]
        self.assertNotIn('DELETE', called)

    def test_bucket_DELETE_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_bucket_for_s3acl('DELETE', 'test:full_control')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        # Don't delete anything in backend Swift
        called = [method for method, _, _ in self.swift.calls_with_headers]
        self.assertNotIn('DELETE', called)


if __name__ == '__main__':
    unittest.main()
