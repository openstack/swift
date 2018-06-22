# Copyright (c) 2010-2012 OpenStack Foundation
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
import mock
import tempfile
import time

from six.moves import range
from test.unit import mock_check_drive

from swift.common.swob import Request, HTTPException
from swift.common.http import HTTP_REQUEST_ENTITY_TOO_LARGE, \
    HTTP_BAD_REQUEST, HTTP_LENGTH_REQUIRED, HTTP_NOT_IMPLEMENTED
from swift.common import constraints, utils
from swift.common.constraints import MAX_OBJECT_NAME_LENGTH


class TestConstraints(unittest.TestCase):
    def test_check_metadata_empty(self):
        headers = {}
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))

    def test_check_metadata_good(self):
        headers = {'X-Object-Meta-Name': 'Value'}
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))

    def test_check_metadata_empty_name(self):
        headers = {'X-Object-Meta-': 'Value'}
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Metadata name cannot be empty', resp.body)

    def test_check_metadata_non_utf8(self):
        # Consciously using native "WSGI strings" in headers
        headers = {'X-Account-Meta-Foo': '\xff'}
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'account')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Metadata must be valid UTF-8', resp.body)

        headers = {'X-Container-Meta-\xff': 'foo'}
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'container')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Metadata must be valid UTF-8', resp.body)
        # Object's OK; its metadata isn't serialized as JSON
        headers = {'X-Object-Meta-Foo': '\xff'}
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))

    def test_check_metadata_name_length(self):
        name = 'a' * constraints.MAX_META_NAME_LENGTH
        headers = {'X-Object-Meta-%s' % name: 'v'}
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))

        name = 'a' * (constraints.MAX_META_NAME_LENGTH + 1)
        headers = {'X-Object-Meta-%s' % name: 'v'}
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(
            b'x-object-meta-%s' % name.encode('ascii'), resp.body.lower())
        self.assertIn(b'Metadata name too long', resp.body)

    def test_check_metadata_value_length(self):
        value = 'a' * constraints.MAX_META_VALUE_LENGTH
        headers = {'X-Object-Meta-Name': value}
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))

        value = 'a' * (constraints.MAX_META_VALUE_LENGTH + 1)
        headers = {'X-Object-Meta-Name': value}
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'x-object-meta-name', resp.body.lower())
        self.assertIn(
            str(constraints.MAX_META_VALUE_LENGTH).encode('ascii'), resp.body)
        self.assertIn(b'Metadata value longer than 256', resp.body)

    def test_check_metadata_count(self):
        headers = {}
        for x in range(constraints.MAX_META_COUNT):
            headers['X-Object-Meta-%d' % x] = 'v'
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))

        headers['X-Object-Meta-Too-Many'] = 'v'
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Too many metadata items', resp.body)

    def test_check_metadata_size(self):
        headers = {}
        size = 0
        chunk = constraints.MAX_META_NAME_LENGTH + \
            constraints.MAX_META_VALUE_LENGTH
        x = 0
        while size + chunk < constraints.MAX_META_OVERALL_SIZE:
            headers['X-Object-Meta-%04d%s' %
                    (x, 'a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
                'v' * constraints.MAX_META_VALUE_LENGTH
            size += chunk
            x += 1
        self.assertIsNone(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'))
        # add two more headers in case adding just one falls exactly on the
        # limit (eg one header adds 1024 and the limit is 2048)
        headers['X-Object-Meta-%04d%s' %
                (x, 'a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
            'v' * constraints.MAX_META_VALUE_LENGTH
        headers['X-Object-Meta-%04d%s' %
                (x + 1, 'a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
            'v' * constraints.MAX_META_VALUE_LENGTH
        resp = constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Total metadata too large', resp.body)

    def test_check_object_creation_content_length(self):
        headers = {'Content-Length': str(constraints.MAX_FILE_SIZE),
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        self.assertIsNone(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'))

        headers = {'Content-Length': str(constraints.MAX_FILE_SIZE + 1),
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_REQUEST_ENTITY_TOO_LARGE)

        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        self.assertIsNone(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'))

        headers = {'Transfer-Encoding': 'gzip',
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Invalid Transfer-Encoding header value', resp.body)

        headers = {'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_LENGTH_REQUIRED)

        headers = {'Content-Length': 'abc',
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Invalid Content-Length header value', resp.body)

        headers = {'Transfer-Encoding': 'gzip,chunked',
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_NOT_IMPLEMENTED)

    def test_check_object_creation_name_length(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        name = 'o' * constraints.MAX_OBJECT_NAME_LENGTH
        self.assertIsNone(constraints.check_object_creation(Request.blank(
            '/', headers=headers), name))

        name = 'o' * (MAX_OBJECT_NAME_LENGTH + 1)
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), name)
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Object name length of %d longer than %d' %
                      (MAX_OBJECT_NAME_LENGTH + 1, MAX_OBJECT_NAME_LENGTH),
                      resp.body)

    def test_check_object_creation_content_type(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Timestamp': str(time.time())}
        self.assertIsNone(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'))

        headers = {'Transfer-Encoding': 'chunked',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'No content type', resp.body)

    def test_check_object_creation_bad_content_type(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': '\xff\xff',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Content-Type', resp.body)

    def test_check_object_creation_bad_delete_headers(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': 'abc',
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Non-integer X-Delete-After', resp.body)

        t = str(int(time.time() - 60))
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Delete-At': t,
                   'X-Timestamp': str(time.time())}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEqual(resp.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'X-Delete-At in past', resp.body)

    def test_check_delete_headers(self):
        # x-delete-at value should be relative to the request timestamp rather
        # than time.time() so separate the two to ensure the checks are robust
        ts = utils.Timestamp(time.time() + 100)

        # X-Delete-After
        headers = {'X-Delete-After': '600',
                   'X-Timestamp': ts.internal}
        req = constraints.check_delete_headers(
            Request.blank('/', headers=headers))
        self.assertIsInstance(req, Request)
        self.assertIn('x-delete-at', req.headers)
        self.assertNotIn('x-delete-after', req.headers)
        expected_delete_at = str(int(ts) + 600)
        self.assertEqual(req.headers.get('X-Delete-At'), expected_delete_at)

        headers = {'X-Delete-After': 'abc',
                   'X-Timestamp': ts.internal}

        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Non-integer X-Delete-After', cm.exception.body)

        headers = {'X-Delete-After': '60.1',
                   'X-Timestamp': ts.internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Non-integer X-Delete-After', cm.exception.body)

        headers = {'X-Delete-After': '-1',
                   'X-Timestamp': ts.internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'X-Delete-After in past', cm.exception.body)

        headers = {'X-Delete-After': '0',
                   'X-Timestamp': ts.internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'X-Delete-After in past', cm.exception.body)

        # x-delete-after = 0 disallowed when it results in x-delete-at equal to
        # the timestamp
        headers = {'X-Delete-After': '0',
                   'X-Timestamp': utils.Timestamp(int(ts)).internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'X-Delete-After in past', cm.exception.body)

        # X-Delete-At
        delete_at = str(int(ts) + 100)
        headers = {'X-Delete-At': delete_at,
                   'X-Timestamp': ts.internal}
        req = constraints.check_delete_headers(
            Request.blank('/', headers=headers))
        self.assertIsInstance(req, Request)
        self.assertIn('x-delete-at', req.headers)
        self.assertEqual(req.headers.get('X-Delete-At'), delete_at)

        headers = {'X-Delete-At': 'abc',
                   'X-Timestamp': ts.internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Non-integer X-Delete-At', cm.exception.body)

        delete_at = str(int(ts) + 100) + '.1'
        headers = {'X-Delete-At': delete_at,
                   'X-Timestamp': ts.internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'Non-integer X-Delete-At', cm.exception.body)

        delete_at = str(int(ts) - 1)
        headers = {'X-Delete-At': delete_at,
                   'X-Timestamp': ts.internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'X-Delete-At in past', cm.exception.body)

        # x-delete-at disallowed when exactly equal to timestamp
        delete_at = str(int(ts))
        headers = {'X-Delete-At': delete_at,
                   'X-Timestamp': utils.Timestamp(int(ts)).internal}
        with self.assertRaises(HTTPException) as cm:
            constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        self.assertEqual(cm.exception.status_int, HTTP_BAD_REQUEST)
        self.assertIn(b'X-Delete-At in past', cm.exception.body)

    def test_check_delete_headers_removes_delete_after(self):
        ts = utils.Timestamp.now()
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42',
                   'X-Delete-At': str(int(ts) + 40),
                   'X-Timestamp': ts.internal}
        req = Request.blank('/', headers=headers)
        constraints.check_delete_headers(req)
        self.assertNotIn('X-Delete-After', req.headers)
        self.assertEqual(req.headers['X-Delete-At'], str(int(ts) + 42))

    def test_check_delete_headers_sets_delete_at(self):
        ts = utils.Timestamp.now()
        expected = str(int(ts) + 1000)
        # check delete-at is passed through
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-At': expected,
                   'X-Timestamp': ts.internal}
        req = Request.blank('/', headers=headers)
        constraints.check_delete_headers(req)
        self.assertIn('X-Delete-At', req.headers)
        self.assertEqual(req.headers['X-Delete-At'], expected)

        # check delete-after is converted to delete-at
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42',
                   'X-Timestamp': ts.internal}
        req = Request.blank('/', headers=headers)
        constraints.check_delete_headers(req)
        self.assertIn('X-Delete-At', req.headers)
        expected = str(int(ts) + 42)
        self.assertEqual(req.headers['X-Delete-At'], expected)

        # check delete-after takes precedence over delete-at
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42',
                   'X-Delete-At': str(int(ts) + 40),
                   'X-Timestamp': ts.internal}
        req = Request.blank('/', headers=headers)
        constraints.check_delete_headers(req)
        self.assertIn('X-Delete-At', req.headers)
        self.assertEqual(req.headers['X-Delete-At'], expected)

        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42',
                   'X-Delete-At': str(int(ts) + 44),
                   'X-Timestamp': ts.internal}
        req = Request.blank('/', headers=headers)
        constraints.check_delete_headers(req)
        self.assertIn('X-Delete-At', req.headers)
        self.assertEqual(req.headers['X-Delete-At'], expected)

    def test_check_drive_invalid_path(self):
        root = '/srv/'
        with mock_check_drive() as mocks:
            drive = 'foo?bar'
            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_dir(root, drive)
            self.assertEqual(str(exc_mgr.exception),
                             '%s is not a valid drive name' % drive)

            drive = 'foo bar'
            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_mount(root, drive)
            self.assertEqual(str(exc_mgr.exception),
                             '%s is not a valid drive name' % drive)

            drive = 'foo/bar'
            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_drive(root, drive, True)
            self.assertEqual(str(exc_mgr.exception),
                             '%s is not a valid drive name' % drive)

            drive = 'foo%bar'
            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_drive(root, drive, False)
            self.assertEqual(str(exc_mgr.exception),
                             '%s is not a valid drive name' % drive)
        self.assertEqual([], mocks['isdir'].call_args_list)
        self.assertEqual([], mocks['ismount'].call_args_list)

    def test_check_drive_ismount(self):
        root = '/srv'
        path = 'sdb1'
        with mock_check_drive(ismount=True) as mocks:
            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_dir(root, path)
            self.assertEqual(str(exc_mgr.exception),
                             '/srv/sdb1 is not a directory')

            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_drive(root, path, False)
            self.assertEqual(str(exc_mgr.exception),
                             '/srv/sdb1 is not a directory')

            self.assertEqual([mock.call('/srv/sdb1'), mock.call('/srv/sdb1')],
                             mocks['isdir'].call_args_list)
            self.assertEqual([], mocks['ismount'].call_args_list)

        with mock_check_drive(ismount=True) as mocks:
            self.assertEqual('/srv/sdb1', constraints.check_mount(root, path))
            self.assertEqual('/srv/sdb1', constraints.check_drive(
                root, path, True))
            self.assertEqual([], mocks['isdir'].call_args_list)
            self.assertEqual([mock.call('/srv/sdb1'), mock.call('/srv/sdb1')],
                             mocks['ismount'].call_args_list)

    def test_check_drive_isdir(self):
        root = '/srv'
        path = 'sdb2'
        with mock_check_drive(isdir=True) as mocks:
            self.assertEqual('/srv/sdb2', constraints.check_dir(root, path))
            self.assertEqual('/srv/sdb2', constraints.check_drive(
                root, path, False))
            self.assertEqual([mock.call('/srv/sdb2'), mock.call('/srv/sdb2')],
                             mocks['isdir'].call_args_list)
            self.assertEqual([], mocks['ismount'].call_args_list)

        with mock_check_drive(isdir=True) as mocks:
            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_mount(root, path)
            self.assertEqual(str(exc_mgr.exception),
                             '/srv/sdb2 is not mounted')

            with self.assertRaises(ValueError) as exc_mgr:
                constraints.check_drive(root, path, True)
            self.assertEqual(str(exc_mgr.exception),
                             '/srv/sdb2 is not mounted')

            self.assertEqual([], mocks['isdir'].call_args_list)
            self.assertEqual([mock.call('/srv/sdb2'), mock.call('/srv/sdb2')],
                             mocks['ismount'].call_args_list)

    def test_check_float(self):
        self.assertFalse(constraints.check_float(''))
        self.assertTrue(constraints.check_float('0'))

    def test_valid_timestamp(self):
        self.assertRaises(HTTPException,
                          constraints.valid_timestamp,
                          Request.blank('/'))
        self.assertRaises(HTTPException,
                          constraints.valid_timestamp,
                          Request.blank('/', headers={
                              'X-Timestamp': 'asdf'}))
        timestamp = utils.Timestamp.now()
        req = Request.blank('/', headers={'X-Timestamp': timestamp.internal})
        self.assertEqual(timestamp, constraints.valid_timestamp(req))
        req = Request.blank('/', headers={'X-Timestamp': timestamp.normal})
        self.assertEqual(timestamp, constraints.valid_timestamp(req))

    def test_check_utf8(self):
        unicode_sample = u'\uc77c\uc601'
        unicode_with_null = u'abc\u0000def'

        # Some false-y values
        self.assertFalse(constraints.check_utf8(None))
        self.assertFalse(constraints.check_utf8(''))
        self.assertFalse(constraints.check_utf8(b''))
        self.assertFalse(constraints.check_utf8(u''))

        # invalid utf8 bytes
        self.assertFalse(constraints.check_utf8(
            unicode_sample.encode('utf-8')[::-1]))
        # unicode with null
        self.assertFalse(constraints.check_utf8(unicode_with_null))
        # utf8 bytes with null
        self.assertFalse(constraints.check_utf8(
            unicode_with_null.encode('utf8')))

        self.assertTrue(constraints.check_utf8('this is ascii and utf-8, too'))
        self.assertTrue(constraints.check_utf8(unicode_sample))
        self.assertTrue(constraints.check_utf8(unicode_sample.encode('utf8')))

    def test_check_utf8_non_canonical(self):
        self.assertFalse(constraints.check_utf8(b'\xed\xa0\xbc\xed\xbc\xb8'))
        self.assertTrue(constraints.check_utf8(u'\U0001f338'))
        self.assertTrue(constraints.check_utf8(b'\xf0\x9f\x8c\xb8'))
        self.assertTrue(constraints.check_utf8(u'\U0001f338'.encode('utf8')))
        self.assertFalse(constraints.check_utf8(b'\xed\xa0\xbd\xed\xb9\x88'))
        self.assertTrue(constraints.check_utf8(u'\U0001f648'))

    def test_check_utf8_lone_surrogates(self):
        self.assertFalse(constraints.check_utf8(b'\xed\xa0\xbc'))
        self.assertFalse(constraints.check_utf8(u'\ud83c'))
        self.assertFalse(constraints.check_utf8(b'\xed\xb9\x88'))
        self.assertFalse(constraints.check_utf8(u'\ude48'))

        self.assertFalse(constraints.check_utf8(u'\ud800'))
        self.assertFalse(constraints.check_utf8(u'\udc00'))
        self.assertFalse(constraints.check_utf8(u'\udcff'))
        self.assertFalse(constraints.check_utf8(u'\udfff'))

    def test_validate_bad_meta(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-object-meta-hello':
                     'ab' * constraints.MAX_HEADER_SIZE})
        self.assertEqual(constraints.check_metadata(req, 'object').status_int,
                         HTTP_BAD_REQUEST)
        resp = constraints.check_metadata(req, 'object')
        self.assertIsNotNone(resp)
        self.assertIn(b'x-object-meta-hello', resp.body.lower())

    def test_validate_constraints(self):
        c = constraints
        self.assertGreater(c.MAX_META_OVERALL_SIZE, c.MAX_META_NAME_LENGTH)
        self.assertGreater(c.MAX_META_OVERALL_SIZE, c.MAX_META_VALUE_LENGTH)
        self.assertGreater(c.MAX_HEADER_SIZE, c.MAX_META_NAME_LENGTH)
        self.assertGreater(c.MAX_HEADER_SIZE, c.MAX_META_VALUE_LENGTH)

    def test_check_account_format(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'X-Copy-From-Account': 'account/with/slashes'})
        self.assertRaises(HTTPException,
                          constraints.check_account_format,
                          req, req.headers['X-Copy-From-Account'])
        req = Request.blank(
            '/v/a/c/o',
            headers={'X-Copy-From-Account': ''})
        self.assertRaises(HTTPException,
                          constraints.check_account_format,
                          req, req.headers['X-Copy-From-Account'])

    def test_check_container_format(self):
        invalid_versions_locations = (
            'container/with/slashes',
            '',  # empty
        )
        for versions_location in invalid_versions_locations:
            req = Request.blank(
                '/v/a/c/o', headers={
                    'X-Versions-Location': versions_location})
            with self.assertRaises(HTTPException) as cm:
                constraints.check_container_format(
                    req, req.headers['X-Versions-Location'])
            self.assertTrue(cm.exception.body.startswith(
                b'Container name cannot'))

    def test_valid_api_version(self):
        version = 'v1'
        self.assertTrue(constraints.valid_api_version(version))

        version = 'v1.0'
        self.assertTrue(constraints.valid_api_version(version))

        version = 'v2'
        self.assertFalse(constraints.valid_api_version(version))


class TestConstraintsConfig(unittest.TestCase):

    def test_default_constraints(self):
        for key in constraints.DEFAULT_CONSTRAINTS:
            # if there is local over-rides in swift.conf we just continue on
            if key in constraints.OVERRIDE_CONSTRAINTS:
                continue
            # module level attrs (that aren't in OVERRIDE) should have the
            # same value as the DEFAULT map
            module_level_value = getattr(constraints, key.upper())
            self.assertEqual(constraints.DEFAULT_CONSTRAINTS[key],
                             module_level_value)

    def test_effective_constraints(self):
        for key in constraints.DEFAULT_CONSTRAINTS:
            # module level attrs should always mirror the same value as the
            # EFFECTIVE map
            module_level_value = getattr(constraints, key.upper())
            self.assertEqual(constraints.EFFECTIVE_CONSTRAINTS[key],
                             module_level_value)
            # if there are local over-rides in swift.conf those should be
            # reflected in the EFFECTIVE, otherwise we expect the DEFAULTs
            self.assertEqual(constraints.EFFECTIVE_CONSTRAINTS[key],
                             constraints.OVERRIDE_CONSTRAINTS.get(
                                 key, constraints.DEFAULT_CONSTRAINTS[key]))

    def test_override_constraints(self):
        try:
            with tempfile.NamedTemporaryFile() as f:
                f.write(b'[swift-constraints]\n')
                # set everything to 1
                for key in constraints.DEFAULT_CONSTRAINTS:
                    f.write(b'%s = 1\n' % key.encode('ascii'))
                f.flush()
                with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                    constraints.reload_constraints()
            for key in constraints.DEFAULT_CONSTRAINTS:
                # module level attrs should all be 1
                module_level_value = getattr(constraints, key.upper())
                self.assertEqual(module_level_value, 1)
                # all keys should be in OVERRIDE
                self.assertEqual(constraints.OVERRIDE_CONSTRAINTS[key],
                                 module_level_value)
                # module level attrs should always mirror the same value as
                # the EFFECTIVE map
                self.assertEqual(constraints.EFFECTIVE_CONSTRAINTS[key],
                                 module_level_value)
        finally:
            constraints.reload_constraints()

    def test_reload_reset(self):
        try:
            with tempfile.NamedTemporaryFile() as f:
                f.write(b'[swift-constraints]\n')
                # set everything to 1
                for key in constraints.DEFAULT_CONSTRAINTS:
                    f.write(b'%s = 1\n' % key.encode('ascii'))
                f.flush()
                with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                    constraints.reload_constraints()
            self.assertTrue(constraints.SWIFT_CONSTRAINTS_LOADED)
            self.assertEqual(sorted(constraints.DEFAULT_CONSTRAINTS.keys()),
                             sorted(constraints.OVERRIDE_CONSTRAINTS.keys()))
            # file is now deleted...
            with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                constraints.reload_constraints()
            # no constraints have been loaded from non-existent swift.conf
            self.assertFalse(constraints.SWIFT_CONSTRAINTS_LOADED)
            # no constraints are in OVERRIDE
            self.assertEqual([], list(constraints.OVERRIDE_CONSTRAINTS.keys()))
            # the EFFECTIVE constraints mirror DEFAULT
            self.assertEqual(constraints.EFFECTIVE_CONSTRAINTS,
                             constraints.DEFAULT_CONSTRAINTS)
        finally:
            constraints.reload_constraints()


if __name__ == '__main__':
    unittest.main()
