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

from test import safe_repr
from test.unit import MockTrue

from swift.common.swob import HTTPBadRequest, Request, HTTPException
from swift.common.http import HTTP_REQUEST_ENTITY_TOO_LARGE, \
    HTTP_BAD_REQUEST, HTTP_LENGTH_REQUIRED, HTTP_NOT_IMPLEMENTED
from swift.common import constraints, utils


class TestConstraints(unittest.TestCase):

    def assertIn(self, member, container, msg=None):
        """Copied from 2.7"""
        if member not in container:
            standardMsg = '%s not found in %s' % (safe_repr(member),
                                                  safe_repr(container))
            self.fail(self._formatMessage(msg, standardMsg))

    def test_check_metadata_empty(self):
        headers = {}
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), None)

    def test_check_metadata_good(self):
        headers = {'X-Object-Meta-Name': 'Value'}
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), None)

    def test_check_metadata_empty_name(self):
        headers = {'X-Object-Meta-': 'Value'}
        self.assert_(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), HTTPBadRequest)

    def test_check_metadata_name_length(self):
        name = 'a' * constraints.MAX_META_NAME_LENGTH
        headers = {'X-Object-Meta-%s' % name: 'v'}
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), None)
        name = 'a' * (constraints.MAX_META_NAME_LENGTH + 1)
        headers = {'X-Object-Meta-%s' % name: 'v'}
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object').status_int, HTTP_BAD_REQUEST)
        self.assertIn(
            ('X-Object-Meta-%s' % name).lower(),
            constraints.check_metadata(Request.blank(
                '/', headers=headers), 'object').body.lower())

    def test_check_metadata_value_length(self):
        value = 'a' * constraints.MAX_META_VALUE_LENGTH
        headers = {'X-Object-Meta-Name': value}
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), None)
        value = 'a' * (constraints.MAX_META_VALUE_LENGTH + 1)
        headers = {'X-Object-Meta-Name': value}
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object').status_int, HTTP_BAD_REQUEST)
        self.assertIn(
            'x-object-meta-name',
            constraints.check_metadata(Request.blank(
                '/', headers=headers),
                'object').body.lower())
        self.assertIn(
            str(constraints.MAX_META_VALUE_LENGTH),
            constraints.check_metadata(Request.blank(
                '/', headers=headers),
                'object').body)

    def test_check_metadata_count(self):
        headers = {}
        for x in xrange(constraints.MAX_META_COUNT):
            headers['X-Object-Meta-%d' % x] = 'v'
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), None)
        headers['X-Object-Meta-Too-Many'] = 'v'
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object').status_int, HTTP_BAD_REQUEST)

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
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object'), None)
        # add two more headers in case adding just one falls exactly on the
        # limit (eg one header adds 1024 and the limit is 2048)
        headers['X-Object-Meta-%04d%s' %
                (x, 'a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
            'v' * constraints.MAX_META_VALUE_LENGTH
        headers['X-Object-Meta-%04d%s' %
                (x + 1, 'a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
            'v' * constraints.MAX_META_VALUE_LENGTH
        self.assertEquals(constraints.check_metadata(Request.blank(
            '/', headers=headers), 'object').status_int, HTTP_BAD_REQUEST)

    def test_check_object_creation_content_length(self):
        headers = {'Content-Length': str(constraints.MAX_FILE_SIZE),
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'), None)

        headers = {'Content-Length': str(constraints.MAX_FILE_SIZE + 1),
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name').status_int,
            HTTP_REQUEST_ENTITY_TOO_LARGE)

        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'), None)

        headers = {'Transfer-Encoding': 'gzip',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name').status_int,
            HTTP_BAD_REQUEST)

        headers = {'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name').status_int,
            HTTP_LENGTH_REQUIRED)

        headers = {'Content-Length': 'abc',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name').status_int,
            HTTP_BAD_REQUEST)

        headers = {'Transfer-Encoding': 'gzip,chunked',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name').status_int,
            HTTP_NOT_IMPLEMENTED)

    def test_check_object_creation_copy(self):
        headers = {'Content-Length': '0',
                   'X-Copy-From': 'c/o2',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'), None)

        headers = {'Content-Length': '1',
                   'X-Copy-From': 'c/o2',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name').status_int,
            HTTP_BAD_REQUEST)

        headers = {'Transfer-Encoding': 'chunked',
                   'X-Copy-From': 'c/o2',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'), None)

        # a content-length header is always required
        headers = {'X-Copy-From': 'c/o2',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name').status_int,
            HTTP_LENGTH_REQUIRED)

    def test_check_object_creation_name_length(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain'}
        name = 'o' * constraints.MAX_OBJECT_NAME_LENGTH
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), name), None)
        name = 'o' * (constraints.MAX_OBJECT_NAME_LENGTH + 1)
        self.assertEquals(constraints.check_object_creation(
            Request.blank('/', headers=headers), name).status_int,
            HTTP_BAD_REQUEST)

    def test_check_object_creation_content_type(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank(
            '/', headers=headers), 'object_name'), None)
        headers = {'Transfer-Encoding': 'chunked'}
        self.assertEquals(constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name').status_int,
            HTTP_BAD_REQUEST)

    def test_check_object_creation_bad_content_type(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': '\xff\xff'}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEquals(resp.status_int, HTTP_BAD_REQUEST)
        self.assert_('Content-Type' in resp.body)

    def test_check_object_creation_bad_delete_headers(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': 'abc'}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEquals(resp.status_int, HTTP_BAD_REQUEST)
        self.assert_('Non-integer X-Delete-After' in resp.body)

        t = str(int(time.time() - 60))
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain',
                   'X-Delete-At': t}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assertEquals(resp.status_int, HTTP_BAD_REQUEST)
        self.assert_('X-Delete-At in past' in resp.body)

    def test_check_delete_headers(self):

        # X-Delete-After
        headers = {'X-Delete-After': '60'}
        resp = constraints.check_delete_headers(
            Request.blank('/', headers=headers))
        self.assertTrue(isinstance(resp, Request))
        self.assertTrue('x-delete-at' in resp.headers)

        headers = {'X-Delete-After': 'abc'}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('Non-integer X-Delete-After' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

        headers = {'X-Delete-After': '60.1'}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('Non-integer X-Delete-After' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

        headers = {'X-Delete-After': '-1'}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('X-Delete-After in past' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

        # X-Delete-At
        t = str(int(time.time() + 100))
        headers = {'X-Delete-At': t}
        resp = constraints.check_delete_headers(
            Request.blank('/', headers=headers))
        self.assertTrue(isinstance(resp, Request))
        self.assertTrue('x-delete-at' in resp.headers)
        self.assertEquals(resp.headers.get('X-Delete-At'), t)

        headers = {'X-Delete-At': 'abc'}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('Non-integer X-Delete-At' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

        t = str(int(time.time() + 100)) + '.1'
        headers = {'X-Delete-At': t}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('Non-integer X-Delete-At' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

        t = str(int(time.time()))
        headers = {'X-Delete-At': t}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('X-Delete-At in past' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

        t = str(int(time.time() - 1))
        headers = {'X-Delete-At': t}
        try:
            resp = constraints.check_delete_headers(
                Request.blank('/', headers=headers))
        except HTTPException as e:
            self.assertEquals(e.status_int, HTTP_BAD_REQUEST)
            self.assertTrue('X-Delete-At in past' in e.body)
        else:
            self.fail("Should have failed with HTTPBadRequest")

    def test_check_delete_headers_sets_delete_at(self):
        t = time.time() + 1000
        # check delete-at is passed through
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-At': str(int(t))}
        req = Request.blank('/', headers=headers)
        constraints.check_delete_headers(req)
        self.assertTrue('X-Delete-At' in req.headers)
        self.assertEqual(req.headers['X-Delete-At'], str(int(t)))

        # check delete-after is converted to delete-at
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42'}
        req = Request.blank('/', headers=headers)
        with mock.patch('time.time', lambda: t):
            constraints.check_delete_headers(req)
        self.assertTrue('X-Delete-At' in req.headers)
        expected = str(int(t) + 42)
        self.assertEqual(req.headers['X-Delete-At'], expected)

        # check delete-after takes precedence over delete-at
        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42',
                   'X-Delete-At': str(int(t) + 40)}
        req = Request.blank('/', headers=headers)
        with mock.patch('time.time', lambda: t):
            constraints.check_delete_headers(req)
        self.assertTrue('X-Delete-At' in req.headers)
        self.assertEqual(req.headers['X-Delete-At'], expected)

        headers = {'Content-Length': '0',
                   'Content-Type': 'text/plain',
                   'X-Delete-After': '42',
                   'X-Delete-At': str(int(t) + 44)}
        req = Request.blank('/', headers=headers)
        with mock.patch('time.time', lambda: t):
            constraints.check_delete_headers(req)
        self.assertTrue('X-Delete-At' in req.headers)
        self.assertEqual(req.headers['X-Delete-At'], expected)

    def test_check_dir(self):
        self.assertFalse(constraints.check_dir('', ''))
        with mock.patch("os.path.isdir", MockTrue()):
            self.assertTrue(constraints.check_dir('/srv', 'foo/bar'))

    def test_check_mount(self):
        self.assertFalse(constraints.check_mount('', ''))
        with mock.patch("swift.common.utils.ismount", MockTrue()):
            self.assertTrue(constraints.check_mount('/srv', '1'))
            self.assertTrue(constraints.check_mount('/srv', 'foo-bar'))
            self.assertTrue(constraints.check_mount(
                '/srv', '003ed03c-242a-4b2f-bee9-395f801d1699'))
            self.assertFalse(constraints.check_mount('/srv', 'foo bar'))
            self.assertFalse(constraints.check_mount('/srv', 'foo/bar'))
            self.assertFalse(constraints.check_mount('/srv', 'foo?bar'))

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
        timestamp = utils.Timestamp(time.time())
        req = Request.blank('/', headers={'X-Timestamp': timestamp.internal})
        self.assertEqual(timestamp, constraints.valid_timestamp(req))
        req = Request.blank('/', headers={'X-Timestamp': timestamp.normal})
        self.assertEqual(timestamp, constraints.valid_timestamp(req))

    def test_check_utf8(self):
        unicode_sample = u'\uc77c\uc601'
        valid_utf8_str = unicode_sample.encode('utf-8')
        invalid_utf8_str = unicode_sample.encode('utf-8')[::-1]
        unicode_with_null = u'abc\u0000def'
        utf8_with_null = unicode_with_null.encode('utf-8')

        for false_argument in [None,
                               '',
                               invalid_utf8_str,
                               unicode_with_null,
                               utf8_with_null]:
            self.assertFalse(constraints.check_utf8(false_argument))

        for true_argument in ['this is ascii and utf-8, too',
                              unicode_sample,
                              valid_utf8_str]:
            self.assertTrue(constraints.check_utf8(true_argument))

    def test_check_utf8_non_canonical(self):
        self.assertFalse(constraints.check_utf8('\xed\xa0\xbc\xed\xbc\xb8'))
        self.assertFalse(constraints.check_utf8('\xed\xa0\xbd\xed\xb9\x88'))

    def test_check_utf8_lone_surrogates(self):
        self.assertFalse(constraints.check_utf8('\xed\xa0\xbc'))
        self.assertFalse(constraints.check_utf8('\xed\xb9\x88'))

    def test_validate_bad_meta(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-object-meta-hello':
                     'ab' * constraints.MAX_HEADER_SIZE})
        self.assertEquals(constraints.check_metadata(req, 'object').status_int,
                          HTTP_BAD_REQUEST)
        self.assertIn('x-object-meta-hello', constraints.check_metadata(req,
                      'object').body.lower())

    def test_validate_constraints(self):
        c = constraints
        self.assertTrue(c.MAX_META_OVERALL_SIZE > c.MAX_META_NAME_LENGTH)
        self.assertTrue(c.MAX_META_OVERALL_SIZE > c.MAX_META_VALUE_LENGTH)
        self.assertTrue(c.MAX_HEADER_SIZE > c.MAX_META_NAME_LENGTH)
        self.assertTrue(c.MAX_HEADER_SIZE > c.MAX_META_VALUE_LENGTH)

    def test_validate_copy_from(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': 'c/o2'})
        src_cont, src_obj = constraints.check_copy_from_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': 'c/subdir/o2'})
        src_cont, src_obj = constraints.check_copy_from_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'subdir/o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': '/c/o2'})
        src_cont, src_obj = constraints.check_copy_from_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')

    def test_validate_bad_copy_from(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': 'bad_object'})
        self.assertRaises(HTTPException,
                          constraints.check_copy_from_header, req)

    def test_validate_destination(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': 'c/o2'})
        src_cont, src_obj = constraints.check_destination_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': 'c/subdir/o2'})
        src_cont, src_obj = constraints.check_destination_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'subdir/o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': '/c/o2'})
        src_cont, src_obj = constraints.check_destination_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')

    def test_validate_bad_destination(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': 'bad_object'})
        self.assertRaises(HTTPException,
                          constraints.check_destination_header, req)

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


class TestConstraintsConfig(unittest.TestCase):

    def test_default_constraints(self):
        for key in constraints.DEFAULT_CONSTRAINTS:
            # if there is local over-rides in swift.conf we just continue on
            if key in constraints.OVERRIDE_CONSTRAINTS:
                continue
            # module level attrs (that aren't in OVERRIDE) should have the
            # same value as the DEFAULT map
            module_level_value = getattr(constraints, key.upper())
            self.assertEquals(constraints.DEFAULT_CONSTRAINTS[key],
                              module_level_value)

    def test_effective_constraints(self):
        for key in constraints.DEFAULT_CONSTRAINTS:
            # module level attrs should always mirror the same value as the
            # EFFECTIVE map
            module_level_value = getattr(constraints, key.upper())
            self.assertEquals(constraints.EFFECTIVE_CONSTRAINTS[key],
                              module_level_value)
            # if there are local over-rides in swift.conf those should be
            # reflected in the EFFECTIVE, otherwise we expect the DEFAULTs
            self.assertEquals(constraints.EFFECTIVE_CONSTRAINTS[key],
                              constraints.OVERRIDE_CONSTRAINTS.get(
                                  key, constraints.DEFAULT_CONSTRAINTS[key]))

    def test_override_constraints(self):
        try:
            with tempfile.NamedTemporaryFile() as f:
                f.write('[swift-constraints]\n')
                # set everything to 1
                for key in constraints.DEFAULT_CONSTRAINTS:
                    f.write('%s = 1\n' % key)
                f.flush()
                with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                    constraints.reload_constraints()
            for key in constraints.DEFAULT_CONSTRAINTS:
                # module level attrs should all be 1
                module_level_value = getattr(constraints, key.upper())
                self.assertEquals(module_level_value, 1)
                # all keys should be in OVERRIDE
                self.assertEquals(constraints.OVERRIDE_CONSTRAINTS[key],
                                  module_level_value)
                # module level attrs should always mirror the same value as
                # the EFFECTIVE map
                self.assertEquals(constraints.EFFECTIVE_CONSTRAINTS[key],
                                  module_level_value)
        finally:
            constraints.reload_constraints()

    def test_reload_reset(self):
        try:
            with tempfile.NamedTemporaryFile() as f:
                f.write('[swift-constraints]\n')
                # set everything to 1
                for key in constraints.DEFAULT_CONSTRAINTS:
                    f.write('%s = 1\n' % key)
                f.flush()
                with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                    constraints.reload_constraints()
            self.assertTrue(constraints.SWIFT_CONSTRAINTS_LOADED)
            self.assertEquals(sorted(constraints.DEFAULT_CONSTRAINTS.keys()),
                              sorted(constraints.OVERRIDE_CONSTRAINTS.keys()))
            # file is now deleted...
            with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                constraints.reload_constraints()
            # no constraints have been loaded from non-existent swift.conf
            self.assertFalse(constraints.SWIFT_CONSTRAINTS_LOADED)
            # no constraints are in OVERRIDE
            self.assertEquals([], constraints.OVERRIDE_CONSTRAINTS.keys())
            # the EFFECTIVE constraints mirror DEFAULT
            self.assertEquals(constraints.EFFECTIVE_CONSTRAINTS,
                              constraints.DEFAULT_CONSTRAINTS)
        finally:
            constraints.reload_constraints()


if __name__ == '__main__':
    unittest.main()
