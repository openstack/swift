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

"""Tests for swift.common.request_helpers"""

import unittest
from swift.common.swob import Request, HTTPException, HeaderKeyDict
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY
from swift.common.request_helpers import is_sys_meta, is_user_meta, \
    is_sys_or_user_meta, strip_sys_meta_prefix, strip_user_meta_prefix, \
    remove_items, copy_header_subset, get_name_and_placement, \
    http_response_to_document_iters, is_object_transient_sysmeta, \
    update_etag_is_at_header, resolve_etag_is_at_header, \
    strip_object_transient_sysmeta_prefix

from test.unit import patch_policies
from test.unit.common.test_utils import FakeResponse


server_types = ['account', 'container', 'object']


class TestRequestHelpers(unittest.TestCase):
    def test_is_user_meta(self):
        m_type = 'meta'
        for st in server_types:
            self.assertTrue(is_user_meta(st, 'x-%s-%s-foo' % (st, m_type)))
            self.assertFalse(is_user_meta(st, 'x-%s-%s-' % (st, m_type)))
            self.assertFalse(is_user_meta(st, 'x-%s-%sfoo' % (st, m_type)))

    def test_is_sys_meta(self):
        m_type = 'sysmeta'
        for st in server_types:
            self.assertTrue(is_sys_meta(st, 'x-%s-%s-foo' % (st, m_type)))
            self.assertFalse(is_sys_meta(st, 'x-%s-%s-' % (st, m_type)))
            self.assertFalse(is_sys_meta(st, 'x-%s-%sfoo' % (st, m_type)))

    def test_is_sys_or_user_meta(self):
        m_types = ['sysmeta', 'meta']
        for mt in m_types:
            for st in server_types:
                self.assertTrue(is_sys_or_user_meta(st, 'x-%s-%s-foo'
                                                    % (st, mt)))
                self.assertFalse(is_sys_or_user_meta(st, 'x-%s-%s-'
                                                     % (st, mt)))
                self.assertFalse(is_sys_or_user_meta(st, 'x-%s-%sfoo'
                                                     % (st, mt)))

    def test_strip_sys_meta_prefix(self):
        mt = 'sysmeta'
        for st in server_types:
            self.assertEqual(strip_sys_meta_prefix(st, 'x-%s-%s-a'
                                                   % (st, mt)), 'a')
        mt = 'not-sysmeta'
        for st in server_types:
            with self.assertRaises(ValueError):
                strip_sys_meta_prefix(st, 'x-%s-%s-a' % (st, mt))

    def test_strip_user_meta_prefix(self):
        mt = 'meta'
        for st in server_types:
            self.assertEqual(strip_user_meta_prefix(st, 'x-%s-%s-a'
                                                    % (st, mt)), 'a')
        mt = 'not-meta'
        for st in server_types:
            with self.assertRaises(ValueError):
                strip_sys_meta_prefix(st, 'x-%s-%s-a' % (st, mt))

    def test_is_object_transient_sysmeta(self):
        self.assertTrue(is_object_transient_sysmeta(
            'x-object-transient-sysmeta-foo'))
        self.assertFalse(is_object_transient_sysmeta(
            'x-object-transient-sysmeta-'))
        self.assertFalse(is_object_transient_sysmeta(
            'x-object-meatmeta-foo'))

    def test_strip_object_transient_sysmeta_prefix(self):
        mt = 'object-transient-sysmeta'
        self.assertEqual(strip_object_transient_sysmeta_prefix('x-%s-a' % mt),
                         'a')

        mt = 'object-sysmeta-transient'
        with self.assertRaises(ValueError):
            strip_object_transient_sysmeta_prefix('x-%s-a' % mt)

    def test_remove_items(self):
        src = {'a': 'b',
               'c': 'd'}
        test = lambda x: x == 'a'
        rem = remove_items(src, test)
        self.assertEqual(src, {'c': 'd'})
        self.assertEqual(rem, {'a': 'b'})

    def test_copy_header_subset(self):
        src = {'a': 'b',
               'c': 'd'}
        from_req = Request.blank('/path', environ={}, headers=src)
        to_req = Request.blank('/path', {})
        test = lambda x: x.lower() == 'a'
        copy_header_subset(from_req, to_req, test)
        self.assertTrue('A' in to_req.headers)
        self.assertEqual(to_req.headers['A'], 'b')
        self.assertFalse('c' in to_req.headers)
        self.assertFalse('C' in to_req.headers)

    @patch_policies(with_ec_default=True)
    def test_get_name_and_placement_object_req(self):
        path = '/device/part/account/container/object'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '0'})
        device, part, account, container, obj, policy = \
            get_name_and_placement(req, 5, 5, True)
        self.assertEqual(device, 'device')
        self.assertEqual(part, 'part')
        self.assertEqual(account, 'account')
        self.assertEqual(container, 'container')
        self.assertEqual(obj, 'object')
        self.assertEqual(policy, POLICIES[0])
        self.assertEqual(policy.policy_type, EC_POLICY)

        req.headers['X-Backend-Storage-Policy-Index'] = 1
        device, part, account, container, obj, policy = \
            get_name_and_placement(req, 5, 5, True)
        self.assertEqual(device, 'device')
        self.assertEqual(part, 'part')
        self.assertEqual(account, 'account')
        self.assertEqual(container, 'container')
        self.assertEqual(obj, 'object')
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

        req.headers['X-Backend-Storage-Policy-Index'] = 'foo'
        with self.assertRaises(HTTPException) as raised:
            device, part, account, container, obj, policy = \
                get_name_and_placement(req, 5, 5, True)
        e = raised.exception
        self.assertEqual(e.status_int, 503)
        self.assertEqual(str(e), '503 Service Unavailable')
        self.assertEqual(e.body, b"No policy with index foo")

    @patch_policies(with_ec_default=True)
    def test_get_name_and_placement_object_replication(self):
        # yup, suffixes are sent '-'.joined in the path
        path = '/device/part/012-345-678-9ab-cde'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '0'})
        device, partition, suffix_parts, policy = \
            get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, '012-345-678-9ab-cde')
        self.assertEqual(policy, POLICIES[0])
        self.assertEqual(policy.policy_type, EC_POLICY)

        path = '/device/part'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '1'})
        device, partition, suffix_parts, policy = \
            get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertIsNone(suffix_parts)  # false-y
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

        path = '/device/part/'  # with a trailing slash
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '1'})
        device, partition, suffix_parts, policy = \
            get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, '')  # still false-y
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)


class TestHTTPResponseToDocumentIters(unittest.TestCase):
    def test_200(self):
        fr = FakeResponse(
            200,
            {'Content-Length': '10', 'Content-Type': 'application/lunch'},
            b'sandwiches')

        doc_iters = http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 0)
        self.assertEqual(last_byte, 9)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Length'), '10')
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'sandwiches')

        self.assertRaises(StopIteration, next, doc_iters)

        fr = FakeResponse(
            200,
            {'Transfer-Encoding': 'chunked',
             'Content-Type': 'application/lunch'},
            b'sandwiches')

        doc_iters = http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 0)
        self.assertIsNone(last_byte)
        self.assertIsNone(length)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Transfer-Encoding'), 'chunked')
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'sandwiches')

        self.assertRaises(StopIteration, next, doc_iters)

    def test_206_single_range(self):
        fr = FakeResponse(
            206,
            {'Content-Length': '8', 'Content-Type': 'application/lunch',
             'Content-Range': 'bytes 1-8/10'},
            b'andwiche')

        doc_iters = http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 1)
        self.assertEqual(last_byte, 8)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Length'), '8')
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'andwiche')

        self.assertRaises(StopIteration, next, doc_iters)

        # Chunked response should be treated in the same way as non-chunked one
        fr = FakeResponse(
            206,
            {'Transfer-Encoding': 'chunked',
             'Content-Type': 'application/lunch',
             'Content-Range': 'bytes 1-8/10'},
            b'andwiche')

        doc_iters = http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 1)
        self.assertEqual(last_byte, 8)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'andwiche')

        self.assertRaises(StopIteration, next, doc_iters)

    def test_206_multiple_ranges(self):
        fr = FakeResponse(
            206,
            {'Content-Type': 'multipart/byteranges; boundary=asdfasdfasdf'},
            (b"--asdfasdfasdf\r\n"
             b"Content-Type: application/lunch\r\n"
             b"Content-Range: bytes 0-3/10\r\n"
             b"\r\n"
             b"sand\r\n"
             b"--asdfasdfasdf\r\n"
             b"Content-Type: application/lunch\r\n"
             b"Content-Range: bytes 6-9/10\r\n"
             b"\r\n"
             b"ches\r\n"
             b"--asdfasdfasdf--"))

        doc_iters = http_response_to_document_iters(fr)

        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 0)
        self.assertEqual(last_byte, 3)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'sand')

        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 6)
        self.assertEqual(last_byte, 9)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'ches')

        self.assertRaises(StopIteration, next, doc_iters)

    def test_update_etag_is_at_header(self):
        # start with no existing X-Backend-Etag-Is-At
        req = Request.blank('/v/a/c/o')
        update_etag_is_at_header(req, 'X-Object-Sysmeta-My-Etag')
        self.assertEqual('X-Object-Sysmeta-My-Etag',
                         req.headers['X-Backend-Etag-Is-At'])
        # add another alternate
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Ec-Etag')
        self.assertEqual('X-Object-Sysmeta-My-Etag,X-Object-Sysmeta-Ec-Etag',
                         req.headers['X-Backend-Etag-Is-At'])
        with self.assertRaises(ValueError) as cm:
            update_etag_is_at_header(req, 'X-Object-Sysmeta-,-Bad')
        self.assertEqual('Header name must not contain commas',
                         cm.exception.args[0])

    def test_resolve_etag_is_at_header(self):
        def do_test():
            req = Request.blank('/v/a/c/o')
            # ok to have no X-Backend-Etag-Is-At
            self.assertIsNone(resolve_etag_is_at_header(req, metadata))

            # ok to have no matching metadata
            req.headers['X-Backend-Etag-Is-At'] = 'X-Not-There'
            self.assertIsNone(resolve_etag_is_at_header(req, metadata))

            # selects from metadata
            req.headers['X-Backend-Etag-Is-At'] = 'X-Object-Sysmeta-Ec-Etag'
            self.assertEqual('an etag value',
                             resolve_etag_is_at_header(req, metadata))
            req.headers['X-Backend-Etag-Is-At'] = 'X-Object-Sysmeta-My-Etag'
            self.assertEqual('another etag value',
                             resolve_etag_is_at_header(req, metadata))

            # first in list takes precedence
            req.headers['X-Backend-Etag-Is-At'] = \
                'X-Object-Sysmeta-My-Etag,X-Object-Sysmeta-Ec-Etag'
            self.assertEqual('another etag value',
                             resolve_etag_is_at_header(req, metadata))

            # non-existent alternates are passed over
            req.headers['X-Backend-Etag-Is-At'] = \
                'X-Bogus,X-Object-Sysmeta-My-Etag,X-Object-Sysmeta-Ec-Etag'
            self.assertEqual('another etag value',
                             resolve_etag_is_at_header(req, metadata))

            # spaces in list are ok
            alts = 'X-Foo, X-Object-Sysmeta-My-Etag , X-Object-Sysmeta-Ec-Etag'
            req.headers['X-Backend-Etag-Is-At'] = alts
            self.assertEqual('another etag value',
                             resolve_etag_is_at_header(req, metadata))

            # lower case in list is ok
            alts = alts.lower()
            req.headers['X-Backend-Etag-Is-At'] = alts
            self.assertEqual('another etag value',
                             resolve_etag_is_at_header(req, metadata))

            # upper case in list is ok
            alts = alts.upper()
            req.headers['X-Backend-Etag-Is-At'] = alts
            self.assertEqual('another etag value',
                             resolve_etag_is_at_header(req, metadata))

        metadata = {'X-Object-Sysmeta-Ec-Etag': 'an etag value',
                    'X-Object-Sysmeta-My-Etag': 'another etag value'}
        do_test()
        metadata = dict((k.lower(), v) for k, v in metadata.items())
        do_test()
        metadata = dict((k.upper(), v) for k, v in metadata.items())
        do_test()
