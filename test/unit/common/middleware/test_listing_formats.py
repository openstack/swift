# Copyright (c) 2017 OpenStack Foundation
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

import json
import unittest

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import Request, HTTPOk, HTTPNoContent
from swift.common.middleware import listing_formats
from swift.common.request_helpers import get_reserved_name
from swift.common.storage_policy import POLICIES
from test.debug_logger import debug_logger
from test.unit.common.middleware.helpers import FakeSwift


TEST_POLICIES = (POLICIES[0].name, 'Policy-1')


class TestListingFormats(unittest.TestCase):
    def setUp(self):
        self.fake_swift = FakeSwift()
        self.logger = debug_logger('test-listing')
        self.app = listing_formats.ListingFilter(self.fake_swift, {},
                                                 logger=self.logger)
        self.fake_account_listing = json.dumps([
            {'name': 'bar', 'bytes': 0, 'count': 0,
             'last_modified': '1970-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[0]},
            {'subdir': 'foo_'},
            {'name': 'foobar', 'bytes': 0, 'count': 0,
             'last_modified': '2025-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[1]},
            {'name': 'nobar', 'bytes': 0, 'count': 0,  # Unknown policy
             'last_modified': '2025-02-01T00:00:00.000000'},
        ]).encode('ascii')
        self.fake_container_listing = json.dumps([
            {'name': 'bar', 'hash': 'etag', 'bytes': 0,
             'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'subdir': 'foo/'},
        ]).encode('ascii')

        self.fake_account_listing_with_reserved = json.dumps([
            {'name': 'bar', 'bytes': 0, 'count': 0,
             'last_modified': '1970-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[0]},
            {'name': get_reserved_name('bar', 'versions'), 'bytes': 0,
             'count': 0, 'last_modified': '1970-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[0]},
            {'subdir': 'foo_'},
            {'subdir': get_reserved_name('foo_')},
            {'name': 'foobar', 'bytes': 0, 'count': 0,
             'last_modified': '2025-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[1]},
            {'name': 'nobar', 'bytes': 0, 'count': 0,  # Unknown policy
             'last_modified': '2025-02-01T00:00:00.000000'},
        ]).encode('ascii')
        self.fake_container_listing_with_reserved = json.dumps([
            {'name': 'bar', 'hash': 'etag', 'bytes': 0,
             'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': get_reserved_name('bar', 'extra data'), 'hash': 'etag',
             'bytes': 0, 'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'subdir': 'foo/'},
            {'subdir': get_reserved_name('foo/')},
        ]).encode('ascii')

    def test_valid_account(self):
        self.fake_swift.register('GET', '/v1/a', HTTPOk, {
            'Content-Length': str(len(self.fake_account_listing)),
            'Content-Type': 'application/json'}, self.fake_account_listing)

        req = Request.blank('/v1/a')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo_\nfoobar\nnobar\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=plain')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo_\nfoobar\nnobar\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(json.loads(resp.body),
                         json.loads(self.fake_account_listing))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=xml')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body.split(b'\n'), [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<account name="a">',
            b'<container><name>bar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % TEST_POLICIES[0].encode('ascii'),
            b'<subdir name="foo_" />',
            b'<container><name>foobar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>2025-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % TEST_POLICIES[1].encode('ascii'),
            b'<container><name>nobar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>2025-02-01T00:00:00.000000</last_modified>'
            b'</container>',
            b'</account>',
        ])
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

    def test_valid_content_type_on_txt_head(self):
        self.fake_swift.register('HEAD', '/v1/a', HTTPNoContent, {
            'Content-Length': '0',
            'Content-Type': 'application/json'}, b'')
        req = Request.blank('/v1/a', method='HEAD')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertIn('Vary', resp.headers)
        # Even though the client didn't send an Accept header, the response
        # could change *if a subsequent request does*, so include Vary: Accept
        self.assertEqual(resp.headers['Vary'], 'Accept')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a?format=json'))

    def test_text_content_type_on_invalid_format_qs(self):
        self.fake_swift.register('HEAD', '/v1/a/c', HTTPNoContent, {
            'Content-Type': 'application/json'}, b'')
        req = Request.blank('/v1/a/c?format=foo', method='HEAD')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Length'], '0')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a/c?format=json'))

    def test_accept_content_type_on_missing_qs(self):
        self.fake_swift.register('HEAD', '/v1/a/c', HTTPNoContent, {
            'Content-Type': 'application/json'}, b'')
        req = Request.blank('/v1/a/c', method='HEAD',
                            headers={'Accept': 'application/xml'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Length'], '0')
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a/c?format=json'))

    def test_accept_ignored_on_invalid_qs(self):
        self.fake_swift.register('HEAD', '/v1/a/c', HTTPNoContent, {
            'Content-Type': 'application/json'}, b'')
        req = Request.blank('/v1/a/c?format=foo', method='HEAD',
                            headers={'Accept': 'application/xml'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Length'], '0')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a/c?format=json'))

    def test_valid_content_type_on_xml_head(self):
        self.fake_swift.register('HEAD', '/v1/a', HTTPNoContent, {
            'Content-Length': '0',
            'Content-Type': 'application/json'}, b'')
        req = Request.blank('/v1/a?format=xml', method='HEAD')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        # query param overrides header, so it won't vary
        self.assertNotIn('Vary', resp.headers)
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a?format=json'))

    def test_valid_content_type_on_xml_head_with_no_content_length(self):
        # note: eventlet 0.38.0 stopped including content-length with 204
        # responses
        self.fake_swift.register('HEAD', '/v1/a', HTTPNoContent, {
            'Content-Type': 'application/json'}, b'')
        req = Request.blank('/v1/a?format=xml', method='HEAD')
        status, headers, body = req.call_application(self.app)
        self.assertEqual(b''.join(body), b'')
        headers_dict = dict(headers)
        self.assertEqual(headers_dict.get('Content-Length'), '0', headers)
        self.assertEqual(headers_dict['Content-Type'],
                         'application/xml; charset=utf-8')
        # query param overrides header, so it won't vary
        self.assertNotIn('Vary', HeaderKeyDict(headers_dict))
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a?format=json'))

    def test_update_vary_if_present(self):
        self.fake_swift.register('HEAD', '/v1/a', HTTPNoContent, {
            'Content-Length': '0',
            'Content-Type': 'application/json',
            'Vary': 'Origin'}, b'')
        req = Request.blank('/v1/a', method='HEAD')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(resp.headers['Vary'], 'Origin, Accept')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a?format=json'))

    def test_add_vary_when_content_type_not_json(self):
        self.fake_swift.register('HEAD', '/v1/a', HTTPNoContent, {
            'Content-Length': '0',
            'Content-Type': 'text/plain'}, b'')
        req = Request.blank('/v1/a', method='HEAD')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        # We actually returned early, we didn't change things in the
        # request, but added the vary to let the cache know this
        # request could vary based on Accept as we didn't pass in
        # a format.
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain')
        self.assertEqual(resp.headers['Vary'], 'Accept')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a?format=json'))

    def test_update_vary_does_not_duplicate(self):
        self.fake_swift.register('HEAD', '/v1/a', HTTPNoContent, {
            'Content-Length': '0',
            'Content-Type': 'application/json',
            'Vary': 'Accept'}, b'')
        req = Request.blank('/v1/a', method='HEAD')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(resp.headers['Vary'], 'Accept')
        self.assertEqual(self.fake_swift.calls[-1], (
            'HEAD', '/v1/a?format=json'))

    def test_valid_account_with_reserved(self):
        body_len = len(self.fake_account_listing_with_reserved)
        self.fake_swift.register(
            'GET', '/v1/a\xe2\x98\x83', HTTPOk, {
                'Content-Length': str(body_len),
                'Content-Type': 'application/json',
            }, self.fake_account_listing_with_reserved)

        req = Request.blank('/v1/a\xe2\x98\x83')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo_\nfoobar\nnobar\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            "Account listing for a%E2%98%83 had reserved byte in name: "
            "'\\x00bar\\x00versions'",
            "Account listing for a%E2%98%83 had reserved byte in subdir: "
            "'\\x00foo_'",
        ])

        req = Request.blank('/v1/a\xe2\x98\x83', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\n%s\nfoo_\n%s\nfoobar\nnobar\n' % (
            get_reserved_name('bar', 'versions').encode('ascii'),
            get_reserved_name('foo_').encode('ascii'),
        ))
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

        req = Request.blank('/v1/a\xe2\x98\x83?format=plain')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo_\nfoobar\nnobar\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

        req = Request.blank('/v1/a\xe2\x98\x83?format=plain', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\n%s\nfoo_\n%s\nfoobar\nnobar\n' % (
            get_reserved_name('bar', 'versions').encode('ascii'),
            get_reserved_name('foo_').encode('ascii'),
        ))
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

        req = Request.blank('/v1/a\xe2\x98\x83?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(json.loads(resp.body),
                         json.loads(self.fake_account_listing))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

        req = Request.blank('/v1/a\xe2\x98\x83?format=json', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(json.loads(resp.body),
                         json.loads(self.fake_account_listing_with_reserved))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

        req = Request.blank('/v1/a\xe2\x98\x83?format=xml')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body.split(b'\n'), [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<account name="a\xe2\x98\x83">',
            b'<container><name>bar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % TEST_POLICIES[0].encode('ascii'),
            b'<subdir name="foo_" />',
            b'<container><name>foobar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>2025-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % TEST_POLICIES[1].encode('ascii'),
            b'<container><name>nobar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>2025-02-01T00:00:00.000000</last_modified>'
            b'</container>',
            b'</account>',
        ])
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

        req = Request.blank('/v1/a\xe2\x98\x83?format=xml', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body.split(b'\n'), [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<account name="a\xe2\x98\x83">',
            b'<container><name>bar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % TEST_POLICIES[0].encode('ascii'),
            b'<container><name>%s</name>'
            b'<count>0</count><bytes>0</bytes>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % (
                get_reserved_name('bar', 'versions').encode('ascii'),
                TEST_POLICIES[0].encode('ascii'),
            ),
            b'<subdir name="foo_" />',
            b'<subdir name="%s" />' % get_reserved_name(
                'foo_').encode('ascii'),
            b'<container><name>foobar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>2025-01-01T00:00:00.000000</last_modified>'
            b'<storage_policy>%s</storage_policy>'
            b'</container>' % TEST_POLICIES[1].encode('ascii'),
            b'<container><name>nobar</name><count>0</count><bytes>0</bytes>'
            b'<last_modified>2025-02-01T00:00:00.000000</last_modified>'
            b'</container>',
            b'</account>',
        ])
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a\xe2\x98\x83?format=json'))

    def test_valid_container(self):
        self.fake_swift.register('GET', '/v1/a/c', HTTPOk, {
            'Content-Length': str(len(self.fake_container_listing)),
            'Content-Type': 'application/json'}, self.fake_container_listing)

        req = Request.blank('/v1/a/c')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo/\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=plain')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo/\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(json.loads(resp.body),
                         json.loads(self.fake_container_listing))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=xml')
        resp = req.get_response(self.app)
        self.assertEqual(
            resp.body,
            b'<?xml version="1.0" encoding="UTF-8"?>\n'
            b'<container name="c">'
            b'<object><name>bar</name><hash>etag</hash><bytes>0</bytes>'
            b'<content_type>text/plain</content_type>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'</object>'
            b'<subdir name="foo/"><name>foo/</name></subdir>'
            b'</container>'
        )
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

    def test_valid_container_with_reserved(self):
        path = '/v1/a\xe2\x98\x83/c\xf0\x9f\x8c\xb4'
        body_len = len(self.fake_container_listing_with_reserved)
        self.fake_swift.register(
            'GET', path, HTTPOk, {
                'Content-Length': str(body_len),
                'Content-Type': 'application/json',
            }, self.fake_container_listing_with_reserved)

        req = Request.blank(path)
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo/\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            "Container listing for a%E2%98%83/c%F0%9F%8C%B4 had reserved byte "
            "in name: '\\x00bar\\x00extra data'",
            "Container listing for a%E2%98%83/c%F0%9F%8C%B4 had reserved byte "
            "in subdir: '\\x00foo/'",
        ])

        req = Request.blank(path, headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\n%s\nfoo/\n%s\n' % (
            get_reserved_name('bar', 'extra data').encode('ascii'),
            get_reserved_name('foo/').encode('ascii'),
        ))
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

        req = Request.blank(path + '?format=plain')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo/\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

        req = Request.blank(path + '?format=plain', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\n%s\nfoo/\n%s\n' % (
            get_reserved_name('bar', 'extra data').encode('ascii'),
            get_reserved_name('foo/').encode('ascii'),
        ))
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

        req = Request.blank(path + '?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(json.loads(resp.body),
                         json.loads(self.fake_container_listing))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

        req = Request.blank(path + '?format=json', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(json.loads(resp.body),
                         json.loads(self.fake_container_listing_with_reserved))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

        req = Request.blank(path + '?format=xml')
        resp = req.get_response(self.app)
        self.assertEqual(
            resp.body,
            b'<?xml version="1.0" encoding="UTF-8"?>\n'
            b'<container name="c\xf0\x9f\x8c\xb4">'
            b'<object><name>bar</name><hash>etag</hash><bytes>0</bytes>'
            b'<content_type>text/plain</content_type>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'</object>'
            b'<subdir name="foo/"><name>foo/</name></subdir>'
            b'</container>'
        )
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

        req = Request.blank(path + '?format=xml', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = req.get_response(self.app)
        self.assertEqual(
            resp.body,
            b'<?xml version="1.0" encoding="UTF-8"?>\n'
            b'<container name="c\xf0\x9f\x8c\xb4">'
            b'<object><name>bar</name><hash>etag</hash><bytes>0</bytes>'
            b'<content_type>text/plain</content_type>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'</object>'
            b'<object><name>%s</name>'
            b'<hash>etag</hash><bytes>0</bytes>'
            b'<content_type>text/plain</content_type>'
            b'<last_modified>1970-01-01T00:00:00.000000</last_modified>'
            b'</object>'
            b'<subdir name="foo/"><name>foo/</name></subdir>'
            b'<subdir name="%s"><name>%s</name></subdir>'
            b'</container>' % (
                get_reserved_name('bar', 'extra data').encode('ascii'),
                get_reserved_name('foo/').encode('ascii'),
                get_reserved_name('foo/').encode('ascii'),
            ))
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', path + '?format=json'))

    def test_blank_account(self):
        self.fake_swift.register('GET', '/v1/a', HTTPOk, {
            'Content-Length': '2', 'Content-Type': 'application/json'}, b'[]')

        req = Request.blank('/v1/a')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '204 No Content')
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=plain')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '204 No Content')
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body, b'[]')
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=xml')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body.split(b'\n'), [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<account name="a">',
            b'</account>',
        ])
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

    def test_blank_container(self):
        self.fake_swift.register('GET', '/v1/a/c', HTTPOk, {
            'Content-Length': '2', 'Content-Type': 'application/json'}, b'[]')

        req = Request.blank('/v1/a/c')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '204 No Content')
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=plain')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '204 No Content')
        self.assertEqual(resp.body, b'')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body, b'[]')
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=xml')
        resp = req.get_response(self.app)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body.split(b'\n'), [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<container name="c" />',
        ])
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

    def test_pass_through(self):
        def do_test(path):
            self.fake_swift.register(
                'GET', path, HTTPOk, {
                    'Content-Length': str(len(self.fake_container_listing)),
                    'Content-Type': 'application/json'},
                self.fake_container_listing)
            req = Request.blank(path + '?format=xml')
            resp = req.get_response(self.app)
            self.assertEqual(resp.body, self.fake_container_listing)
            self.assertEqual(resp.headers['Content-Type'], 'application/json')
            self.assertEqual(self.fake_swift.calls[-1], (
                'GET', path + '?format=xml'))  # query param is unchanged

        do_test('/')
        do_test('/v1')
        do_test('/auth/v1.0')
        do_test('/v1/a/c/o')

    def test_static_web_not_json(self):
        body = b'doesnt matter'
        self.fake_swift.register(
            'GET', '/v1/staticweb/not-json', HTTPOk,
            {'Content-Length': str(len(body)),
             'Content-Type': 'text/plain'},
            body)

        resp = Request.blank('/v1/staticweb/not-json').get_response(self.app)
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.headers['Content-Type'], 'text/plain')
        # We *did* try, though
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/staticweb/not-json?format=json'))
        # TODO: add a similar test that has *no* content-type
        # FakeSwift seems to make this hard to do

    def test_static_web_not_really_json(self):
        body = b'raises ValueError'
        self.fake_swift.register(
            'GET', '/v1/staticweb/not-json', HTTPOk,
            {'Content-Length': str(len(body)),
             'Content-Type': 'application/json'},
            body)

        resp = Request.blank('/v1/staticweb/not-json').get_response(self.app)
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.headers['Content-Type'], 'application/json')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/staticweb/not-json?format=json'))

    def test_static_web_pretend_to_be_giant_json(self):
        body = json.dumps([
            {'name': 'bar', 'hash': 'etag', 'bytes': 0,
             'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[0]},
            {'subdir': 'foo/'},
            {'name': 'foobar', 'hash': 'etag', 'bytes': 0,
             'content_type': 'text/plain',
             'last_modified': '2025-01-01T00:00:00.000000',
             'storage_policy': TEST_POLICIES[1]},
            {'name': 'nobar', 'hash': 'etag', 'bytes': 0,
             'content_type': 'text/plain',
             'last_modified': '2025-02-01T00:00:00.000000'},
        ] * 160000).encode('ascii')
        self.assertGreater(  # sanity
            len(body), listing_formats.MAX_CONTAINER_LISTING_CONTENT_LENGTH)

        self.fake_swift.register(
            'GET', '/v1/staticweb/long-json', HTTPOk,
            {'Content-Type': 'application/json'},
            body)

        resp = Request.blank('/v1/staticweb/long-json').get_response(self.app)
        self.assertEqual(resp.headers['Content-Type'], 'application/json')
        self.assertEqual(resp.body, body)
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/staticweb/long-json?format=json'))
        # TODO: add a similar test for chunked transfers
        # (staticweb referencing a DLO that doesn't fit in a single listing?)

    def test_static_web_bad_json(self):
        def do_test(body_obj):
            body = json.dumps(body_obj).encode('ascii')
            self.fake_swift.register(
                'GET', '/v1/staticweb/bad-json', HTTPOk,
                {'Content-Length': str(len(body)),
                 'Content-Type': 'application/json'},
                body)

            def do_sub_test(path):
                resp = Request.blank(path).get_response(self.app)
                self.assertEqual(resp.body, body)
                # NB: no charset is added; we pass through whatever we got
                self.assertEqual(resp.headers['Content-Type'],
                                 'application/json')
                self.assertEqual(self.fake_swift.calls[-1], (
                    'GET', '/v1/staticweb/bad-json?format=json'))

            do_sub_test('/v1/staticweb/bad-json')
            do_sub_test('/v1/staticweb/bad-json?format=plain')
            do_sub_test('/v1/staticweb/bad-json?format=xml')
            do_sub_test('/v1/staticweb/bad-json?format=json')

        do_test({})
        do_test({'non-empty': 'hash'})
        do_test(None)
        do_test(0)
        do_test('some string')
        do_test([None])
        do_test([0])
        do_test(['some string'])

    def test_static_web_bad_but_not_terrible_json(self):
        body = json.dumps([{'no name': 'nor subdir'}]).encode('ascii')
        self.fake_swift.register(
            'GET', '/v1/staticweb/bad-json', HTTPOk,
            {'Content-Length': str(len(body)),
             'Content-Type': 'application/json'},
            body)

        def do_test(path, expect_charset=False):
            resp = Request.blank(path).get_response(self.app)
            self.assertEqual(resp.body, body)
            if expect_charset:
                self.assertEqual(resp.headers['Content-Type'],
                                 'application/json; charset=utf-8')
            else:
                self.assertEqual(resp.headers['Content-Type'],
                                 'application/json')
            self.assertEqual(self.fake_swift.calls[-1], (
                'GET', '/v1/staticweb/bad-json?format=json'))

        do_test('/v1/staticweb/bad-json')
        do_test('/v1/staticweb/bad-json?format=plain')
        do_test('/v1/staticweb/bad-json?format=xml')
        # The response we get is *just close enough* to being valid that we
        # assume it is and slap on the missing charset. If you set up staticweb
        # to serve back such responses, your clients are already hosed.
        do_test('/v1/staticweb/bad-json?format=json', expect_charset=True)
