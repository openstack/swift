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

from swift.common.swob import Request, HTTPOk
from swift.common.middleware import listing_formats
from test.unit.common.middleware.helpers import FakeSwift


class TestListingFormats(unittest.TestCase):
    def setUp(self):
        self.fake_swift = FakeSwift()
        self.app = listing_formats.ListingFilter(self.fake_swift)
        self.fake_account_listing = json.dumps([
            {'name': 'bar', 'bytes': 0, 'count': 0,
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'subdir': 'foo_'},
        ]).encode('ascii')
        self.fake_container_listing = json.dumps([
            {'name': 'bar', 'hash': 'etag', 'bytes': 0,
             'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'subdir': 'foo/'},
        ]).encode('ascii')

    def test_valid_account(self):
        self.fake_swift.register('GET', '/v1/a', HTTPOk, {
            'Content-Length': str(len(self.fake_account_listing)),
            'Content-Type': 'application/json'}, self.fake_account_listing)

        req = Request.blank('/v1/a')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo_\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=txt')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo_\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

        req = Request.blank('/v1/a?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, self.fake_account_listing)
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
            b'</container>',
            b'<subdir name="foo_" />',
            b'</account>',
        ])
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a?format=json'))

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

        req = Request.blank('/v1/a/c?format=txt')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, b'bar\nfoo/\n')
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')
        self.assertEqual(self.fake_swift.calls[-1], (
            'GET', '/v1/a/c?format=json'))

        req = Request.blank('/v1/a/c?format=json')
        resp = req.get_response(self.app)
        self.assertEqual(resp.body, self.fake_container_listing)
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

        req = Request.blank('/v1/a?format=txt')
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

        req = Request.blank('/v1/a/c?format=txt')
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
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'subdir': 'foo/'},
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
            do_sub_test('/v1/staticweb/bad-json?format=txt')
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
        do_test('/v1/staticweb/bad-json?format=txt')
        do_test('/v1/staticweb/bad-json?format=xml')
        # The response we get is *just close enough* to being valid that we
        # assume it is and slap on the missing charset. If you set up staticweb
        # to serve back such responses, your clients are already hosed.
        do_test('/v1/staticweb/bad-json?format=json', expect_charset=True)
