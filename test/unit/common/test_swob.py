# Copyright (c) 2012 OpenStack Foundation
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

"Tests for swift.common.swob"

import datetime
import unittest
import re
import time

from six import BytesIO
from six.moves.urllib.parse import quote

import swift.common.swob
from swift.common import utils, exceptions


class TestHeaderEnvironProxy(unittest.TestCase):
    def test_proxy(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        self.assertEqual(
            proxy.environ, {'CONTENT_LENGTH': '20',
                            'CONTENT_TYPE': 'text/plain',
                            'HTTP_SOMETHING_ELSE': 'somevalue'})
        self.assertEqual(proxy['content-length'], '20')
        self.assertEqual(proxy['content-type'], 'text/plain')
        self.assertEqual(proxy['something-else'], 'somevalue')
        self.assertEqual(set(['Something-Else',
                              'Content-Length', 'Content-Type']),
                         set(proxy.keys()))
        self.assertEqual(list(iter(proxy)), proxy.keys())
        self.assertEqual(3, len(proxy))

    def test_ignored_keys(self):
        # Constructor doesn't normalize keys
        key = 'wsgi.input'
        environ = {key: ''}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        self.assertEqual([], list(iter(proxy)))
        self.assertEqual([], proxy.keys())
        self.assertEqual(0, len(proxy))
        self.assertRaises(KeyError, proxy.__getitem__, key)
        self.assertNotIn(key, proxy)

        proxy['Content-Type'] = 'text/plain'
        self.assertEqual(['Content-Type'], list(iter(proxy)))
        self.assertEqual(['Content-Type'], proxy.keys())
        self.assertEqual(1, len(proxy))
        self.assertEqual('text/plain', proxy['Content-Type'])
        self.assertIn('Content-Type', proxy)

    def test_del(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        del proxy['Content-Length']
        del proxy['Content-Type']
        del proxy['Something-Else']
        self.assertEqual(proxy.environ, {})
        self.assertEqual(0, len(proxy))

    def test_contains(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        self.assertTrue('content-length' in proxy)
        self.assertTrue('content-type' in proxy)
        self.assertTrue('something-else' in proxy)

    def test_keys(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        self.assertEqual(
            set(proxy.keys()),
            set(('Content-Length', 'Content-Type', 'Something-Else')))


class TestRange(unittest.TestCase):
    def test_range(self):
        swob_range = swift.common.swob.Range('bytes=1-7')
        self.assertEqual(swob_range.ranges[0], (1, 7))

    def test_upsidedown_range(self):
        swob_range = swift.common.swob.Range('bytes=5-10')
        self.assertEqual(swob_range.ranges_for_length(2), [])

    def test_str(self):
        for range_str in ('bytes=1-7', 'bytes=1-', 'bytes=-1',
                          'bytes=1-7,9-12', 'bytes=-7,9-'):
            swob_range = swift.common.swob.Range(range_str)
            self.assertEqual(str(swob_range), range_str)

    def test_ranges_for_length(self):
        swob_range = swift.common.swob.Range('bytes=1-7')
        self.assertEqual(swob_range.ranges_for_length(10), [(1, 8)])
        self.assertEqual(swob_range.ranges_for_length(5), [(1, 5)])
        self.assertIsNone(swob_range.ranges_for_length(None))

    def test_ranges_for_large_length(self):
        swob_range = swift.common.swob.Range('bytes=-100000000000000000000000')
        self.assertEqual(swob_range.ranges_for_length(100), [(0, 100)])

    def test_ranges_for_length_no_end(self):
        swob_range = swift.common.swob.Range('bytes=1-')
        self.assertEqual(swob_range.ranges_for_length(10), [(1, 10)])
        self.assertEqual(swob_range.ranges_for_length(5), [(1, 5)])
        self.assertIsNone(swob_range.ranges_for_length(None))
        # This used to freak out:
        swob_range = swift.common.swob.Range('bytes=100-')
        self.assertEqual(swob_range.ranges_for_length(5), [])
        self.assertIsNone(swob_range.ranges_for_length(None))

        swob_range = swift.common.swob.Range('bytes=4-6,100-')
        self.assertEqual(swob_range.ranges_for_length(5), [(4, 5)])

    def test_ranges_for_length_no_start(self):
        swob_range = swift.common.swob.Range('bytes=-7')
        self.assertEqual(swob_range.ranges_for_length(10), [(3, 10)])
        self.assertEqual(swob_range.ranges_for_length(5), [(0, 5)])
        self.assertIsNone(swob_range.ranges_for_length(None))

        swob_range = swift.common.swob.Range('bytes=4-6,-100')
        self.assertEqual(swob_range.ranges_for_length(5), [(4, 5), (0, 5)])

    def test_ranges_for_length_multi(self):
        swob_range = swift.common.swob.Range('bytes=-20,4-')
        self.assertEqual(len(swob_range.ranges_for_length(200)), 2)

        # the actual length greater than each range element
        self.assertEqual(swob_range.ranges_for_length(200),
                         [(180, 200), (4, 200)])

        swob_range = swift.common.swob.Range('bytes=30-150,-10')
        self.assertEqual(len(swob_range.ranges_for_length(200)), 2)

        # the actual length lands in the middle of a range
        self.assertEqual(swob_range.ranges_for_length(90),
                         [(30, 90), (80, 90)])

        # the actual length greater than any of the range
        self.assertEqual(swob_range.ranges_for_length(200),
                         [(30, 151), (190, 200)])

        self.assertIsNone(swob_range.ranges_for_length(None))

    def test_ranges_for_length_edges(self):
        swob_range = swift.common.swob.Range('bytes=0-1, -7')
        self.assertEqual(swob_range.ranges_for_length(10),
                         [(0, 2), (3, 10)])

        swob_range = swift.common.swob.Range('bytes=-7, 0-1')
        self.assertEqual(swob_range.ranges_for_length(10),
                         [(3, 10), (0, 2)])

        swob_range = swift.common.swob.Range('bytes=-7, 0-1')
        self.assertEqual(swob_range.ranges_for_length(5),
                         [(0, 5), (0, 2)])

    def test_ranges_for_length_overlapping(self):
        # Fewer than 3 overlaps is okay
        swob_range = swift.common.swob.Range('bytes=10-19,15-24')
        self.assertEqual(swob_range.ranges_for_length(100),
                         [(10, 20), (15, 25)])
        swob_range = swift.common.swob.Range('bytes=10-19,15-24,20-29')
        self.assertEqual(swob_range.ranges_for_length(100),
                         [(10, 20), (15, 25), (20, 30)])

        # Adjacent ranges, though suboptimal, don't overlap
        swob_range = swift.common.swob.Range('bytes=10-19,20-29,30-39')
        self.assertEqual(swob_range.ranges_for_length(100),
                         [(10, 20), (20, 30), (30, 40)])

        # Ranges that share a byte do overlap
        swob_range = swift.common.swob.Range('bytes=10-20,20-30,30-40,40-50')
        self.assertEqual(swob_range.ranges_for_length(100), [])

        # With suffix byte range specs (e.g. bytes=-2), make sure that we
        # correctly determine overlapping-ness based on the entity length
        swob_range = swift.common.swob.Range('bytes=10-15,15-20,30-39,-9')
        self.assertEqual(swob_range.ranges_for_length(100),
                         [(10, 16), (15, 21), (30, 40), (91, 100)])
        self.assertEqual(swob_range.ranges_for_length(20), [])

    def test_ranges_for_length_nonascending(self):
        few_ranges = ("bytes=100-109,200-209,300-309,500-509,"
                      "400-409,600-609,700-709")
        many_ranges = few_ranges + ",800-809"

        swob_range = swift.common.swob.Range(few_ranges)
        self.assertEqual(swob_range.ranges_for_length(100000),
                         [(100, 110), (200, 210), (300, 310), (500, 510),
                          (400, 410), (600, 610), (700, 710)])

        swob_range = swift.common.swob.Range(many_ranges)
        self.assertEqual(swob_range.ranges_for_length(100000), [])

    def test_ranges_for_length_too_many(self):
        at_the_limit_ranges = (
            "bytes=" + ",".join("%d-%d" % (x * 1000, x * 1000 + 10)
                                for x in range(50)))
        too_many_ranges = at_the_limit_ranges + ",10000000-10000009"

        rng = swift.common.swob.Range(at_the_limit_ranges)
        self.assertEqual(len(rng.ranges_for_length(1000000000)), 50)

        rng = swift.common.swob.Range(too_many_ranges)
        self.assertEqual(rng.ranges_for_length(1000000000), [])

    def test_range_invalid_syntax(self):

        def _assert_invalid_range(range_value):
            try:
                swift.common.swob.Range(range_value)
                self.fail("Expected %r to be invalid, but wasn't" %
                          (range_value,))
            except ValueError:
                pass

        """
        All the following cases should result ValueError exception
        1. value not starts with bytes=
        2. range value start is greater than the end, eg. bytes=5-3
        3. range does not have start or end, eg. bytes=-
        4. range does not have hyphen, eg. bytes=45
        5. range value is non numeric
        6. any combination of the above
        """

        _assert_invalid_range(None)
        _assert_invalid_range('nonbytes=0-')
        _assert_invalid_range('nonbytes=foobar,10-2')
        _assert_invalid_range('bytes=5-3')
        _assert_invalid_range('bytes=-')
        _assert_invalid_range('bytes=45')
        _assert_invalid_range('bytes=foo-bar,3-5')
        _assert_invalid_range('bytes=4-10,45')
        _assert_invalid_range('bytes=foobar,3-5')
        _assert_invalid_range('bytes=nonumber-5')
        _assert_invalid_range('bytes=nonumber')
        _assert_invalid_range('bytes=--1')
        _assert_invalid_range('bytes=--0')


class TestMatch(unittest.TestCase):
    def test_match(self):
        match = swift.common.swob.Match('"a", "b"')
        self.assertEqual(match.tags, set(('a', 'b')))
        self.assertIn('a', match)
        self.assertIn('b', match)
        self.assertNotIn('c', match)
        self.assertEqual(repr(match), "Match('a, b')")

    def test_match_star(self):
        match = swift.common.swob.Match('"a", "*"')
        self.assertIn('a', match)
        self.assertIn('b', match)
        self.assertIn('c', match)
        self.assertEqual(repr(match), "Match('*, a')")

    def test_match_noquote(self):
        match = swift.common.swob.Match('a, b')
        self.assertEqual(match.tags, set(('a', 'b')))
        self.assertIn('a', match)
        self.assertIn('b', match)
        self.assertNotIn('c', match)

    def test_match_no_optional_white_space(self):
        match = swift.common.swob.Match('"a","b"')
        self.assertEqual(match.tags, set(('a', 'b')))
        self.assertIn('a', match)
        self.assertIn('b', match)
        self.assertNotIn('c', match)

    def test_match_lots_of_optional_white_space(self):
        match = swift.common.swob.Match('"a"   ,  ,   "b"   ')
        self.assertEqual(match.tags, set(('a', 'b')))
        self.assertIn('a', match)
        self.assertIn('b', match)
        self.assertNotIn('c', match)


class TestTransferEncoding(unittest.TestCase):
    def test_is_chunked(self):
        headers = {}
        self.assertFalse(swift.common.swob.is_chunked(headers))

        headers['Transfer-Encoding'] = 'chunked'
        self.assertTrue(swift.common.swob.is_chunked(headers))

        headers['Transfer-Encoding'] = 'gzip,chunked'
        try:
            swift.common.swob.is_chunked(headers)
        except AttributeError as e:
            self.assertEqual(str(e), "Unsupported Transfer-Coding header"
                             " value specified in Transfer-Encoding header")
        else:
            self.fail("Expected an AttributeError raised for 'gzip'")

        headers['Transfer-Encoding'] = 'gzip'
        try:
            swift.common.swob.is_chunked(headers)
        except ValueError as e:
            self.assertEqual(str(e), "Invalid Transfer-Encoding header value")
        else:
            self.fail("Expected a ValueError raised for 'gzip'")

        headers['Transfer-Encoding'] = 'gzip,identity'
        try:
            swift.common.swob.is_chunked(headers)
        except AttributeError as e:
            self.assertEqual(str(e), "Unsupported Transfer-Coding header"
                             " value specified in Transfer-Encoding header")
        else:
            self.fail("Expected an AttributeError raised for 'gzip,identity'")


class TestAccept(unittest.TestCase):
    def test_accept_json(self):
        for accept in ('application/json', 'application/json;q=1.0,*/*;q=0.9',
                       '*/*;q=0.9,application/json;q=1.0', 'application/*',
                       'text/*,application/json', 'application/*,text/*',
                       'application/json,text/xml'):
            acc = swift.common.swob.Accept(accept)
            match = acc.best_match(['text/plain', 'application/json',
                                    'application/xml', 'text/xml'])
            self.assertEqual(match, 'application/json')

    def test_accept_plain(self):
        for accept in ('', 'text/plain', 'application/xml;q=0.8,*/*;q=0.9',
                       '*/*;q=0.9,application/xml;q=0.8', '*/*',
                       'text/plain,application/xml'):
            acc = swift.common.swob.Accept(accept)
            match = acc.best_match(['text/plain', 'application/json',
                                    'application/xml', 'text/xml'])
            self.assertEqual(match, 'text/plain')

    def test_accept_xml(self):
        for accept in ('application/xml', 'application/xml;q=1.0,*/*;q=0.9',
                       '*/*;q=0.9,application/xml;q=1.0',
                       'application/xml;charset=UTF-8',
                       'application/xml;charset=UTF-8;qws="quoted with space"',
                       'application/xml; q=0.99 ; qws="quoted with space"'):
            acc = swift.common.swob.Accept(accept)
            match = acc.best_match(['text/plain', 'application/xml',
                                   'text/xml'])
            self.assertEqual(match, 'application/xml')

    def test_accept_invalid(self):
        for accept in ('*', 'text/plain,,', 'some stuff',
                       'application/xml;q=1.0;q=1.1', 'text/plain,*',
                       'text /plain', 'text\x7f/plain',
                       'text/plain;a=b=c',
                       'text/plain;q=1;q=2',
                       'text/plain;q=not-a-number',
                       'text/plain; ubq="unbalanced " quotes"'):
            acc = swift.common.swob.Accept(accept)
            with self.assertRaises(ValueError):
                acc.best_match(['text/plain', 'application/xml', 'text/xml'])

    def test_repr(self):
        acc = swift.common.swob.Accept("application/json")
        self.assertEqual(repr(acc), "application/json")


class TestRequest(unittest.TestCase):
    def test_blank(self):
        req = swift.common.swob.Request.blank(
            '/', environ={'REQUEST_METHOD': 'POST'},
            headers={'Content-Type': 'text/plain'}, body='hi')
        self.assertEqual(req.path_info, '/')
        self.assertEqual(req.body, 'hi')
        self.assertEqual(req.headers['Content-Type'], 'text/plain')
        self.assertEqual(req.method, 'POST')

    def test_blank_req_environ_property_args(self):
        blank = swift.common.swob.Request.blank
        req = blank('/', method='PATCH')
        self.assertEqual(req.method, 'PATCH')
        self.assertEqual(req.environ['REQUEST_METHOD'], 'PATCH')
        req = blank('/', referer='http://example.com')
        self.assertEqual(req.referer, 'http://example.com')
        self.assertEqual(req.referrer, 'http://example.com')
        self.assertEqual(req.environ['HTTP_REFERER'], 'http://example.com')
        self.assertEqual(req.headers['Referer'], 'http://example.com')
        req = blank('/', script_name='/application')
        self.assertEqual(req.script_name, '/application')
        self.assertEqual(req.environ['SCRIPT_NAME'], '/application')
        req = blank('/', host='www.example.com')
        self.assertEqual(req.host, 'www.example.com')
        self.assertEqual(req.environ['HTTP_HOST'], 'www.example.com')
        self.assertEqual(req.headers['Host'], 'www.example.com')
        req = blank('/', remote_addr='127.0.0.1')
        self.assertEqual(req.remote_addr, '127.0.0.1')
        self.assertEqual(req.environ['REMOTE_ADDR'], '127.0.0.1')
        req = blank('/', remote_user='username')
        self.assertEqual(req.remote_user, 'username')
        self.assertEqual(req.environ['REMOTE_USER'], 'username')
        req = blank('/', user_agent='curl/7.22.0 (x86_64-pc-linux-gnu)')
        self.assertEqual(req.user_agent, 'curl/7.22.0 (x86_64-pc-linux-gnu)')
        self.assertEqual(req.environ['HTTP_USER_AGENT'],
                         'curl/7.22.0 (x86_64-pc-linux-gnu)')
        self.assertEqual(req.headers['User-Agent'],
                         'curl/7.22.0 (x86_64-pc-linux-gnu)')
        req = blank('/', query_string='a=b&c=d')
        self.assertEqual(req.query_string, 'a=b&c=d')
        self.assertEqual(req.environ['QUERY_STRING'], 'a=b&c=d')
        req = blank('/', if_match='*')
        self.assertEqual(req.environ['HTTP_IF_MATCH'], '*')
        self.assertEqual(req.headers['If-Match'], '*')

        # multiple environ property kwargs
        req = blank('/', method='PATCH', referer='http://example.com',
                    script_name='/application', host='www.example.com',
                    remote_addr='127.0.0.1', remote_user='username',
                    user_agent='curl/7.22.0 (x86_64-pc-linux-gnu)',
                    query_string='a=b&c=d', if_match='*')
        self.assertEqual(req.method, 'PATCH')
        self.assertEqual(req.referer, 'http://example.com')
        self.assertEqual(req.script_name, '/application')
        self.assertEqual(req.host, 'www.example.com')
        self.assertEqual(req.remote_addr, '127.0.0.1')
        self.assertEqual(req.remote_user, 'username')
        self.assertEqual(req.user_agent, 'curl/7.22.0 (x86_64-pc-linux-gnu)')
        self.assertEqual(req.query_string, 'a=b&c=d')
        self.assertEqual(req.environ['QUERY_STRING'], 'a=b&c=d')

    def test_invalid_req_environ_property_args(self):
        # getter only property
        try:
            swift.common.swob.Request.blank(
                '/', host_url='http://example.com:8080/v1/a/c/o')
        except TypeError as e:
            self.assertEqual("got unexpected keyword argument 'host_url'",
                             str(e))
        else:
            self.fail("invalid req_environ_property didn't raise error!")
        # regular attribute
        try:
            swift.common.swob.Request.blank('/', _params_cache={'a': 'b'})
        except TypeError as e:
            self.assertEqual("got unexpected keyword "
                             "argument '_params_cache'", str(e))
        else:
            self.fail("invalid req_environ_property didn't raise error!")
        # non-existent attribute
        try:
            swift.common.swob.Request.blank('/', params_cache={'a': 'b'})
        except TypeError as e:
            self.assertEqual("got unexpected keyword "
                             "argument 'params_cache'", str(e))
        else:
            self.fail("invalid req_environ_property didn't raise error!")
        # method
        try:
            swift.common.swob.Request.blank(
                '/', as_referer='GET http://example.com')
        except TypeError as e:
            self.assertEqual("got unexpected keyword "
                             "argument 'as_referer'", str(e))
        else:
            self.fail("invalid req_environ_property didn't raise error!")

    def test_blank_path_info_precedence(self):
        blank = swift.common.swob.Request.blank
        req = blank('/a')
        self.assertEqual(req.path_info, '/a')
        req = blank('/a', environ={'PATH_INFO': '/a/c'})
        self.assertEqual(req.path_info, '/a/c')
        req = blank('/a', environ={'PATH_INFO': '/a/c'}, path_info='/a/c/o')
        self.assertEqual(req.path_info, '/a/c/o')
        req = blank('/a', path_info='/a/c/o')
        self.assertEqual(req.path_info, '/a/c/o')

    def test_blank_body_precedence(self):
        req = swift.common.swob.Request.blank(
            '/', environ={'REQUEST_METHOD': 'POST',
                          'wsgi.input': BytesIO(b'')},
            headers={'Content-Type': 'text/plain'}, body='hi')
        self.assertEqual(req.path_info, '/')
        self.assertEqual(req.body, 'hi')
        self.assertEqual(req.headers['Content-Type'], 'text/plain')
        self.assertEqual(req.method, 'POST')
        body_file = BytesIO(b'asdf')
        req = swift.common.swob.Request.blank(
            '/', environ={'REQUEST_METHOD': 'POST',
                          'wsgi.input': BytesIO(b'')},
            headers={'Content-Type': 'text/plain'}, body='hi',
            body_file=body_file)
        self.assertTrue(req.body_file is body_file)
        req = swift.common.swob.Request.blank(
            '/', environ={'REQUEST_METHOD': 'POST',
                          'wsgi.input': BytesIO(b'')},
            headers={'Content-Type': 'text/plain'}, body='hi',
            content_length=3)
        self.assertEqual(req.content_length, 3)
        self.assertEqual(len(req.body), 2)

    def test_blank_parsing(self):
        req = swift.common.swob.Request.blank('http://test.com/')
        self.assertEqual(req.environ['wsgi.url_scheme'], 'http')
        self.assertEqual(req.environ['SERVER_PORT'], '80')
        self.assertEqual(req.environ['SERVER_NAME'], 'test.com')

        req = swift.common.swob.Request.blank('https://test.com:456/')
        self.assertEqual(req.environ['wsgi.url_scheme'], 'https')
        self.assertEqual(req.environ['SERVER_PORT'], '456')

        req = swift.common.swob.Request.blank('test.com/')
        self.assertEqual(req.environ['wsgi.url_scheme'], 'http')
        self.assertEqual(req.environ['SERVER_PORT'], '80')
        self.assertEqual(req.environ['PATH_INFO'], 'test.com/')

        self.assertRaises(TypeError, swift.common.swob.Request.blank,
                          'ftp://test.com/')

    def test_params(self):
        req = swift.common.swob.Request.blank('/?a=b&c=d')
        self.assertEqual(req.params['a'], 'b')
        self.assertEqual(req.params['c'], 'd')

        new_params = {'e': 'f', 'g': 'h'}
        req.params = new_params
        self.assertDictEqual(new_params, req.params)

        new_params = (('i', 'j'), ('k', 'l'))
        req.params = new_params
        self.assertDictEqual(dict(new_params), req.params)

    def test_timestamp_missing(self):
        req = swift.common.swob.Request.blank('/')
        self.assertRaises(exceptions.InvalidTimestamp,
                          getattr, req, 'timestamp')

    def test_timestamp_invalid(self):
        req = swift.common.swob.Request.blank(
            '/', headers={'X-Timestamp': 'asdf'})
        self.assertRaises(exceptions.InvalidTimestamp,
                          getattr, req, 'timestamp')

    def test_timestamp(self):
        req = swift.common.swob.Request.blank(
            '/', headers={'X-Timestamp': '1402447134.13507_00000001'})
        expected = utils.Timestamp('1402447134.13507', offset=1)
        self.assertEqual(req.timestamp, expected)
        self.assertEqual(req.timestamp.normal, expected.normal)
        self.assertEqual(req.timestamp.internal, expected.internal)

    def test_path(self):
        req = swift.common.swob.Request.blank('/hi?a=b&c=d')
        self.assertEqual(req.path, '/hi')
        req = swift.common.swob.Request.blank(
            '/', environ={'SCRIPT_NAME': '/hi', 'PATH_INFO': '/there'})
        self.assertEqual(req.path, '/hi/there')

    def test_path_question_mark(self):
        req = swift.common.swob.Request.blank('/test%3Ffile')
        # This tests that .blank unquotes the path when setting PATH_INFO
        self.assertEqual(req.environ['PATH_INFO'], '/test?file')
        # This tests that .path requotes it
        self.assertEqual(req.path, '/test%3Ffile')

    def test_path_info_pop(self):
        req = swift.common.swob.Request.blank('/hi/there')
        self.assertEqual(req.path_info_pop(), 'hi')
        self.assertEqual(req.path_info, '/there')
        self.assertEqual(req.script_name, '/hi')

    def test_bad_path_info_pop(self):
        req = swift.common.swob.Request.blank('blahblah')
        self.assertIsNone(req.path_info_pop())

    def test_path_info_pop_last(self):
        req = swift.common.swob.Request.blank('/last')
        self.assertEqual(req.path_info_pop(), 'last')
        self.assertEqual(req.path_info, '')
        self.assertEqual(req.script_name, '/last')

    def test_path_info_pop_none(self):
        req = swift.common.swob.Request.blank('/')
        self.assertEqual(req.path_info_pop(), '')
        self.assertEqual(req.path_info, '')
        self.assertEqual(req.script_name, '/')

    def test_copy_get(self):
        req = swift.common.swob.Request.blank(
            '/hi/there', environ={'REQUEST_METHOD': 'POST'})
        self.assertEqual(req.method, 'POST')
        req2 = req.copy_get()
        self.assertEqual(req2.method, 'GET')

    def test_get_response(self):
        def test_app(environ, start_response):
            start_response('200 OK', [])
            return ['hi']

        req = swift.common.swob.Request.blank('/')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, 'hi')

    def test_401_unauthorized(self):
        # No request environment
        resp = swift.common.swob.HTTPUnauthorized()
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        # Request environment
        req = swift.common.swob.Request.blank('/')
        resp = swift.common.swob.HTTPUnauthorized(request=req)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_401_valid_account_path(self):

        def test_app(environ, start_response):
            start_response('401 Unauthorized', [])
            return ['hi']

        # Request environment contains valid account in path
        req = swift.common.swob.Request.blank('/v1/account-name')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="account-name"',
                         resp.headers['Www-Authenticate'])

        # Request environment contains valid account/container in path
        req = swift.common.swob.Request.blank('/v1/account-name/c')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="account-name"',
                         resp.headers['Www-Authenticate'])

    def test_401_invalid_path(self):

        def test_app(environ, start_response):
            start_response('401 Unauthorized', [])
            return ['hi']

        # Request environment contains bad path
        req = swift.common.swob.Request.blank('/random')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="unknown"',
                         resp.headers['Www-Authenticate'])

    def test_401_non_keystone_auth_path(self):

        def test_app(environ, start_response):
            start_response('401 Unauthorized', [])
            return ['no creds in request']

        # Request to get token
        req = swift.common.swob.Request.blank('/v1.0/auth')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="unknown"',
                         resp.headers['Www-Authenticate'])

        # Other form of path
        req = swift.common.swob.Request.blank('/auth/v1.0')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="unknown"',
                         resp.headers['Www-Authenticate'])

    def test_401_www_authenticate_exists(self):

        def test_app(environ, start_response):
            start_response('401 Unauthorized', {
                           'Www-Authenticate': 'Me realm="whatever"'})
            return ['no creds in request']

        # Auth middleware sets own Www-Authenticate
        req = swift.common.swob.Request.blank('/auth/v1.0')
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Me realm="whatever"',
                         resp.headers['Www-Authenticate'])

    def test_401_www_authenticate_is_quoted(self):

        def test_app(environ, start_response):
            start_response('401 Unauthorized', [])
            return ['hi']

        hacker = 'account-name\n\n<b>foo<br>'  # url injection test
        quoted_hacker = quote(hacker)
        req = swift.common.swob.Request.blank('/v1/' + hacker)
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="%s"' % quoted_hacker,
                         resp.headers['Www-Authenticate'])

        req = swift.common.swob.Request.blank('/v1/' + quoted_hacker)
        resp = req.get_response(test_app)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual('Swift realm="%s"' % quoted_hacker,
                         resp.headers['Www-Authenticate'])

    def test_not_401(self):

        # Other status codes should not have WWW-Authenticate in response
        def test_app(environ, start_response):
            start_response('200 OK', [])
            return ['hi']

        req = swift.common.swob.Request.blank('/')
        resp = req.get_response(test_app)
        self.assertNotIn('Www-Authenticate', resp.headers)

    def test_properties(self):
        req = swift.common.swob.Request.blank('/hi/there', body='hi')

        self.assertEqual(req.body, 'hi')
        self.assertEqual(req.content_length, 2)

        req.remote_addr = 'something'
        self.assertEqual(req.environ['REMOTE_ADDR'], 'something')
        req.body = 'whatever'
        self.assertEqual(req.content_length, 8)
        self.assertEqual(req.body, 'whatever')
        self.assertEqual(req.method, 'GET')

        req.range = 'bytes=1-7'
        self.assertEqual(req.range.ranges[0], (1, 7))

        self.assertIn('Range', req.headers)
        req.range = None
        self.assertNotIn('Range', req.headers)

    def test_datetime_properties(self):
        req = swift.common.swob.Request.blank('/hi/there', body='hi')

        req.if_unmodified_since = 0
        self.assertTrue(isinstance(req.if_unmodified_since, datetime.datetime))
        if_unmodified_since = req.if_unmodified_since
        req.if_unmodified_since = if_unmodified_since
        self.assertEqual(if_unmodified_since, req.if_unmodified_since)

        req.if_unmodified_since = 'something'
        self.assertEqual(req.headers['If-Unmodified-Since'], 'something')
        self.assertIsNone(req.if_unmodified_since)

        self.assertIn('If-Unmodified-Since', req.headers)
        req.if_unmodified_since = None
        self.assertNotIn('If-Unmodified-Since', req.headers)

        too_big_date_list = list(datetime.datetime.max.timetuple())
        too_big_date_list[0] += 1  # bump up the year
        too_big_date = time.strftime(
            "%a, %d %b %Y %H:%M:%S UTC", time.struct_time(too_big_date_list))

        req.if_unmodified_since = too_big_date
        self.assertIsNone(req.if_unmodified_since)

    def test_bad_range(self):
        req = swift.common.swob.Request.blank('/hi/there', body='hi')
        req.range = 'bad range'
        self.assertIsNone(req.range)

    def test_accept_header(self):
        req = swift.common.swob.Request({'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/',
                                         'HTTP_ACCEPT': 'application/json'})
        self.assertEqual(
            req.accept.best_match(['application/json', 'text/plain']),
            'application/json')
        self.assertEqual(
            req.accept.best_match(['text/plain', 'application/json']),
            'application/json')

    def test_swift_entity_path(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        self.assertEqual(req.swift_entity_path, '/a/c/o')

        req = swift.common.swob.Request.blank('/v1/a/c')
        self.assertEqual(req.swift_entity_path, '/a/c')

        req = swift.common.swob.Request.blank('/v1/a')
        self.assertEqual(req.swift_entity_path, '/a')

        req = swift.common.swob.Request.blank('/v1')
        self.assertIsNone(req.swift_entity_path)

    def test_path_qs(self):
        req = swift.common.swob.Request.blank('/hi/there?hello=equal&acl')
        self.assertEqual(req.path_qs, '/hi/there?hello=equal&acl')

        req = swift.common.swob.Request({'PATH_INFO': '/hi/there',
                                         'QUERY_STRING': 'hello=equal&acl'})
        self.assertEqual(req.path_qs, '/hi/there?hello=equal&acl')

    def test_url(self):
        req = swift.common.swob.Request.blank('/hi/there?hello=equal&acl')
        self.assertEqual(req.url,
                         'http://localhost/hi/there?hello=equal&acl')

    def test_wsgify(self):
        used_req = []

        @swift.common.swob.wsgify
        def _wsgi_func(req):
            used_req.append(req)
            return swift.common.swob.Response('200 OK')

        req = swift.common.swob.Request.blank('/hi/there')
        resp = req.get_response(_wsgi_func)
        self.assertEqual(used_req[0].path, '/hi/there')
        self.assertEqual(resp.status_int, 200)

    def test_wsgify_raise(self):
        used_req = []

        @swift.common.swob.wsgify
        def _wsgi_func(req):
            used_req.append(req)
            raise swift.common.swob.HTTPServerError()

        req = swift.common.swob.Request.blank('/hi/there')
        resp = req.get_response(_wsgi_func)
        self.assertEqual(used_req[0].path, '/hi/there')
        self.assertEqual(resp.status_int, 500)

    def test_split_path(self):
        """
        Copied from swift.common.utils.split_path
        """
        def _test_split_path(path, minsegs=1, maxsegs=None, rwl=False):
            req = swift.common.swob.Request.blank(path)
            return req.split_path(minsegs, maxsegs, rwl)
        self.assertRaises(ValueError, _test_split_path, '')
        self.assertRaises(ValueError, _test_split_path, '/')
        self.assertRaises(ValueError, _test_split_path, '//')
        self.assertEqual(_test_split_path('/a'), ['a'])
        self.assertRaises(ValueError, _test_split_path, '//a')
        self.assertEqual(_test_split_path('/a/'), ['a'])
        self.assertRaises(ValueError, _test_split_path, '/a/c')
        self.assertRaises(ValueError, _test_split_path, '//c')
        self.assertRaises(ValueError, _test_split_path, '/a/c/')
        self.assertRaises(ValueError, _test_split_path, '/a//')
        self.assertRaises(ValueError, _test_split_path, '/a', 2)
        self.assertRaises(ValueError, _test_split_path, '/a', 2, 3)
        self.assertRaises(ValueError, _test_split_path, '/a', 2, 3, True)
        self.assertEqual(_test_split_path('/a/c', 2), ['a', 'c'])
        self.assertEqual(_test_split_path('/a/c/o', 3), ['a', 'c', 'o'])
        self.assertRaises(ValueError, _test_split_path, '/a/c/o/r', 3, 3)
        self.assertEqual(_test_split_path('/a/c/o/r', 3, 3, True),
                         ['a', 'c', 'o/r'])
        self.assertEqual(_test_split_path('/a/c', 2, 3, True),
                         ['a', 'c', None])
        self.assertRaises(ValueError, _test_split_path, '/a', 5, 4)
        self.assertEqual(_test_split_path('/a/c/', 2), ['a', 'c'])
        self.assertEqual(_test_split_path('/a/c/', 2, 3), ['a', 'c', ''])
        try:
            _test_split_path('o\nn e', 2)
        except ValueError as err:
            self.assertEqual(str(err), 'Invalid path: o%0An%20e')
        try:
            _test_split_path('o\nn e', 2, 3, True)
        except ValueError as err:
            self.assertEqual(str(err), 'Invalid path: o%0An%20e')

    def test_unicode_path(self):
        req = swift.common.swob.Request.blank(u'/\u2661')
        self.assertEqual(req.path, quote(u'/\u2661'.encode('utf-8')))

    def test_unicode_query(self):
        req = swift.common.swob.Request.blank(u'/')
        req.query_string = u'x=\u2661'
        self.assertEqual(req.params['x'], u'\u2661'.encode('utf-8'))

    def test_url2(self):
        pi = '/hi/there'
        path = pi
        req = swift.common.swob.Request.blank(path)
        sche = 'http'
        exp_url = '%s://localhost%s' % (sche, pi)
        self.assertEqual(req.url, exp_url)

        qs = 'hello=equal&acl'
        path = '%s?%s' % (pi, qs)
        s, p = 'unit.test.example.com', '90'
        req = swift.common.swob.Request({'PATH_INFO': pi,
                                         'QUERY_STRING': qs,
                                         'SERVER_NAME': s,
                                         'SERVER_PORT': p})
        exp_url = '%s://%s:%s%s?%s' % (sche, s, p, pi, qs)
        self.assertEqual(req.url, exp_url)

        host = 'unit.test.example.com'
        req = swift.common.swob.Request({'PATH_INFO': pi,
                                         'QUERY_STRING': qs,
                                         'HTTP_HOST': host + ':80'})
        exp_url = '%s://%s%s?%s' % (sche, host, pi, qs)
        self.assertEqual(req.url, exp_url)

        host = 'unit.test.example.com'
        sche = 'https'
        req = swift.common.swob.Request({'PATH_INFO': pi,
                                         'QUERY_STRING': qs,
                                         'HTTP_HOST': host + ':443',
                                         'wsgi.url_scheme': sche})
        exp_url = '%s://%s%s?%s' % (sche, host, pi, qs)
        self.assertEqual(req.url, exp_url)

        host = 'unit.test.example.com:81'
        req = swift.common.swob.Request({'PATH_INFO': pi,
                                         'QUERY_STRING': qs,
                                         'HTTP_HOST': host,
                                         'wsgi.url_scheme': sche})
        exp_url = '%s://%s%s?%s' % (sche, host, pi, qs)
        self.assertEqual(req.url, exp_url)

    def test_as_referer(self):
        pi = '/hi/there'
        qs = 'hello=equal&acl'
        sche = 'https'
        host = 'unit.test.example.com:81'
        req = swift.common.swob.Request({'REQUEST_METHOD': 'POST',
                                         'PATH_INFO': pi,
                                         'QUERY_STRING': qs,
                                         'HTTP_HOST': host,
                                         'wsgi.url_scheme': sche})
        exp_url = '%s://%s%s?%s' % (sche, host, pi, qs)
        self.assertEqual(req.as_referer(), 'POST ' + exp_url)

    def test_message_length_just_content_length(self):
        req = swift.common.swob.Request.blank(
            u'/',
            environ={'REQUEST_METHOD': 'PUT', 'PATH_INFO': '/'})
        self.assertIsNone(req.message_length())

        req = swift.common.swob.Request.blank(
            u'/',
            environ={'REQUEST_METHOD': 'PUT', 'PATH_INFO': '/'},
            body='x' * 42)
        self.assertEqual(req.message_length(), 42)

        req.headers['Content-Length'] = 'abc'
        try:
            req.message_length()
        except ValueError as e:
            self.assertEqual(str(e), "Invalid Content-Length header value")
        else:
            self.fail("Expected a ValueError raised for 'abc'")

    def test_message_length_transfer_encoding(self):
        req = swift.common.swob.Request.blank(
            u'/',
            environ={'REQUEST_METHOD': 'PUT', 'PATH_INFO': '/'},
            headers={'transfer-encoding': 'chunked'},
            body='x' * 42)
        self.assertIsNone(req.message_length())

        req.headers['Transfer-Encoding'] = 'gzip,chunked'
        try:
            req.message_length()
        except AttributeError as e:
            self.assertEqual(str(e), "Unsupported Transfer-Coding header"
                             " value specified in Transfer-Encoding header")
        else:
            self.fail("Expected an AttributeError raised for 'gzip'")

        req.headers['Transfer-Encoding'] = 'gzip'
        try:
            req.message_length()
        except ValueError as e:
            self.assertEqual(str(e), "Invalid Transfer-Encoding header value")
        else:
            self.fail("Expected a ValueError raised for 'gzip'")

        req.headers['Transfer-Encoding'] = 'gzip,identity'
        try:
            req.message_length()
        except AttributeError as e:
            self.assertEqual(str(e), "Unsupported Transfer-Coding header"
                             " value specified in Transfer-Encoding header")
        else:
            self.fail("Expected an AttributeError raised for 'gzip,identity'")


class TestStatusMap(unittest.TestCase):
    def test_status_map(self):
        response_args = []

        def start_response(status, headers):
            response_args.append(status)
            response_args.append(headers)
        resp_cls = swift.common.swob.status_map[404]
        resp = resp_cls()
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.title, 'Not Found')
        body = ''.join(resp({}, start_response))
        self.assertTrue('The resource could not be found.' in body)
        self.assertEqual(response_args[0], '404 Not Found')
        headers = dict(response_args[1])
        self.assertEqual(headers['Content-Type'], 'text/html; charset=UTF-8')
        self.assertTrue(int(headers['Content-Length']) > 0)


class TestResponse(unittest.TestCase):
    def _get_response(self):
        def test_app(environ, start_response):
            start_response('200 OK', [])
            return ['hi']

        req = swift.common.swob.Request.blank('/')
        return req.get_response(test_app)

    def test_properties(self):
        resp = self._get_response()

        resp.location = 'something'
        self.assertEqual(resp.location, 'something')
        self.assertIn('Location', resp.headers)
        resp.location = None
        self.assertNotIn('Location', resp.headers)

        resp.content_type = 'text/plain'
        self.assertIn('Content-Type', resp.headers)
        resp.content_type = None
        self.assertNotIn('Content-Type', resp.headers)

    def test_empty_body(self):
        resp = self._get_response()
        resp.body = ''
        self.assertEqual(resp.body, '')

    def test_unicode_body(self):
        resp = self._get_response()
        resp.body = u'\N{SNOWMAN}'
        self.assertEqual(resp.body, u'\N{SNOWMAN}'.encode('utf-8'))

    def test_call_reifies_request_if_necessary(self):
        """
        The actual bug was a HEAD response coming out with a body because the
        Request object wasn't passed into the Response object's constructor.
        The Response object's __call__ method should be able to reify a
        Request object from the env it gets passed.
        """
        def test_app(environ, start_response):
            start_response('200 OK', [])
            return ['hi']
        req = swift.common.swob.Request.blank('/')
        req.method = 'HEAD'
        status, headers, app_iter = req.call_application(test_app)
        resp = swift.common.swob.Response(status=status, headers=dict(headers),
                                          app_iter=app_iter)
        output_iter = resp(req.environ, lambda *_: None)
        self.assertEqual(list(output_iter), [''])

    def test_call_preserves_closeability(self):
        def test_app(environ, start_response):
            start_response('200 OK', [])
            yield "igloo"
            yield "shindig"
            yield "macadamia"
            yield "hullabaloo"
        req = swift.common.swob.Request.blank('/')
        req.method = 'GET'
        status, headers, app_iter = req.call_application(test_app)
        iterator = iter(app_iter)
        self.assertEqual('igloo', next(iterator))
        self.assertEqual('shindig', next(iterator))
        app_iter.close()
        self.assertRaises(StopIteration, iterator.next)

    def test_call_finds_nonempty_chunk(self):
        def test_app(environ, start_response):
            start_response('400 Bad Request', [])
            yield ''
            start_response('200 OK', [])
            yield 'complete '
            yield ''
            yield 'response'
        req = swift.common.swob.Request.blank('/')
        req.method = 'GET'
        status, headers, app_iter = req.call_application(test_app)
        self.assertEqual(status, '200 OK')
        self.assertEqual(list(app_iter), ['complete ', '', 'response'])

    def test_call_requires_that_start_response_is_called(self):
        def test_app(environ, start_response):
            yield 'response'
        req = swift.common.swob.Request.blank('/')
        req.method = 'GET'
        with self.assertRaises(RuntimeError) as mgr:
            req.call_application(test_app)
        self.assertEqual(mgr.exception.args[0],
                         'application never called start_response')

    def test_location_rewrite(self):
        def start_response(env, headers):
            pass
        req = swift.common.swob.Request.blank(
            '/', environ={'HTTP_HOST': 'somehost'})
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'http://somehost/something')

        req = swift.common.swob.Request.blank(
            '/', environ={'HTTP_HOST': 'somehost:80'})
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'http://somehost/something')

        req = swift.common.swob.Request.blank(
            '/', environ={'HTTP_HOST': 'somehost:443',
                          'wsgi.url_scheme': 'http'})
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'http://somehost:443/something')

        req = swift.common.swob.Request.blank(
            '/', environ={'HTTP_HOST': 'somehost:443',
                          'wsgi.url_scheme': 'https'})
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'https://somehost/something')

    def test_location_rewrite_no_host(self):
        def start_response(env, headers):
            pass
        req = swift.common.swob.Request.blank(
            '/', environ={'SERVER_NAME': 'local', 'SERVER_PORT': 80})
        del req.environ['HTTP_HOST']
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'http://local/something')

        req = swift.common.swob.Request.blank(
            '/', environ={'SERVER_NAME': 'local', 'SERVER_PORT': 81})
        del req.environ['HTTP_HOST']
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'http://local:81/something')

    def test_location_no_rewrite(self):
        def start_response(env, headers):
            pass
        req = swift.common.swob.Request.blank(
            '/', environ={'HTTP_HOST': 'somehost'})
        resp = self._get_response()
        resp.location = 'http://www.google.com/'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, 'http://www.google.com/')

    def test_location_no_rewrite_when_told_not_to(self):
        def start_response(env, headers):
            pass
        req = swift.common.swob.Request.blank(
            '/', environ={'SERVER_NAME': 'local', 'SERVER_PORT': 81,
                          'swift.leave_relative_location': True})
        del req.environ['HTTP_HOST']
        resp = self._get_response()
        resp.location = '/something'
        # read response
        ''.join(resp(req.environ, start_response))
        self.assertEqual(resp.location, '/something')

    def test_app_iter(self):
        def start_response(env, headers):
            pass
        resp = self._get_response()
        resp.app_iter = ['a', 'b', 'c']
        body = ''.join(resp({}, start_response))
        self.assertEqual(body, 'abc')

    def test_multi_ranges_wo_iter_ranges(self):
        def test_app(environ, start_response):
            start_response('200 OK', [('Content-Length', '10')])
            return ['1234567890']

        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=0-9,10-19,20-29'})

        resp = req.get_response(test_app)
        resp.conditional_response = True
        resp.content_length = 10

        # read response
        ''.join(resp._response_iter(resp.app_iter, ''))

        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(10, resp.content_length)

    def test_single_range_wo_iter_range(self):
        def test_app(environ, start_response):
            start_response('200 OK', [('Content-Length', '10')])
            return ['1234567890']

        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=0-9'})

        resp = req.get_response(test_app)
        resp.conditional_response = True
        resp.content_length = 10

        # read response
        ''.join(resp._response_iter(resp.app_iter, ''))

        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(10, resp.content_length)

    def test_multi_range_body(self):
        def test_app(environ, start_response):
            start_response('200 OK', [('Content-Length', '4')])
            return ['abcd']

        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=0-9,10-19,20-29'})

        resp = req.get_response(test_app)
        resp.conditional_response = True
        resp.content_length = 100

        resp.content_type = 'text/plain; charset=utf8'
        content = ''.join(resp._response_iter(None,
                                              ('0123456789112345678'
                                               '92123456789')))

        self.assertTrue(re.match(('--[a-f0-9]{32}\r\n'
                                  'Content-Type: text/plain; charset=utf8\r\n'
                                  'Content-Range: bytes '
                                  '0-9/100\r\n\r\n0123456789\r\n'
                                  '--[a-f0-9]{32}\r\n'
                                  'Content-Type: text/plain; charset=utf8\r\n'
                                  'Content-Range: bytes '
                                  '10-19/100\r\n\r\n1123456789\r\n'
                                  '--[a-f0-9]{32}\r\n'
                                  'Content-Type: text/plain; charset=utf8\r\n'
                                  'Content-Range: bytes '
                                  '20-29/100\r\n\r\n2123456789\r\n'
                                  '--[a-f0-9]{32}--'), content))

    def test_multi_response_iter(self):
        def test_app(environ, start_response):
            start_response('200 OK', [('Content-Length', '10'),
                                      ('Content-Type', 'application/xml')])
            return ['0123456789']

        app_iter_ranges_args = []

        class App_iter(object):
            def app_iter_ranges(self, ranges, content_type, boundary, size):
                app_iter_ranges_args.append((ranges, content_type, boundary,
                                             size))
                for i in range(3):
                    yield str(i) + 'fun'
                yield boundary

            def __iter__(self):
                for i in range(3):
                    yield str(i) + 'fun'

        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=1-5,8-11'})

        resp = req.get_response(test_app)
        resp.conditional_response = True
        resp.content_length = 12

        content = ''.join(resp._response_iter(App_iter(), ''))
        boundary = content[-32:]
        self.assertEqual(content[:-32], '0fun1fun2fun')
        self.assertEqual(app_iter_ranges_args,
                         [([(1, 6), (8, 12)], 'application/xml',
                           boundary, 12)])

    def test_range_body(self):

        def test_app(environ, start_response):
            start_response('200 OK', [('Content-Length', '10')])
            return ['1234567890']

        def start_response(env, headers):
            pass

        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=1-3'})

        resp = swift.common.swob.Response(
            body='1234567890', request=req,
            conditional_response=True)
        body = ''.join(resp([], start_response))
        self.assertEqual(body, '234')
        self.assertEqual(resp.content_range, 'bytes 1-3/10')
        self.assertEqual(resp.status, '206 Partial Content')

        # syntactically valid, but does not make sense, so returning 416
        # in next couple of cases.
        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=-0'})
        resp = req.get_response(test_app)
        resp.conditional_response = True
        body = ''.join(resp([], start_response))
        self.assertIn('The Range requested is not available', body)
        self.assertEqual(resp.content_length, len(body))
        self.assertEqual(resp.status, '416 Requested Range Not Satisfiable')
        self.assertEqual(resp.content_range, 'bytes */10')

        resp = swift.common.swob.Response(
            body='1234567890', request=req,
            conditional_response=True)
        body = ''.join(resp([], start_response))
        self.assertIn('The Range requested is not available', body)
        self.assertEqual(resp.content_length, len(body))
        self.assertEqual(resp.status, '416 Requested Range Not Satisfiable')

        # Syntactically-invalid Range headers "MUST" be ignored
        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=3-2'})
        resp = req.get_response(test_app)
        resp.conditional_response = True
        body = ''.join(resp([], start_response))
        self.assertEqual(body, '1234567890')
        self.assertEqual(resp.status, '200 OK')
        self.assertNotIn('Content-Range', resp.headers)

        resp = swift.common.swob.Response(
            body='1234567890', request=req,
            conditional_response=True)
        body = ''.join(resp([], start_response))
        self.assertEqual(body, '1234567890')
        self.assertEqual(resp.status, '200 OK')

    def test_content_type(self):
        resp = self._get_response()
        resp.content_type = 'text/plain; charset=utf8'
        self.assertEqual(resp.content_type, 'text/plain')

    def test_charset(self):
        resp = self._get_response()
        resp.content_type = 'text/plain; charset=utf8'
        self.assertEqual(resp.charset, 'utf8')
        resp.charset = 'utf16'
        self.assertEqual(resp.charset, 'utf16')

    def test_charset_content_type(self):
        resp = swift.common.swob.Response(
            content_type='text/plain', charset='utf-8')
        self.assertEqual(resp.charset, 'utf-8')
        resp = swift.common.swob.Response(
            charset='utf-8', content_type='text/plain')
        self.assertEqual(resp.charset, 'utf-8')

    def test_etag(self):
        resp = self._get_response()
        resp.etag = 'hi'
        self.assertEqual(resp.headers['Etag'], '"hi"')
        self.assertEqual(resp.etag, 'hi')

        self.assertIn('etag', resp.headers)
        resp.etag = None
        self.assertNotIn('etag', resp.headers)

    def test_host_url_default(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'http'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '1234'
        del env['HTTP_HOST']
        self.assertEqual(resp.host_url, 'http://bob:1234')

    def test_host_url_default_port_squelched(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'http'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '80'
        del env['HTTP_HOST']
        self.assertEqual(resp.host_url, 'http://bob')

    def test_host_url_https(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'https'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '1234'
        del env['HTTP_HOST']
        self.assertEqual(resp.host_url, 'https://bob:1234')

    def test_host_url_https_port_squelched(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'https'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '443'
        del env['HTTP_HOST']
        self.assertEqual(resp.host_url, 'https://bob')

    def test_host_url_host_override(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'http'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '1234'
        env['HTTP_HOST'] = 'someother'
        self.assertEqual(resp.host_url, 'http://someother')

    def test_host_url_host_port_override(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'http'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '1234'
        env['HTTP_HOST'] = 'someother:5678'
        self.assertEqual(resp.host_url, 'http://someother:5678')

    def test_host_url_host_https(self):
        resp = self._get_response()
        env = resp.environ
        env['wsgi.url_scheme'] = 'https'
        env['SERVER_NAME'] = 'bob'
        env['SERVER_PORT'] = '1234'
        env['HTTP_HOST'] = 'someother:5678'
        self.assertEqual(resp.host_url, 'https://someother:5678')

    def test_507(self):
        resp = swift.common.swob.HTTPInsufficientStorage()
        content = ''.join(resp._response_iter(resp.app_iter, resp._body))
        self.assertEqual(
            content,
            '<html><h1>Insufficient Storage</h1><p>There was not enough space '
            'to save the resource. Drive: unknown</p></html>')
        resp = swift.common.swob.HTTPInsufficientStorage(drive='sda1')
        content = ''.join(resp._response_iter(resp.app_iter, resp._body))
        self.assertEqual(
            content,
            '<html><h1>Insufficient Storage</h1><p>There was not enough space '
            'to save the resource. Drive: sda1</p></html>')

    def test_200_with_body_and_headers(self):
        headers = {'Content-Length': '0'}
        content = 'foo'
        resp = swift.common.swob.HTTPOk(body=content, headers=headers)
        self.assertEqual(resp.body, content)
        self.assertEqual(resp.content_length, len(content))

    def test_init_with_body_headers_app_iter(self):
        # body exists but no headers and no app_iter
        body = 'ok'
        resp = swift.common.swob.Response(body=body)
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.content_length, len(body))

        # body and headers with 0 content_length exist but no app_iter
        body = 'ok'
        resp = swift.common.swob.Response(
            body=body, headers={'Content-Length': '0'})
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.content_length, len(body))

        # body and headers with content_length exist but no app_iter
        body = 'ok'
        resp = swift.common.swob.Response(
            body=body, headers={'Content-Length': '5'})
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.content_length, len(body))

        # body and headers with no content_length exist but no app_iter
        body = 'ok'
        resp = swift.common.swob.Response(body=body, headers={})
        self.assertEqual(resp.body, body)
        self.assertEqual(resp.content_length, len(body))

        # body, headers with content_length and app_iter exist
        resp = swift.common.swob.Response(
            body='ok', headers={'Content-Length': '5'}, app_iter=iter([]))
        self.assertEqual(resp.content_length, 5)
        self.assertEqual(resp.body, '')

        # headers with content_length and app_iter exist but no body
        resp = swift.common.swob.Response(
            headers={'Content-Length': '5'}, app_iter=iter([]))
        self.assertEqual(resp.content_length, 5)
        self.assertEqual(resp.body, '')

        # app_iter exists but no body and headers
        resp = swift.common.swob.Response(app_iter=iter([]))
        self.assertIsNone(resp.content_length)
        self.assertEqual(resp.body, '')


class TestUTC(unittest.TestCase):
    def test_tzname(self):
        self.assertEqual(swift.common.swob.UTC.tzname(None), 'UTC')


class TestConditionalIfNoneMatch(unittest.TestCase):
    def fake_app(self, environ, start_response):
        start_response('200 OK', [('Etag', 'the-etag')])
        return ['hi']

    def fake_start_response(*a, **kw):
        pass

    def test_simple_match(self):
        # etag matches --> 304
        req = swift.common.swob.Request.blank(
            '/', headers={'If-None-Match': 'the-etag'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(body, '')

    def test_quoted_simple_match(self):
        # double quotes don't matter
        req = swift.common.swob.Request.blank(
            '/', headers={'If-None-Match': '"the-etag"'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(body, '')

    def test_list_match(self):
        # it works with lists of etags to match
        req = swift.common.swob.Request.blank(
            '/', headers={'If-None-Match': '"bert", "the-etag", "ernie"'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(body, '')

    def test_list_no_match(self):
        # no matches --> whatever the original status was
        req = swift.common.swob.Request.blank(
            '/', headers={'If-None-Match': '"bert", "ernie"'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_match_star(self):
        # "*" means match anything; see RFC 2616 section 14.24
        req = swift.common.swob.Request.blank(
            '/', headers={'If-None-Match': '*'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(body, '')


class TestConditionalIfMatch(unittest.TestCase):
    def fake_app(self, environ, start_response):
        start_response('200 OK', [('Etag', 'the-etag')])
        return ['hi']

    def fake_start_response(*a, **kw):
        pass

    def test_simple_match(self):
        # if etag matches, proceed as normal
        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': 'the-etag'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_simple_conditional_etag_match(self):
        # if etag matches, proceed as normal
        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': 'not-the-etag'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        resp._conditional_etag = 'not-the-etag'
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_quoted_simple_match(self):
        # double quotes or not, doesn't matter
        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': '"the-etag"'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_no_match(self):
        # no match --> 412
        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': 'not-the-etag'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 412)
        self.assertEqual(body, '')

    def test_simple_conditional_etag_no_match(self):
        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': 'the-etag'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        resp._conditional_etag = 'not-the-etag'
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 412)
        self.assertEqual(body, '')

    def test_match_star(self):
        # "*" means match anything; see RFC 2616 section 14.24
        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': '*'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_match_star_on_404(self):

        def fake_app_404(environ, start_response):
            start_response('404 Not Found', [])
            return ['hi']

        req = swift.common.swob.Request.blank(
            '/', headers={'If-Match': '*'})
        resp = req.get_response(fake_app_404)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 412)
        self.assertEqual(body, '')


class TestConditionalIfModifiedSince(unittest.TestCase):
    def fake_app(self, environ, start_response):
        start_response(
            '200 OK', [('Last-Modified', 'Thu, 27 Feb 2014 03:29:37 GMT')])
        return ['hi']

    def fake_start_response(*a, **kw):
        pass

    def test_absent(self):
        req = swift.common.swob.Request.blank('/')
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_before(self):
        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Modified-Since': 'Thu, 27 Feb 2014 03:29:36 GMT'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_same(self):
        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Modified-Since': 'Thu, 27 Feb 2014 03:29:37 GMT'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(body, '')

    def test_greater(self):
        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Modified-Since': 'Thu, 27 Feb 2014 03:29:38 GMT'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(body, '')

    def test_out_of_range_is_ignored(self):
        # All that datetime gives us is a ValueError or OverflowError when
        # something is out of range (i.e. less than datetime.datetime.min or
        # greater than datetime.datetime.max). Unfortunately, we can't
        # distinguish between a date being too old and a date being too new,
        # so the best we can do is ignore such headers.
        max_date_list = list(datetime.datetime.max.timetuple())
        max_date_list[0] += 1  # bump up the year
        too_big_date_header = time.strftime(
            "%a, %d %b %Y %H:%M:%S GMT", time.struct_time(max_date_list))

        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Modified-Since': too_big_date_header})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')


class TestConditionalIfUnmodifiedSince(unittest.TestCase):
    def fake_app(self, environ, start_response):
        start_response(
            '200 OK', [('Last-Modified', 'Thu, 20 Feb 2014 03:29:37 GMT')])
        return ['hi']

    def fake_start_response(*a, **kw):
        pass

    def test_absent(self):
        req = swift.common.swob.Request.blank('/')
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_before(self):
        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Unmodified-Since': 'Thu, 20 Feb 2014 03:29:36 GMT'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 412)
        self.assertEqual(body, '')

    def test_same(self):
        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Unmodified-Since': 'Thu, 20 Feb 2014 03:29:37 GMT'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_greater(self):
        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Unmodified-Since': 'Thu, 20 Feb 2014 03:29:38 GMT'})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')

    def test_out_of_range_is_ignored(self):
        # All that datetime gives us is a ValueError or OverflowError when
        # something is out of range (i.e. less than datetime.datetime.min or
        # greater than datetime.datetime.max). Unfortunately, we can't
        # distinguish between a date being too old and a date being too new,
        # so the best we can do is ignore such headers.
        max_date_list = list(datetime.datetime.max.timetuple())
        max_date_list[0] += 1  # bump up the year
        too_big_date_header = time.strftime(
            "%a, %d %b %Y %H:%M:%S GMT", time.struct_time(max_date_list))

        req = swift.common.swob.Request.blank(
            '/',
            headers={'If-Unmodified-Since': too_big_date_header})
        resp = req.get_response(self.fake_app)
        resp.conditional_response = True
        body = ''.join(resp(req.environ, self.fake_start_response))
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(body, 'hi')


if __name__ == '__main__':
    unittest.main()
