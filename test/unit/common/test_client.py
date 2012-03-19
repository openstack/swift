# Copyright (c) 2010-2012 OpenStack, LLC.
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

# TODO: More tests
import socket
import unittest
from urlparse import urlparse

# TODO: mock http connection class with more control over headers
from test.unit.proxy.test_server import fake_http_connect

from swift.common import client as c


class TestClientException(unittest.TestCase):

    def test_is_exception(self):
        self.assertTrue(issubclass(c.ClientException, Exception))

    def test_format(self):
        exc = c.ClientException('something failed')
        self.assertTrue('something failed' in str(exc))
        test_kwargs = (
            'scheme',
            'host',
            'port',
            'path',
            'query',
            'status',
            'reason',
            'device',
        )
        for value in test_kwargs:
            kwargs = {
               'http_%s' % value: value,
            }
            exc = c.ClientException('test', **kwargs)
            self.assertTrue(value in str(exc))


class TestJsonImport(unittest.TestCase):

    def tearDown(self):
        try:
            import json
        except ImportError:
            pass
        else:
            reload(json)

        try:
            import simplejson
        except ImportError:
            pass
        else:
            reload(simplejson)

    def test_any(self):
        self.assertTrue(hasattr(c, 'json_loads'))

    def test_no_simplejson(self):
        # break simplejson
        try:
            import simplejson
        except ImportError:
            # not installed, so we don't have to break it for these tests
            pass
        else:
            delattr(simplejson, 'loads')
            reload(c)

        try:
            from json import loads
        except ImportError:
            # this case is stested in _no_json
            pass
        else:
            self.assertEquals(loads, c.json_loads)


class MockHttpTest(unittest.TestCase):

    def setUp(self):
        def fake_http_connection(*args, **kwargs):
            _orig_http_connection = c.http_connection
            return_read = kwargs.get('return_read')

            def wrapper(url, proxy=None):
                parsed, _conn = _orig_http_connection(url, proxy=proxy)
                conn = fake_http_connect(*args, **kwargs)()

                def request(*args, **kwargs):
                    return
                conn.request = request

                conn.has_been_read = False
                _orig_read = conn.read

                def read(*args, **kwargs):
                    conn.has_been_read = True
                    return _orig_read(*args, **kwargs)
                conn.read = return_read or read

                return parsed, conn
            return wrapper
        self.fake_http_connection = fake_http_connection

    def tearDown(self):
        reload(c)


class TestHttpHelpers(MockHttpTest):

    def test_quote(self):
        value = 'standard string'
        self.assertEquals('standard%20string', c.quote(value))
        value = u'\u0075nicode string'
        self.assertEquals('unicode%20string', c.quote(value))

    def test_http_connection(self):
        url = 'http://www.test.com'
        _junk, conn = c.http_connection(url)
        self.assertTrue(isinstance(conn, c.HTTPConnection))
        url = 'https://www.test.com'
        _junk, conn = c.http_connection(url)
        self.assertTrue(isinstance(conn, c.HTTPSConnection))
        url = 'ftp://www.test.com'
        self.assertRaises(c.ClientException, c.http_connection, url)

    def test_json_request(self):
        def read(*args, **kwargs):
            body = {'a': '1',
                    'b': '2'}
            return c.json_dumps(body)
        c.http_connection = self.fake_http_connection(200, return_read=read)
        url = 'http://www.test.com'
        _junk, conn = c.json_request('GET', url, body={'username': 'user1',
                                                       'password': 'secure'})
        self.assertTrue(type(conn) is dict)

# TODO: following tests are placeholders, need more tests, better coverage


class TestGetAuth(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        url, token = c.get_auth('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(url, None)
        self.assertEquals(token, None)

    def test_auth_v1(self):
        c.http_connection = self.fake_http_connection(200)
        url, token = c.get_auth('http://www.test.com', 'asdf', 'asdf',
                                auth_version="1.0")
        self.assertEquals(url, None)
        self.assertEquals(token, None)

    def test_auth_v2(self):
        def read(*args, **kwargs):
            acct_url = 'http://127.0.01/AUTH_FOO'
            body = {'access': {'serviceCatalog':
                                   [{u'endpoints': [{'publicURL': acct_url}],
                                     'type': 'object-store'}],
                               'token': {'id': 'XXXXXXX'}}}
            return c.json_dumps(body)
        c.http_connection = self.fake_http_connection(200, return_read=read)
        url, token = c.get_auth('http://www.test.com', 'asdf', 'asdf',
                                auth_version="2.0")
        self.assertTrue(url.startswith("http"))
        self.assertTrue(token)


class TestGetAccount(MockHttpTest):

    def test_no_content(self):
        c.http_connection = self.fake_http_connection(204)
        value = c.get_account('http://www.test.com', 'asdf')[1]
        self.assertEquals(value, [])


class TestHeadAccount(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.head_account('http://www.tests.com', 'asdf')
        # TODO: Hmm. This doesn't really test too much as it uses a fake that
        # always returns the same dict. I guess it "exercises" the code, so
        # I'll leave it for now.
        self.assertEquals(type(value), dict)

    def test_server_error(self):
        body = 'c' * 65
        c.http_connection = self.fake_http_connection(500, body=body)
        self.assertRaises(c.ClientException, c.head_account,
                          'http://www.tests.com', 'asdf')
        try:
            value = c.head_account('http://www.tests.com', 'asdf')
        except c.ClientException as e:
            new_body = "[first 60 chars of response] " + body[0:60]
            self.assertEquals(e.__str__()[-89:], new_body)


class TestGetContainer(MockHttpTest):

    def test_no_content(self):
        c.http_connection = self.fake_http_connection(204)
        value = c.get_container('http://www.test.com', 'asdf', 'asdf')[1]
        self.assertEquals(value, [])


class TestHeadContainer(MockHttpTest):

    def test_server_error(self):
        body = 'c' * 60
        c.http_connection = self.fake_http_connection(500, body=body)
        self.assertRaises(c.ClientException, c.head_container,
                         'http://www.test.com', 'asdf', 'asdf',
                         )
        try:
            value = c.head_container('http://www.test.com', 'asdf', 'asdf')
        except c.ClientException as e:
            self.assertEquals(e.http_response_content, body)


class TestPutContainer(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.put_container('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(value, None)

    def test_server_error(self):
        body = 'c' * 60
        c.http_connection = self.fake_http_connection(500, body=body)
        self.assertRaises(c.ClientException, c.put_container,
                         'http://www.test.com', 'asdf', 'asdf',
                         )
        try:
            value = c.put_container('http://www.test.com', 'asdf', 'asdf')
        except c.ClientException as e:
            self.assertEquals(e.http_response_content, body)


class TestDeleteContainer(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.delete_container('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(value, None)


class TestGetObject(MockHttpTest):

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.get_object,
                          'http://www.test.com', 'asdf', 'asdf', 'asdf')


class TestHeadObject(MockHttpTest):

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.head_object,
                          'http://www.test.com', 'asdf', 'asdf', 'asdf')


class TestPutObject(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        args = ('http://www.test.com', 'asdf', 'asdf', 'asdf', 'asdf')
        value = c.put_object(*args)
        self.assertTrue(isinstance(value, basestring))

    def test_server_error(self):
        body = 'c' * 60
        c.http_connection = self.fake_http_connection(500, body=body)
        args = ('http://www.test.com', 'asdf', 'asdf', 'asdf', 'asdf')
        self.assertRaises(c.ClientException, c.put_object, *args)
        try:
            value = c.put_object(*args)
        except c.ClientException as e:
            self.assertEquals(e.http_response_content, body)


class TestPostObject(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        args = ('http://www.test.com', 'asdf', 'asdf', 'asdf', {})
        value = c.post_object(*args)

    def test_server_error(self):
        body = 'c' * 60
        c.http_connection = self.fake_http_connection(500, body=body)
        args = ('http://www.test.com', 'asdf', 'asdf', 'asdf', {})
        self.assertRaises(c.ClientException, c.post_object, *args)
        try:
            value = c.post_object(*args)
        except c.ClientException as e:
            self.assertEquals(e.http_response_content, body)


class TestDeleteObject(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.delete_object('http://www.test.com', 'asdf', 'asdf', 'asdf')

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.delete_object,
                          'http://www.test.com', 'asdf', 'asdf', 'asdf')


class TestConnection(MockHttpTest):

    def test_instance(self):
        conn = c.Connection('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(conn.retries, 5)

    def test_retry(self):
        c.http_connection = self.fake_http_connection(500)

        def quick_sleep(*args):
            pass
        c.sleep = quick_sleep
        conn = c.Connection('http://www.test.com', 'asdf', 'asdf')
        self.assertRaises(c.ClientException, conn.head_account)
        self.assertEquals(conn.attempts, conn.retries + 1)

    def test_resp_read_on_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        conn = c.Connection('http://www.test.com', 'asdf', 'asdf', retries=0)

        def get_auth(*args, **kwargs):
            return 'http://www.new.com', 'new'
        conn.get_auth = get_auth
        self.url, self.token = conn.get_auth()

        method_signatures = (
            (conn.head_account, []),
            (conn.get_account, []),
            (conn.head_container, ('asdf',)),
            (conn.get_container, ('asdf',)),
            (conn.put_container, ('asdf',)),
            (conn.delete_container, ('asdf',)),
            (conn.head_object, ('asdf', 'asdf')),
            (conn.get_object, ('asdf', 'asdf')),
            (conn.put_object, ('asdf', 'asdf', 'asdf')),
            (conn.post_object, ('asdf', 'asdf', {})),
            (conn.delete_object, ('asdf', 'asdf')),
        )

        for method, args in method_signatures:
            self.assertRaises(c.ClientException, method, *args)
            try:
                self.assertTrue(conn.http_conn[1].has_been_read)
            except AssertionError:
                msg = '%s did not read resp on server error' % method.__name__
                self.fail(msg)
            except Exception, e:
                raise e.__class__("%s - %s" % (method.__name__, e))

    def test_reauth(self):
        c.http_connection = self.fake_http_connection(401)

        def get_auth(*args, **kwargs):
            return 'http://www.new.com', 'new'

        def swap_sleep(*args):
            self.swap_sleep_called = True
            c.get_auth = get_auth
            c.http_connection = self.fake_http_connection(200)
        c.sleep = swap_sleep
        self.swap_sleep_called = False

        conn = c.Connection('http://www.test.com', 'asdf', 'asdf',
                            preauthurl='http://www.old.com',
                            preauthtoken='old',
                           )

        self.assertEquals(conn.attempts, 0)
        self.assertEquals(conn.url, 'http://www.old.com')
        self.assertEquals(conn.token, 'old')

        value = conn.head_account()

        self.assertTrue(self.swap_sleep_called)
        self.assertEquals(conn.attempts, 2)
        self.assertEquals(conn.url, 'http://www.new.com')
        self.assertEquals(conn.token, 'new')

    def test_reset_stream(self):

        class LocalContents(object):

            def __init__(self, tell_value=0):
                self.already_read = False
                self.seeks = []
                self.tell_value = tell_value

            def tell(self):
                return self.tell_value

            def seek(self, position):
                self.seeks.append(position)
                self.already_read = False

            def read(self, size=-1):
                if self.already_read:
                    return ''
                else:
                    self.already_read = True
                    return 'abcdef'

        class LocalConnection(object):

            def putrequest(self, *args, **kwargs):
                return

            def putheader(self, *args, **kwargs):
                return

            def endheaders(self, *args, **kwargs):
                return

            def send(self, *args, **kwargs):
                raise socket.error('oops')

            def request(self, *args, **kwargs):
                return

            def getresponse(self, *args, **kwargs):
                self.status = 200
                return self

            def getheader(self, *args, **kwargs):
                return ''

            def read(self, *args, **kwargs):
                return ''

        def local_http_connection(url, proxy=None):
            parsed = urlparse(url)
            return parsed, LocalConnection()

        orig_conn = c.http_connection
        try:
            c.http_connection = local_http_connection
            conn = c.Connection('http://www.example.com', 'asdf', 'asdf',
                                retries=1, starting_backoff=.0001)

            contents = LocalContents()
            exc = None
            try:
                conn.put_object('c', 'o', contents)
            except socket.error, err:
                exc = err
            self.assertEquals(contents.seeks, [0])
            self.assertEquals(str(exc), 'oops')

            contents = LocalContents(tell_value=123)
            exc = None
            try:
                conn.put_object('c', 'o', contents)
            except socket.error, err:
                exc = err
            self.assertEquals(contents.seeks, [123])
            self.assertEquals(str(exc), 'oops')

            contents = LocalContents()
            contents.tell = None
            exc = None
            try:
                conn.put_object('c', 'o', contents)
            except c.ClientException, err:
                exc = err
            self.assertEquals(contents.seeks, [])
            self.assertEquals(str(exc), "put_object('c', 'o', ...) failure "
                "and no ability to reset contents for reupload.")
        finally:
            c.http_connection = orig_conn


if __name__ == '__main__':
    unittest.main()
