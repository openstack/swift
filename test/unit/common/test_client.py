# Copyright (c) 2010 OpenStack, LLC.
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

import unittest

from swift.common import client as c

class TestHttpHelpers(unittest.TestCase):

    def test_quote(self):
        value = 'standard string'
        self.assertEquals('standard%20string', c.quote(value))
        value = u'\u0075nicode string'
        self.assertEquals('unicode%20string', c.quote(value))
    
    def test_http_connection(self):
        url = 'http://www.test.com'
        _, conn = c.http_connection(url)
        self.assertTrue(isinstance(conn, c.HTTPConnection))
        url = 'https://www.test.com'
        _, conn = c.http_connection(url)
        self.assertTrue(isinstance(conn, c.HTTPSConnection))
        url = 'ftp://www.test.com'
        self.assertRaises(c.ClientException, c.http_connection, url)

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
               'http_%s' % value: value 
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
            # that was easy
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

    def test_no_json(self):
        # first break simplejson
        try:
            import simplejson
        except ImportError:
            # that was easy
            pass
        else:
            delattr(simplejson, 'loads')

        # then break json
        try:
            import json
        except ImportError:
            # that was easy
            _orig_dumps = None
        else:
            _orig_dumps = json.dumps
            delattr(json, 'loads')
            reload(c)

        if _orig_dumps:
            # thank goodness
            data = {
                'string': 'value',
                'int': 0,
                'bool': True,
                'none': None,
            }        
            json_string = _orig_dumps(data)
        else:
            # wow, I guess we really need this thing...
            data = ['value1', 'value2']
            json_string = "['value1', 'value2']"

        self.assertEquals(data, c.json_loads(json_string))
        self.assertRaises(AttributeError, c.json_loads, self)

class MockHttpTest(unittest.TestCase):

    def setUp(self):
        # Yoink!
        from test.unit.proxy.test_server import fake_http_connect
        # TODO: mock http connection class with more control over headers
        def fake_http_connection(*args, **kwargs):
            _orig_http_connection = c.http_connection
            def wrapper(url):
                parsed, _conn = _orig_http_connection(url)
                conn = fake_http_connect(*args, **kwargs)()
                def request(*args, **kwargs):
                    return
                conn.request = request
                return parsed, conn
            return wrapper
        self.fake_http_connection = fake_http_connection

    def tearDown(self):
        reload(c)

# TODO: following tests cases are placeholders, need more tests, better coverage

class TestGetAuth(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        url, token = c.get_auth('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(url, None)
        self.assertEquals(token, None)

class TestGetAccount(MockHttpTest):

    def test_no_content(self):
        c.http_connection = self.fake_http_connection(204)
        value = c.get_account('http://www.test.com', 'asdf')
        self.assertEquals(value, [])
        
class TestHeadAccount(MockHttpTest):

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.head_account,
                          'http://www.tests.com', 'asdf')

class TestGetContainer(MockHttpTest):

    def test_no_content(self):
        c.http_connection = self.fake_http_connection(204)
        value = c.get_container('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(value, [])

class TestHeadContainer(MockHttpTest):

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.head_container,
                         'http://www.test.com', 'asdf', 'asdf',
                         )

class TestPutContainer(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.put_container('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(value, None)

class TestDeleteContainer(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.delete_container('http://www.test.com', 'asdf', 'asdf')
        self.assertEquals(value, None)

class TestGetObject(MockHttpTest):

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.get_object, 'http://www.test.com', 'asdf', 'asdf', 'asdf')

class TestHeadObject(MockHttpTest):

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.head_object, 'http://www.test.com', 'asdf', 'asdf', 'asdf')

class TestPutObject(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.put_object('http://www.test.com', 'asdf', 'asdf', 'asdf', 'asdf')
        self.assertTrue(isinstance(value, basestring))

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.put_object,
                          'http://www.test.com', 'asdf', 'asdf', 'asdf', 'asdf')

class TestPostObject(MockHttpTest):

    def test_ok(self):
        c.http_connection = self.fake_http_connection(200)
        value = c.post_object('http://www.test.com', 'asdf', 'asdf', 'asdf', {})

    def test_server_error(self):
        c.http_connection = self.fake_http_connection(500)
        self.assertRaises(c.ClientException, c.post_object,
                          'http://www.test.com', 'asdf', 'asdf', 'asdf', {})

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

if __name__ == '__main__':
    unittest.main()
