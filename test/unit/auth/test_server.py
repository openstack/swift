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

from __future__ import with_statement
import unittest
import os
from shutil import rmtree
from StringIO import StringIO
from uuid import uuid4
from logging import StreamHandler

from webob import Request

from swift.auth import server as auth_server
from swift.common.db import DatabaseConnectionError
from swift.common.utils import get_logger


class TestException(Exception):
    pass


def fake_http_connect(*code_iter, **kwargs):
    class FakeConn(object):
        def __init__(self, status):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = '1234'
        def getresponse(self):
            if 'slow' in kwargs:
                sleep(0.2)
            if 'raise_exc' in kwargs:
                raise kwargs['raise_exc']
            return self
        def getheaders(self):
            return {'x-account-bytes-used': '20'}
        def read(self, amt=None):
            return ''
        def getheader(self, name):
            return self.getheaders().get(name.lower())
    code_iter = iter(code_iter)
    def connect(*args, **ckwargs):
        if 'give_content_type' in kwargs:
            if len(args) >= 7 and 'content_type' in args[6]:
                kwargs['give_content_type'](args[6]['content-type'])
            else:
                kwargs['give_content_type']('')
        return FakeConn(code_iter.next())
    return connect


class FakeRing(object):
    def get_nodes(self, path):
        return 1, [{'ip': '10.0.0.%s' % x, 'port': 1000+x, 'device': 'sda'}
                    for x in xrange(3)]


class TestAuthServer(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(os.path.dirname(__file__),
                        'auth_server')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        self.conf = {'swift_dir': self.testdir}
        self.controller = auth_server.AuthController(self.conf, FakeRing())

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_get_conn(self):
        with self.controller.get_conn() as conn:
            pass
        exc = False
        try:
            with self.controller.get_conn() as conn:
                raise TestException('test')
        except TestException:
            exc = True
        self.assert_(exc)
        # We allow reentrant calls for the auth-server
        with self.controller.get_conn() as conn1:
            exc = False
            try:
                with self.controller.get_conn() as conn2:
                    self.assert_(conn1 is not conn2)
            except DatabaseConnectionError:
                exc = True
            self.assert_(not exc)
        self.controller.conn = None
        with self.controller.get_conn() as conn:
            self.assert_(conn is not None)

    def test_validate_token_non_existant_token(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing',).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        self.assertEquals(self.controller.validate_token(token + 'bad',
            cfaccount), False)

    def test_validate_token_non_existant_cfaccount(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        self.assertEquals(self.controller.validate_token(token,
            cfaccount + 'bad'), False)

    def test_validate_token_good(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing',).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        ttl = self.controller.validate_token(token, cfaccount)
        self.assert_(ttl > 0, repr(ttl))

    def test_validate_token_expired(self):
        orig_time = auth_server.time
        try:
            auth_server.time = lambda: 1
            auth_server.http_connect = fake_http_connect(201, 201, 201)
            cfaccount = self.controller.create_account('test', 'tester',
                            'testing').split('/')[-1]
            res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                    environ={'REQUEST_METHOD': 'GET'},
                    headers={'X-Storage-User': 'tester',
                             'X-Storage-Pass': 'testing'}))
            token = res.headers['x-storage-token']
            ttl = self.controller.validate_token(
                token, cfaccount)
            self.assert_(ttl > 0, repr(ttl))
            auth_server.time = lambda: 1 + self.controller.token_life
            self.assertEquals(self.controller.validate_token(
                token, cfaccount), False)
        finally:
            auth_server.time = orig_time

    def test_create_account_no_new_account(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        result = self.controller.create_account('', 'tester', 'testing')
        self.assertFalse(result)

    def test_create_account_no_new_user(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        result = self.controller.create_account('test', '', 'testing')
        self.assertFalse(result)

    def test_create_account_no_new_password(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        result = self.controller.create_account('test', 'tester', '')
        self.assertFalse(result)

    def test_create_account_good(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test', 'tester', 'testing')
        self.assert_(url)
        self.assertEquals('/'.join(url.split('/')[:-1]),
            self.controller.default_cluster_url.rstrip('/'), repr(url))

    def test_recreate_accounts_none(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '0', repr(rv))
        self.assertEquals(rv.split()[-1], '[]', repr(rv))

    def test_recreate_accounts_one(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        self.controller.create_account('test', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '1', repr(rv))
        self.assertEquals(rv.split()[-1], '[]', repr(rv))

    def test_recreate_accounts_several(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        self.controller.create_account('test1', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        self.controller.create_account('test2', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        self.controller.create_account('test3', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        self.controller.create_account('test4', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201, 201, 201,
                                                     201, 201, 201,
                                                     201, 201, 201,
                                                     201, 201, 201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '4', repr(rv))
        self.assertEquals(rv.split()[-1], '[]', repr(rv))

    def test_recreate_accounts_one_fail(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test', 'tester', 'testing')
        cfaccount = url.split('/')[-1]
        auth_server.http_connect = fake_http_connect(500, 500, 500)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '1', repr(rv))
        self.assertEquals(rv.split()[-1], '[%s]' % repr(cfaccount),
                          repr(rv))

    def test_recreate_accounts_several_fail(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test1', 'tester', 'testing')
        cfaccounts = [url.split('/')[-1]]
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test2', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test3', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test4', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(500, 500, 500,
                                                     500, 500, 500,
                                                     500, 500, 500,
                                                     500, 500, 500)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '4', repr(rv))
        failed = rv.split('[', 1)[-1][:-1].split(', ')
        self.assertEquals(failed, [repr(a) for a in cfaccounts])

    def test_recreate_accounts_several_fail_some(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test1', 'tester', 'testing')
        cfaccounts = [url.split('/')[-1]]
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test2', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test3', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        url = self.controller.create_account('test4', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(500, 500, 500,
                                                     201, 201, 201,
                                                     500, 500, 500,
                                                     201, 201, 201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '4', repr(rv))
        failed = rv.split('[', 1)[-1][:-1].split(', ')
        expected = []
        for i, value in enumerate(cfaccounts):
            if not i % 2:
                expected.append(repr(value))
        self.assertEquals(failed, expected)

    def test_auth_bad_path(self):
        self.assertRaises(ValueError, self.controller.handle_auth,
            Request.blank('', environ={'REQUEST_METHOD': 'GET'}))
        res = self.controller.handle_auth(Request.blank('/bad',
                environ={'REQUEST_METHOD': 'GET'}))
        self.assertEquals(res.status_int, 400)

    def test_auth_SOSO_missing_headers(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-Pass': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_SOSO_bad_account(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/testbad/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1//auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_SOSO_bad_user(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'testerbad',
                         'X-Storage-Pass': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': '',
                         'X-Storage-Pass': 'testing'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_SOSO_bad_password(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testingbad'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': ''}))
        self.assertEquals(res.status_int, 401)

    def test_auth_SOSO_good(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        ttl = self.controller.validate_token(token, cfaccount)
        self.assert_(ttl > 0, repr(ttl))

    def test_auth_SOSO_good_Mosso_headers(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester',
                         'X-Auth-Key': 'testing'}))
        token = res.headers['x-storage-token']
        ttl = self.controller.validate_token(token, cfaccount)
        self.assert_(ttl > 0, repr(ttl))

    def test_auth_SOSO_bad_Mosso_headers(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing',).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test2:tester',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': ':tester',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_Mosso_missing_headers(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_Mosso_bad_header_format(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'badformat',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': '',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_Mosso_bad_account(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'testbad:tester',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': ':tester',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_Mosso_bad_user(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:testerbad',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:',
                         'X-Auth-Key': 'testing'}))
        self.assertEquals(res.status_int, 401)

    def test_auth_Mosso_bad_password(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester',
                         'X-Auth-Key': 'testingbad'}))
        self.assertEquals(res.status_int, 401)
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester',
                         'X-Auth-Key': ''}))
        self.assertEquals(res.status_int, 401)

    def test_auth_Mosso_good(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester',
                         'X-Auth-Key': 'testing'}))
        token = res.headers['x-storage-token']
        ttl = self.controller.validate_token(token, cfaccount)
        self.assert_(ttl > 0, repr(ttl))

    def test_auth_Mosso_good_SOSO_header_names(self):
        auth_server.http_connect = fake_http_connect(201, 201, 201)
        cfaccount = self.controller.create_account(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'test:tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        ttl = self.controller.validate_token(token, cfaccount)
        self.assert_(ttl > 0, repr(ttl))

    def test_basic_logging(self):
        log = StringIO()
        log_handler = StreamHandler(log)
        logger = get_logger(self.conf, 'auth')
        logger.logger.addHandler(log_handler)
        try:
            auth_server.http_connect = fake_http_connect(201, 201, 201)
            url = self.controller.create_account('test', 'tester', 'testing')
            self.assertEquals(log.getvalue().rsplit(' ', 1)[0],
                "auth SUCCESS create_account('test', 'tester', _) = %s" %
                repr(url))
            log.truncate(0)
            def start_response(*args):
                pass
            self.controller.handleREST({'REQUEST_METHOD': 'GET',
                                     'SCRIPT_NAME': '',
                                     'PATH_INFO': '/v1/test/auth',
                                     'QUERY_STRING': 'test=True',
                                     'SERVER_NAME': '127.0.0.1',
                                     'SERVER_PORT': '8080',
                                     'SERVER_PROTOCOL': 'HTTP/1.0',
                                     'CONTENT_LENGTH': '0',
                                     'wsgi.version': (1, 0),
                                     'wsgi.url_scheme': 'http',
                                     'wsgi.input': StringIO(),
                                     'wsgi.errors': StringIO(),
                                     'wsgi.multithread': False,
                                     'wsgi.multiprocess': False,
                                     'wsgi.run_once': False,
                                     'HTTP_X_FORWARDED_FOR': 'testhost',
                                     'HTTP_X_STORAGE_USER': 'tester',
                                     'HTTP_X_STORAGE_PASS': 'testing'},
                                    start_response)
            logsegs = log.getvalue().split(' [', 1)
            logsegs[1:] = logsegs[1].split('] ', 1)
            logsegs[1] = '[01/Jan/2001:01:02:03 +0000]'
            logsegs[2:] = logsegs[2].split(' ')
            logsegs[-1] = '0.1234'
            self.assertEquals(' '.join(logsegs), 'auth testhost - - '
                '[01/Jan/2001:01:02:03 +0000] "GET /v1/test/auth?test=True '
                'HTTP/1.0" 204 - "-" "-" - - - - - - - - - "-" "None" "-" '
                '0.1234')
            self.controller.log_headers = True
            log.truncate(0)
            self.controller.handleREST({'REQUEST_METHOD': 'GET',
                                     'SCRIPT_NAME': '',
                                     'PATH_INFO': '/v1/test/auth',
                                     'SERVER_NAME': '127.0.0.1',
                                     'SERVER_PORT': '8080',
                                     'SERVER_PROTOCOL': 'HTTP/1.0',
                                     'CONTENT_LENGTH': '0',
                                     'wsgi.version': (1, 0),
                                     'wsgi.url_scheme': 'http',
                                     'wsgi.input': StringIO(),
                                     'wsgi.errors': StringIO(),
                                     'wsgi.multithread': False,
                                     'wsgi.multiprocess': False,
                                     'wsgi.run_once': False,
                                     'HTTP_X_STORAGE_USER': 'tester',
                                     'HTTP_X_STORAGE_PASS': 'testing'},
                                    start_response)
            logsegs = log.getvalue().split(' [', 1)
            logsegs[1:] = logsegs[1].split('] ', 1)
            logsegs[1] = '[01/Jan/2001:01:02:03 +0000]'
            logsegs[2:] = logsegs[2].split(' ')
            logsegs[-1] = '0.1234'
            self.assertEquals(' '.join(logsegs), 'auth None - - [01/Jan/2001:'
                '01:02:03 +0000] "GET /v1/test/auth HTTP/1.0" 204 - "-" "-" - '
                '- - - - - - - - "-" "None" "Content-Length: 0\n'
                'X-Storage-User: tester\nX-Storage-Pass: testing" 0.1234')
        finally:
            logger.logger.handlers.remove(log_handler)

    def test_unhandled_exceptions(self):
        def request_causing_exception(*args, **kwargs):
            pass
        def start_response(*args):
            pass
        orig_Request = auth_server.Request
        log = StringIO()
        log_handler = StreamHandler(log)
        logger = get_logger(self.conf, 'auth')
        logger.logger.addHandler(log_handler)
        try:
            auth_server.Request = request_causing_exception
            self.controller.handleREST({'REQUEST_METHOD': 'GET',
                                     'SCRIPT_NAME': '',
                                     'PATH_INFO': '/v1/test/auth',
                                     'SERVER_NAME': '127.0.0.1',
                                     'SERVER_PORT': '8080',
                                     'SERVER_PROTOCOL': 'HTTP/1.0',
                                     'CONTENT_LENGTH': '0',
                                     'wsgi.version': (1, 0),
                                     'wsgi.url_scheme': 'http',
                                     'wsgi.input': StringIO(),
                                     'wsgi.errors': StringIO(),
                                     'wsgi.multithread': False,
                                     'wsgi.multiprocess': False,
                                     'wsgi.run_once': False,
                                     'HTTP_X_STORAGE_USER': 'tester',
                                     'HTTP_X_STORAGE_PASS': 'testing'},
                                    start_response)
            self.assert_(log.getvalue().startswith(
                'auth ERROR Unhandled exception in ReST request'),
                log.getvalue())
            log.truncate(0)
        finally:
            auth_server.Request = orig_Request
            logger.logger.handlers.remove(log_handler)


if __name__ == '__main__':
    unittest.main()
