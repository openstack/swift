# Copyright (c) 2010-2011 OpenStack, LLC.
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

import sqlite3
from webob import Request

from swift.auth import server as auth_server
from swift.common.db import DatabaseConnectionError, get_db_connection
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
    def connect(*args, **kwargs):
        connect.last_args = args
        connect.last_kwargs = kwargs
        return FakeConn(code_iter.next())
    return connect


class TestAuthServer(unittest.TestCase):

    def setUp(self):
        self.ohttp_connect = auth_server.http_connect
        self.testdir = os.path.join(os.path.dirname(__file__),
                        'auth_server')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        self.conf = {'swift_dir': self.testdir, 'log_name': 'auth',
                     'super_admin_key': 'testkey'}
        self.controller = auth_server.AuthController(self.conf)

    def tearDown(self):
        auth_server.http_connect = self.ohttp_connect
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
            'test', 'tester', 'testing',).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        self.assertEquals(self.controller.validate_token(token + 'bad'), False)

    def test_validate_token_good(self):
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
            'test', 'tester', 'testing',).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        ttl, _junk, _junk, _junk = self.controller.validate_token(token)
        self.assert_(ttl > 0, repr(ttl))

    def test_validate_token_expired(self):
        orig_time = auth_server.time
        try:
            auth_server.time = lambda: 1
            auth_server.http_connect = fake_http_connect(201)
            cfaccount = self.controller.create_user('test', 'tester',
                            'testing').split('/')[-1]
            res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                    environ={'REQUEST_METHOD': 'GET'},
                    headers={'X-Storage-User': 'tester',
                             'X-Storage-Pass': 'testing'}))
            token = res.headers['x-storage-token']
            ttl, _junk, _junk, _junk = self.controller.validate_token(token)
            self.assert_(ttl > 0, repr(ttl))
            auth_server.time = lambda: 1 + self.controller.token_life
            self.assertEquals(self.controller.validate_token(token), False)
        finally:
            auth_server.time = orig_time

    def test_create_user_no_new_account(self):
        auth_server.http_connect = fake_http_connect(201)
        result = self.controller.create_user('', 'tester', 'testing')
        self.assertFalse(result)

    def test_create_user_no_new_user(self):
        auth_server.http_connect = fake_http_connect(201)
        result = self.controller.create_user('test', '', 'testing')
        self.assertFalse(result)

    def test_create_user_no_new_password(self):
        auth_server.http_connect = fake_http_connect(201)
        result = self.controller.create_user('test', 'tester', '')
        self.assertFalse(result)

    def test_create_user_good(self):
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test', 'tester', 'testing')
        self.assert_(url)
        self.assertEquals('/'.join(url.split('/')[:-1]),
            self.controller.default_cluster_url.rstrip('/'), repr(url))

    def test_recreate_accounts_none(self):
        auth_server.http_connect = fake_http_connect(201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '0', repr(rv))
        self.assertEquals(rv.split()[-1], '[]', repr(rv))

    def test_recreate_accounts_one(self):
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('test', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '1', repr(rv))
        self.assertEquals(rv.split()[-1], '[]', repr(rv))

    def test_recreate_accounts_several(self):
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('test1', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('test2', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('test3', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('test4', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201, 201, 201, 201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '4', repr(rv))
        self.assertEquals(rv.split()[-1], '[]', repr(rv))

    def test_recreate_accounts_one_fail(self):
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test', 'tester', 'testing')
        cfaccount = url.split('/')[-1]
        auth_server.http_connect = fake_http_connect(500)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '1', repr(rv))
        self.assertEquals(rv.split()[-1], '[%s]' % repr(cfaccount),
                          repr(rv))

    def test_recreate_accounts_several_fail(self):
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test1', 'tester', 'testing')
        cfaccounts = [url.split('/')[-1]]
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test2', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test3', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test4', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(500, 500, 500, 500)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '4', repr(rv))
        failed = rv.split('[', 1)[-1][:-1].split(', ')
        self.assertEquals(set(failed), set(repr(a) for a in cfaccounts))

    def test_recreate_accounts_several_fail_some(self):
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test1', 'tester', 'testing')
        cfaccounts = [url.split('/')[-1]]
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test2', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test3', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test4', 'tester', 'testing')
        cfaccounts.append(url.split('/')[-1])
        auth_server.http_connect = fake_http_connect(500, 201, 500, 201)
        rv = self.controller.recreate_accounts()
        self.assertEquals(rv.split()[0], '4', repr(rv))
        failed = rv.split('[', 1)[-1][:-1].split(', ')
        self.assertEquals(
            len(set(repr(a) for a in cfaccounts) - set(failed)), 2)

    def test_auth_bad_path(self):
        res = self.controller.handle_auth(
            Request.blank('', environ={'REQUEST_METHOD': 'GET'}))
        self.assertEquals(res.status_int, 400)
        res = self.controller.handle_auth(Request.blank('/bad',
                environ={'REQUEST_METHOD': 'GET'}))
        self.assertEquals(res.status_int, 400)

    def test_auth_SOSO_missing_headers(self):
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        ttl, _junk, _junk, _junk = self.controller.validate_token(token)
        self.assert_(ttl > 0, repr(ttl))

    def test_auth_SOSO_good_Mosso_headers(self):
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1/test/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester',
                         'X-Auth-Key': 'testing'}))
        token = res.headers['x-storage-token']
        ttl, _junk, _junk, _junk = self.controller.validate_token(token)
        self.assert_(ttl > 0, repr(ttl))

    def test_auth_SOSO_bad_Mosso_headers(self):
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
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
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'test:tester',
                         'X-Auth-Key': 'testing'}))
        token = res.headers['x-storage-token']
        ttl, _junk, _junk, _junk = self.controller.validate_token(token)
        self.assert_(ttl > 0, repr(ttl))

    def test_auth_Mosso_good_SOSO_header_names(self):
        auth_server.http_connect = fake_http_connect(201)
        cfaccount = self.controller.create_user(
            'test', 'tester', 'testing').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/auth',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Storage-User': 'test:tester',
                         'X-Storage-Pass': 'testing'}))
        token = res.headers['x-storage-token']
        ttl, _junk, _junk, _junk = self.controller.validate_token(token)
        self.assert_(ttl > 0, repr(ttl))

    def test_basic_logging(self):
        log = StringIO()
        log_handler = StreamHandler(log)
        logger = get_logger(self.conf, 'auth')
        logger.logger.addHandler(log_handler)
        try:
            auth_server.http_connect = fake_http_connect(201)
            url = self.controller.create_user('test', 'tester', 'testing')
            self.assertEquals(log.getvalue().rsplit(' ', 1)[0],
                "SUCCESS create_user('test', 'tester', _, False, False) "
                "= %s" % repr(url))
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
            self.assertEquals(' '.join(logsegs), 'testhost - - '
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
            self.assertEquals(' '.join(logsegs), 'None - - [01/Jan/2001:'
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
                'ERROR Unhandled exception in ReST request'),
                log.getvalue())
            log.truncate(0)
        finally:
            auth_server.Request = orig_Request
            logger.logger.handlers.remove(log_handler)

    def test_upgrading_from_db1(self):
        swift_dir = '/tmp/swift_test_auth_%s' % uuid4().hex
        os.mkdir(swift_dir)
        try:
            # Create db1
            db_file = os.path.join(swift_dir, 'auth.db')
            conn = get_db_connection(db_file, okay_to_create=True)
            conn.execute('''CREATE TABLE IF NOT EXISTS account (
                            account TEXT, url TEXT, cfaccount TEXT,
                            user TEXT, password TEXT)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_account_account
                            ON account (account)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS token (
                            cfaccount TEXT, token TEXT, created FLOAT)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_cfaccount
                            ON token (cfaccount)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_created
                            ON token (created)''')
            conn.execute('''INSERT INTO account
                            (account, url, cfaccount, user, password)
                            VALUES ('act', 'url', 'cfa', 'usr', 'pas')''')
            conn.execute('''INSERT INTO token (cfaccount, token, created)
                            VALUES ('cfa', 'tok', '1')''')
            conn.commit()
            conn.close()
            # Upgrade to current db
            conf = {'swift_dir': swift_dir, 'super_admin_key': 'testkey'}
            exc = None
            try:
                auth_server.AuthController(conf)
            except Exception, err:
                exc = err
            self.assert_(str(err).strip().startswith('THERE ARE ACCOUNTS IN '
                'YOUR auth.db THAT DO NOT BEGIN WITH YOUR NEW RESELLER'), err)
            # Check new items exist and are correct
            conn = get_db_connection(db_file)
            row = conn.execute('SELECT admin FROM account').fetchone()
            self.assertEquals(row[0], 't')
            row = conn.execute('SELECT user FROM token').fetchone()
            self.assert_(not row)
        finally:
            rmtree(swift_dir)

    def test_upgrading_from_db2(self):
        swift_dir = '/tmp/swift_test_auth_%s' % uuid4().hex
        os.mkdir(swift_dir)
        try:
            # Create db1
            db_file = os.path.join(swift_dir, 'auth.db')
            conn = get_db_connection(db_file, okay_to_create=True)
            conn.execute('''CREATE TABLE IF NOT EXISTS account (
                               account TEXT, url TEXT, cfaccount TEXT,
                               user TEXT, password TEXT, admin TEXT)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_account_account
                            ON account (account)''')
            conn.execute('''CREATE TABLE IF NOT EXISTS token (
                               token TEXT, created FLOAT,
                               account TEXT, user TEXT, cfaccount TEXT)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_token
                            ON token (token)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_created
                            ON token (created)''')
            conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_account
                            ON token (account)''')
            conn.execute('''INSERT INTO account
                            (account, url, cfaccount, user, password, admin)
                            VALUES ('act', 'url', 'cfa', 'us1', 'pas', '')''')
            conn.execute('''INSERT INTO account
                            (account, url, cfaccount, user, password, admin)
                            VALUES ('act', 'url', 'cfa', 'us2', 'pas', 't')''')
            conn.execute('''INSERT INTO token
                            (token, created, account, user, cfaccount)
                            VALUES ('tok', '1', 'act', 'us1', 'cfa')''')
            conn.commit()
            conn.close()
            # Upgrade to current db
            conf = {'swift_dir': swift_dir, 'super_admin_key': 'testkey'}
            exc = None
            try:
                auth_server.AuthController(conf)
            except Exception, err:
                exc = err
            self.assert_(str(err).strip().startswith('THERE ARE ACCOUNTS IN '
                'YOUR auth.db THAT DO NOT BEGIN WITH YOUR NEW RESELLER'), err)
            # Check new items exist and are correct
            conn = get_db_connection(db_file)
            row = conn.execute('''SELECT admin, reseller_admin
                                FROM account WHERE user = 'us1' ''').fetchone()
            self.assert_(not row[0], row[0])
            self.assert_(not row[1], row[1])
            row = conn.execute('''SELECT admin, reseller_admin
                                FROM account WHERE user = 'us2' ''').fetchone()
            self.assertEquals(row[0], 't')
            self.assert_(not row[1], row[1])
            row = conn.execute('SELECT user FROM token').fetchone()
            self.assert_(row)
        finally:
            rmtree(swift_dir)

    def test_create_user_twice(self):
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('test', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201)
        self.assertEquals(
            self.controller.create_user('test', 'tester', 'testing'),
            'already exists')
        req = Request.blank('/account/test/tester',
                headers={'X-Auth-User-Key': 'testing'})
        resp = self.controller.handle_add_user(req)
        self.assertEquals(resp.status_int, 409)


    def test_create_2users_1account(self):
        auth_server.http_connect = fake_http_connect(201)
        url = self.controller.create_user('test', 'tester', 'testing')
        auth_server.http_connect = fake_http_connect(201)
        url2 = self.controller.create_user('test', 'tester2', 'testing2')
        self.assertEquals(url, url2)

    def test_no_super_admin_key(self):
        conf = {'swift_dir': self.testdir, 'log_name': 'auth'}
        self.assertRaises(ValueError, auth_server.AuthController, conf)
        conf['super_admin_key'] = 'testkey'
        controller = auth_server.AuthController(conf)
        self.assertEquals(controller.super_admin_key, conf['super_admin_key'])

    def test_add_storage_account(self):
        auth_server.http_connect = fake_http_connect(201)
        stgact = self.controller.add_storage_account()
        self.assert_(stgact.startswith(self.controller.reseller_prefix),
                     stgact)
        # Make sure token given is the expected single use token
        token = auth_server.http_connect.last_args[-1]['X-Auth-Token']
        self.assert_(self.controller.validate_token(token))
        self.assert_(not self.controller.validate_token(token))
        auth_server.http_connect = fake_http_connect(201)
        stgact = self.controller.add_storage_account('bob')
        self.assertEquals(stgact, 'bob')
        # Make sure token given is the expected single use token
        token = auth_server.http_connect.last_args[-1]['X-Auth-Token']
        self.assert_(self.controller.validate_token(token))
        self.assert_(not self.controller.validate_token(token))

    def test_regular_user(self):
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('act', 'usr', 'pas').split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1.0',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'act:usr', 'X-Auth-Key': 'pas'}))
        _junk, _junk, _junk, stgact = \
            self.controller.validate_token(res.headers['x-auth-token'])
        self.assertEquals(stgact, '')

    def test_account_admin(self):
        auth_server.http_connect = fake_http_connect(201)
        stgact = self.controller.create_user(
            'act', 'usr', 'pas', admin=True).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1.0',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'act:usr', 'X-Auth-Key': 'pas'}))
        _junk, _junk, _junk, vstgact = \
            self.controller.validate_token(res.headers['x-auth-token'])
        self.assertEquals(stgact, vstgact)

    def test_reseller_admin(self):
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user(
            'act', 'usr', 'pas', reseller_admin=True).split('/')[-1]
        res = self.controller.handle_auth(Request.blank('/v1.0',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Auth-User': 'act:usr', 'X-Auth-Key': 'pas'}))
        _junk, _junk, _junk, stgact = \
            self.controller.validate_token(res.headers['x-auth-token'])
        self.assertEquals(stgact, '.reseller_admin')

    def test_is_account_admin(self):
        req = Request.blank('/', headers={'X-Auth-Admin-User': '.super_admin',
                                          'X-Auth-Admin-Key': 'testkey'})
        self.assert_(self.controller.is_account_admin(req, 'any'))
        req = Request.blank('/', headers={'X-Auth-Admin-User': '.super_admin',
                                          'X-Auth-Admin-Key': 'testkey2'})
        self.assert_(not self.controller.is_account_admin(req, 'any'))
        req = Request.blank('/', headers={'X-Auth-Admin-User': '.super_admi',
                                          'X-Auth-Admin-Key': 'testkey'})
        self.assert_(not self.controller.is_account_admin(req, 'any'))

        auth_server.http_connect = fake_http_connect(201, 201)
        self.controller.create_user(
            'act1', 'resadmin', 'pas', reseller_admin=True).split('/')[-1]
        self.controller.create_user('act1', 'usr', 'pas').split('/')[-1]
        self.controller.create_user(
            'act2', 'actadmin', 'pas', admin=True).split('/')[-1]

        req = Request.blank('/', headers={'X-Auth-Admin-User': 'act1:resadmin',
                                          'X-Auth-Admin-Key': 'pas'})
        self.assert_(self.controller.is_account_admin(req, 'any'))
        self.assert_(self.controller.is_account_admin(req, 'act1'))
        self.assert_(self.controller.is_account_admin(req, 'act2'))

        req = Request.blank('/', headers={'X-Auth-Admin-User': 'act1:usr',
                                          'X-Auth-Admin-Key': 'pas'})
        self.assert_(not self.controller.is_account_admin(req, 'any'))
        self.assert_(not self.controller.is_account_admin(req, 'act1'))
        self.assert_(not self.controller.is_account_admin(req, 'act2'))

        req = Request.blank('/', headers={'X-Auth-Admin-User': 'act2:actadmin',
                                          'X-Auth-Admin-Key': 'pas'})
        self.assert_(not self.controller.is_account_admin(req, 'any'))
        self.assert_(not self.controller.is_account_admin(req, 'act1'))
        self.assert_(self.controller.is_account_admin(req, 'act2'))

    def test_handle_add_user_create_reseller_admin(self):
        auth_server.http_connect = fake_http_connect(201)
        self.controller.create_user('act', 'usr', 'pas')
        self.controller.create_user('act', 'actadmin', 'pas', admin=True)
        self.controller.create_user('act', 'resadmin', 'pas',
                                    reseller_admin=True)

        req = Request.blank('/account/act/resadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Reseller-Admin': 'true'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/resadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Reseller-Admin': 'true',
                         'X-Auth-Admin-User': 'act:usr',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/resadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Reseller-Admin': 'true',
                         'X-Auth-Admin-User': 'act:actadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/resadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Reseller-Admin': 'true',
                         'X-Auth-Admin-User': 'act:resadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/resadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Reseller-Admin': 'true',
                         'X-Auth-Admin-User': '.super_admin',
                         'X-Auth-Admin-Key': 'testkey'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

    def test_handle_add_user_create_account_admin(self):
        auth_server.http_connect = fake_http_connect(201, 201)
        self.controller.create_user('act', 'usr', 'pas')
        self.controller.create_user('act', 'actadmin', 'pas', admin=True)
        self.controller.create_user('act2', 'actadmin', 'pas', admin=True)
        self.controller.create_user('act2', 'resadmin', 'pas',
                                    reseller_admin=True)

        req = Request.blank('/account/act/actadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/actadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:usr',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/actadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act2:actadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/actadmin2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:actadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

        req = Request.blank('/account/act/actadmin3',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act2:resadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

        req = Request.blank('/account/act/actadmin4',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': '.super_admin',
                         'X-Auth-Admin-Key': 'testkey'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

    def test_handle_add_user_create_normal_user(self):
        auth_server.http_connect = fake_http_connect(201, 201)
        self.controller.create_user('act', 'usr', 'pas')
        self.controller.create_user('act', 'actadmin', 'pas', admin=True)
        self.controller.create_user('act2', 'actadmin', 'pas', admin=True)
        self.controller.create_user('act2', 'resadmin', 'pas',
                                    reseller_admin=True)

        req = Request.blank('/account/act/usr2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/usr2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:usr',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/usr2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act2:actadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/account/act/usr2',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:actadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

        req = Request.blank('/account/act/usr3',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act2:resadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

        req = Request.blank('/account/act/usr4',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': '.super_admin',
                         'X-Auth-Admin-Key': 'testkey'})
        resp = self.controller.handle_add_user(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)

    def test_handle_account_recreate_permissions(self):
        auth_server.http_connect = fake_http_connect(201, 201)
        self.controller.create_user('act', 'usr', 'pas')
        self.controller.create_user('act', 'actadmin', 'pas', admin=True)
        self.controller.create_user('act', 'resadmin', 'pas',
                                    reseller_admin=True)

        req = Request.blank('/recreate_accounts',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true'})
        resp = self.controller.handle_account_recreate(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/recreate_accounts',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:usr',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_account_recreate(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/recreate_accounts',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:actadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_account_recreate(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/recreate_accounts',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': 'act:resadmin',
                         'X-Auth-Admin-Key': 'pas'})
        resp = self.controller.handle_account_recreate(req)
        self.assert_(resp.status_int // 100 == 4, resp.status_int)

        req = Request.blank('/recreate_accounts',
                headers={'X-Auth-User-Key': 'pas',
                         'X-Auth-User-Admin': 'true',
                         'X-Auth-Admin-User': '.super_admin',
                         'X-Auth-Admin-Key': 'testkey'})
        resp = self.controller.handle_account_recreate(req)
        self.assert_(resp.status_int // 100 == 2, resp.status_int)


if __name__ == '__main__':
    unittest.main()
