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
import logging
import os
import sys
import unittest
from contextlib import contextmanager

import eventlet
from webob import Request

from swift.common.middleware import auth

# mocks
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class FakeMemcache(object):
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except:
            pass
        return True


def mock_http_connect(response, headers=None, with_exc=False):
    class FakeConn(object):
        def __init__(self, status, headers, with_exc):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = '1234'
            self.with_exc = with_exc
            self.headers = headers
            if self.headers is None:
                self.headers = {}
        def getresponse(self):
            if self.with_exc:
                raise Exception('test')
            return self
        def getheader(self, header):
            return self.headers[header]
        def read(self, amt=None):
            return ''
        def close(self):
            return
    return lambda *args, **kwargs: FakeConn(response, headers, with_exc)


class Logger(object):
    def __init__(self):
        self.error_value = None
        self.exception_value = None
    def error(self, msg, *args, **kwargs):
        self.error_value = (msg, args, kwargs)
    def exception(self, msg, *args, **kwargs):
        _, exc, _ = sys.exc_info()
        self.exception_value = (msg,
            '%s %s' % (exc.__class__.__name__, str(exc)), args, kwargs)
# tests

class FakeApp(object):
    def __call__(self, env, start_response):
        return "OK"

def start_response(*args):
    pass

class TestAuth(unittest.TestCase):
    # TODO: With the auth refactor, these tests have to be refactored as well.
    # I brought some over from another refactor I've been trying, but these
    # also need work.

    def test_clean_acl(self):
        devauth = auth.DevAuthorization(None, None)
        value = devauth.clean_acl('header', '.ref:any')
        self.assertEquals(value, '.ref:any')
        value = devauth.clean_acl('header', '.ref:specific.host')
        self.assertEquals(value, '.ref:specific.host')
        value = devauth.clean_acl('header', '.ref:.ending.with')
        self.assertEquals(value, '.ref:.ending.with')
        value = devauth.clean_acl('header', '.ref:one,.ref:two')
        self.assertEquals(value, '.ref:one,.ref:two')
        value = devauth.clean_acl('header', '.ref:any,.ref:-specific.host')
        self.assertEquals(value, '.ref:any,.ref:-specific.host')
        value = devauth.clean_acl('header', '.ref:any,.ref:-.ending.with')
        self.assertEquals(value, '.ref:any,.ref:-.ending.with')
        value = devauth.clean_acl('header', '.ref:one,.ref:-two')
        self.assertEquals(value, '.ref:one,.ref:-two')
        value = devauth.clean_acl('header', 
                    ' .ref : one , ,, .ref:two , .ref : - three ')
        self.assertEquals(value, '.ref:one,.ref:two,.ref:-three')
        self.assertRaises(ValueError, devauth.clean_acl, 'header', '.ref:')
        self.assertRaises(ValueError, devauth.clean_acl, 'header', ' .ref : ')
        self.assertRaises(ValueError, devauth.clean_acl, 'header',
                          'user , .ref : ')
        self.assertRaises(ValueError, devauth.clean_acl, 'header', '.ref:-')
        self.assertRaises(ValueError, devauth.clean_acl, 'header',
                          ' .ref : - ')
        self.assertRaises(ValueError, devauth.clean_acl, 'header',
                          'user , .ref : - ')

    def test_parse_acl(self):
        devauth = auth.DevAuthorization(None, None)
        self.assertEquals(devauth.parse_acl(None), None)
        self.assertEquals(devauth.parse_acl(''), None)
        self.assertEquals(devauth.parse_acl('.ref:ref1'),
                          (['ref1'], [], []))
        self.assertEquals(devauth.parse_acl('.ref:-ref1'),
                          (['-ref1'], [], []))
        self.assertEquals(devauth.parse_acl('account:user'),
                          ([], [], ['account:user']))
        self.assertEquals(devauth.parse_acl('account'),
                          ([], ['account'], []))
        self.assertEquals(
            devauth.parse_acl('acc1,acc2:usr2,.ref:ref3,.ref:-ref4'),
            (['ref3', '-ref4'], ['acc1'], ['acc2:usr2']))
        self.assertEquals(devauth.parse_acl(
            'acc1,acc2:usr2,.ref:ref3,acc3,acc4:usr4,.ref:ref5,.ref:-ref6'),
            (['ref3', 'ref5', '-ref6'], ['acc1', 'acc3'],
             ['acc2:usr2', 'acc4:usr4']))


if __name__ == '__main__':
    unittest.main()
