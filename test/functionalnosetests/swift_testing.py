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

import errno
import os
import socket
import sys
from httplib import HTTPException
from time import sleep
from nose import SkipTest
from ConfigParser import MissingSectionHeaderError

from test import get_config

from swiftclient import get_auth, http_connection

conf = get_config('func_test')

# If no conf was read, we will fall back to old school env vars
swift_test_auth = os.environ.get('SWIFT_TEST_AUTH')
swift_test_user = [os.environ.get('SWIFT_TEST_USER'), None, None]
swift_test_key = [os.environ.get('SWIFT_TEST_KEY'), None, None]

if conf:
    swift_test_auth = 'http'
    if conf.get('auth_ssl', 'no').lower() in ('yes', 'true', 'on', '1'):
        swift_test_auth = 'https'
    if 'auth_prefix' not in conf:
        conf['auth_prefix'] = '/'
    try:
        swift_test_auth += \
                '://%(auth_host)s:%(auth_port)s%(auth_prefix)sv1.0' % conf
    except KeyError:
        pass  # skip
    if 'account' in conf:
        swift_test_user[0] = '%(account)s:%(username)s' % conf
    else:
        swift_test_user[0] = '%(username)s' % conf
    swift_test_key[0] = conf['password']
    try:
        swift_test_user[1] = '%s%s' % ('%s:' % conf['account2'] if 'account2'
                                       in conf else '', conf['username2'])
        swift_test_key[1] = conf['password2']
    except KeyError, err:
        pass  # old conf, no second account tests can be run
    try:
        swift_test_user[2] = '%s%s' % ('%s:' % conf['account'] if 'account'
                                       in conf else '', conf['username3'])
        swift_test_key[2] = conf['password3']
    except KeyError, err:
        pass  # old conf, no third account tests can be run

skip = not all([swift_test_auth, swift_test_user[0], swift_test_key[0]])
if skip:
    print >>sys.stderr, 'SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG'

skip2 = not all([not skip, swift_test_user[1], swift_test_key[1]])
if not skip and skip2:
    print >>sys.stderr, \
          'SKIPPING SECOND ACCOUNT FUNCTIONAL TESTS DUE TO NO CONFIG FOR THEM'

skip3 = not all([not skip, swift_test_user[2], swift_test_key[2]])
if not skip and skip3:
    print >>sys.stderr, \
          'SKIPPING THIRD ACCOUNT FUNCTIONAL TESTS DUE TO NO CONFIG FOR THEM'


class AuthError(Exception):
    pass


class InternalServerError(Exception):
    pass


url = [None, None, None]
token = [None, None, None]
parsed = [None, None, None]
conn = [None, None, None]


def retry(func, *args, **kwargs):
    """
    You can use the kwargs to override the 'retries' (default: 5) and
    'use_account' (default: 1).
    """
    global url, token, parsed, conn
    retries = kwargs.get('retries', 5)
    use_account = 1
    if 'use_account' in kwargs:
        use_account = kwargs['use_account']
        del kwargs['use_account']
    use_account -= 1
    attempts = 0
    backoff = 1
    while attempts <= retries:
        attempts += 1
        try:
            if not url[use_account] or not token[use_account]:
                url[use_account], token[use_account] = \
                    get_auth(swift_test_auth, swift_test_user[use_account],
                             swift_test_key[use_account])
                parsed[use_account] = conn[use_account] = None
            if not parsed[use_account] or not conn[use_account]:
                parsed[use_account], conn[use_account] = \
                    http_connection(url[use_account])
            return func(url[use_account], token[use_account],
                       parsed[use_account], conn[use_account], *args, **kwargs)
        except (socket.error, HTTPException):
            if attempts > retries:
                raise
            parsed[use_account] = conn[use_account] = None
        except AuthError, err:
            url[use_account] = token[use_account] = None
            continue
        except InternalServerError, err:
            pass
        if attempts <= retries:
            sleep(backoff)
            backoff *= 2
    raise Exception('No result after %s retries.' % retries)


def check_response(conn):
    resp = conn.getresponse()
    if resp.status == 401:
        resp.read()
        raise AuthError()
    elif resp.status // 100 == 5:
        resp.read()
        raise InternalServerError()
    return resp
