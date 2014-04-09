# Copyright (c) 2014 OpenStack Foundation
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

import os
import sys
import socket
import locale
import functools
from time import sleep
from httplib import HTTPException
from urlparse import urlparse
from nose import SkipTest

from swift.common import constraints

from swiftclient import get_auth, http_connection

from test import get_config
from test.functional.swift_test_client import Connection


config = {}
web_front_end = None
normalized_urls = None

# If no config was read, we will fall back to old school env vars
swift_test_auth_version = None
swift_test_auth = os.environ.get('SWIFT_TEST_AUTH')
swift_test_user = [os.environ.get('SWIFT_TEST_USER'), None, None]
swift_test_key = [os.environ.get('SWIFT_TEST_KEY'), None, None]
swift_test_tenant = ['', '', '']
swift_test_perm = ['', '', '']

skip, skip2, skip3 = False, False, False

orig_collate = ''


def setup_package():
    global config
    config.update(get_config('func_test'))
    for k in constraints.DEFAULT_CONSTRAINTS:
        if k in config:
            # prefer what's in test.conf
            config[k] = int(config[k])
        elif constraints.SWIFT_CONSTRAINTS_LOADED:
            # swift.conf exists, so use what's defined there (or swift
            # defaults) This normally happens when the test is running locally
            # to the cluster as in a SAIO.
            config[k] = constraints.EFFECTIVE_CONSTRAINTS[k]
        else:
            # .functests don't know what the constraints of the tested cluster
            # are, so the tests can't reliably pass or fail. Therefore, skip
            # those tests.
            config[k] = '%s constraint is not defined' % k

    global web_front_end
    web_front_end = config.get('web_front_end', 'integral')
    global normalized_urls
    normalized_urls = config.get('normalized_urls', False)

    global orig_collate
    orig_collate = locale.setlocale(locale.LC_COLLATE)
    locale.setlocale(locale.LC_COLLATE, config.get('collate', 'C'))

    global swift_test_auth_version
    global swift_test_auth
    global swift_test_user
    global swift_test_key
    global swift_test_tenant
    global swift_test_perm

    if config:
        swift_test_auth_version = str(config.get('auth_version', '1'))

        swift_test_auth = 'http'
        if config.get('auth_ssl', 'no').lower() in ('yes', 'true', 'on', '1'):
            swift_test_auth = 'https'
        if 'auth_prefix' not in config:
            config['auth_prefix'] = '/'
        try:
            suffix = '://%(auth_host)s:%(auth_port)s%(auth_prefix)s' % config
            swift_test_auth += suffix
        except KeyError:
            pass  # skip

        if swift_test_auth_version == "1":
            swift_test_auth += 'v1.0'

            try:
                if 'account' in config:
                    swift_test_user[0] = '%(account)s:%(username)s' % config
                else:
                    swift_test_user[0] = '%(username)s' % config
                swift_test_key[0] = config['password']
            except KeyError:
                # bad config, no account/username configured, tests cannot be
                # run
                pass
            try:
                swift_test_user[1] = '%s%s' % (
                    '%s:' % config['account2'] if 'account2' in config else '',
                    config['username2'])
                swift_test_key[1] = config['password2']
            except KeyError:
                pass  # old config, no second account tests can be run
            try:
                swift_test_user[2] = '%s%s' % (
                    '%s:' % config['account'] if 'account'
                    in config else '', config['username3'])
                swift_test_key[2] = config['password3']
            except KeyError:
                pass  # old config, no third account tests can be run

            for _ in range(3):
                swift_test_perm[_] = swift_test_user[_]

        else:
            swift_test_user[0] = config['username']
            swift_test_tenant[0] = config['account']
            swift_test_key[0] = config['password']
            swift_test_user[1] = config['username2']
            swift_test_tenant[1] = config['account2']
            swift_test_key[1] = config['password2']
            swift_test_user[2] = config['username3']
            swift_test_tenant[2] = config['account']
            swift_test_key[2] = config['password3']

            for _ in range(3):
                swift_test_perm[_] = swift_test_tenant[_] + ':' \
                    + swift_test_user[_]

    global skip
    skip = not all([swift_test_auth, swift_test_user[0], swift_test_key[0]])
    if skip:
        print >>sys.stderr, 'SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG'

    global skip2
    skip2 = not all([not skip, swift_test_user[1], swift_test_key[1]])
    if not skip and skip2:
        print >>sys.stderr, \
            'SKIPPING SECOND ACCOUNT FUNCTIONAL TESTS' \
            ' DUE TO NO CONFIG FOR THEM'

    global skip3
    skip3 = not all([not skip, swift_test_user[2], swift_test_key[2]])
    if not skip and skip3:
        print >>sys.stderr, \
            'SKIPPING THIRD ACCOUNT FUNCTIONAL TESTS DUE TO NO CONFIG FOR THEM'


def teardown_package():
    global orig_collate
    locale.setlocale(locale.LC_COLLATE, orig_collate)


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
    You can use the kwargs to override:
      'retries' (default: 5)
      'use_account' (default: 1) - which user's token to pass
      'url_account' (default: matches 'use_account') - which user's storage URL
      'resource' (default: url[url_account] - URL to connect to; retry()
          will interpolate the variable :storage_url: if present
    """
    global url, token, parsed, conn
    retries = kwargs.get('retries', 5)
    attempts, backoff = 0, 1

    # use account #1 by default; turn user's 1-indexed account into 0-indexed
    use_account = kwargs.pop('use_account', 1) - 1

    # access our own account by default
    url_account = kwargs.pop('url_account', use_account + 1) - 1

    while attempts <= retries:
        attempts += 1
        try:
            if not url[use_account] or not token[use_account]:
                url[use_account], token[use_account] = \
                    get_auth(swift_test_auth, swift_test_user[use_account],
                             swift_test_key[use_account],
                             snet=False,
                             tenant_name=swift_test_tenant[use_account],
                             auth_version=swift_test_auth_version,
                             os_options={})
                parsed[use_account] = conn[use_account] = None
            if not parsed[use_account] or not conn[use_account]:
                parsed[use_account], conn[use_account] = \
                    http_connection(url[use_account])

            # default resource is the account url[url_account]
            resource = kwargs.pop('resource', '%(storage_url)s')
            template_vars = {'storage_url': url[url_account]}
            parsed_result = urlparse(resource % template_vars)
            return func(url[url_account], token[use_account],
                        parsed_result, conn[url_account],
                        *args, **kwargs)
        except (socket.error, HTTPException):
            if attempts > retries:
                raise
            parsed[use_account] = conn[use_account] = None
        except AuthError:
            url[use_account] = token[use_account] = None
            continue
        except InternalServerError:
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


def load_constraint(name):
    global config
    c = config[name]
    if not isinstance(c, int):
        raise SkipTest(c)
    return c


cluster_info = {}


def get_cluster_info():
    conn = Connection(config)
    conn.authenticate()
    global cluster_info
    cluster_info = conn.cluster_info()


def reset_acl():
    def post(url, token, parsed, conn):
        conn.request('POST', parsed.path, '', {
            'X-Auth-Token': token,
            'X-Account-Access-Control': '{}'
        })
        return check_response(conn)
    resp = retry(post, use_account=1)
    resp.read()


def requires_acls(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if skip:
            raise SkipTest
        if not cluster_info:
            get_cluster_info()
        # Determine whether this cluster has account ACLs; if not, skip test
        if not cluster_info.get('tempauth', {}).get('account_acls'):
            raise SkipTest
        if 'keystoneauth' in cluster_info:
            # remove when keystoneauth supports account acls
            raise SkipTest
        reset_acl()
        try:
            rv = f(*args, **kwargs)
        finally:
            reset_acl()
        return rv
    return wrapper
