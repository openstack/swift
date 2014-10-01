#!/usr/bin/python

# Copyright (c) 2010-2012 OpenStack Foundation
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

import unittest
import json
from uuid import uuid4
from nose import SkipTest
from string import letters

from swift.common.middleware.acl import format_acl

from test.functional import check_response, retry, requires_acls, \
    load_constraint
import test.functional as tf


class TestAccount(unittest.TestCase):

    def setUp(self):
        self.max_meta_count = load_constraint('max_meta_count')
        self.max_meta_name_length = load_constraint('max_meta_name_length')
        self.max_meta_overall_size = load_constraint('max_meta_overall_size')
        self.max_meta_value_length = load_constraint('max_meta_value_length')

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(head)
        self.existing_metadata = set([
            k for k, v in resp.getheaders() if
            k.lower().startswith('x-account-meta')])

    def tearDown(self):
        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(head)
        resp.read()
        new_metadata = set(
            [k for k, v in resp.getheaders() if
             k.lower().startswith('x-account-meta')])

        def clear_meta(url, token, parsed, conn, remove_metadata_keys):
            headers = {'X-Auth-Token': token}
            headers.update((k, '') for k in remove_metadata_keys)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)
        extra_metadata = list(self.existing_metadata ^ new_metadata)
        for i in range(0, len(extra_metadata), 90):
            batch = extra_metadata[i:i + 90]
            resp = retry(clear_meta, batch)
            resp.read()
            self.assertEqual(resp.status // 100, 2)

    def test_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, value):
            conn.request('POST', parsed.path, '',
                         {'X-Auth-Token': token, 'X-Account-Meta-Test': value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(post, '')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-account-meta-test'), None)
        resp = retry(get)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-account-meta-test'), None)
        resp = retry(post, 'Value')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-account-meta-test'), 'Value')
        resp = retry(get)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-account-meta-test'), 'Value')

    def test_invalid_acls(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        # needs to be an acceptable header size
        num_keys = 8
        max_key_size = load_constraint('max_header_size') / num_keys
        acl = {'admin': [c * max_key_size for c in letters[:num_keys]]}
        headers = {'x-account-access-control': format_acl(
            version=2, acl_dict=acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 400)

        # and again a touch smaller
        acl = {'admin': [c * max_key_size for c in letters[:num_keys - 1]]}
        headers = {'x-account-access-control': format_acl(
            version=2, acl_dict=acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

    @requires_acls
    def test_invalid_acl_keys(self):
        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        # needs to be json
        resp = retry(post, headers={'X-Account-Access-Control': 'invalid'},
                     use_account=1)
        resp.read()
        self.assertEqual(resp.status, 400)

        acl_user = tf.swift_test_user[1]
        acl = {'admin': [acl_user], 'invalid_key': 'invalid_value'}
        headers = {'x-account-access-control': format_acl(
            version=2, acl_dict=acl)}

        resp = retry(post, headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

    @requires_acls
    def test_invalid_acl_values(self):
        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        acl = {'admin': 'invalid_value'}
        headers = {'x-account-access-control': format_acl(
            version=2, acl_dict=acl)}

        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

    @requires_acls
    def test_read_only_acl(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        # cannot read account
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant read access
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': format_acl(
            version=2, acl_dict=acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-only can read account headers
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204))
        # but not acls
        self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

        # read-only can not write metadata
        headers = {'x-account-meta-test': 'value'}
        resp = retry(post, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # but they can read it
        headers = {'x-account-meta-test': 'value'}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204))
        self.assertEqual(resp.getheader('X-Account-Meta-Test'), 'value')

    @requires_acls
    def test_read_write_acl(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        # cannot read account
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant read-write access
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': format_acl(
            version=2, acl_dict=acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-write can read account headers
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204))
        # but not acls
        self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

        # read-write can not write account metadata
        headers = {'x-account-meta-test': 'value'}
        resp = retry(post, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

    @requires_acls
    def test_admin_acl(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        # cannot read account
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant admin access
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        acl_json_str = format_acl(version=2, acl_dict=acl)
        headers = {'x-account-access-control': acl_json_str}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # admin can read account headers
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204))
        # including acls
        self.assertEqual(resp.getheader('X-Account-Access-Control'),
                         acl_json_str)

        # admin can write account metadata
        value = str(uuid4())
        headers = {'x-account-meta-test': value}
        resp = retry(post, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204))
        self.assertEqual(resp.getheader('X-Account-Meta-Test'), value)

        # admin can even revoke their own access
        headers = {'x-account-access-control': '{}'}
        resp = retry(post, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)

        # and again, cannot read account
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

    @requires_acls
    def test_protected_tempurl(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        # add an account metadata, and temp-url-key to account
        value = str(uuid4())
        headers = {
            'x-account-meta-temp-url-key': 'secret',
            'x-account-meta-test': value,
        }
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # grant read-only access to tester3
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        acl_json_str = format_acl(version=2, acl_dict=acl)
        headers = {'x-account-access-control': acl_json_str}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-only tester3 can read account metadata
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204),
                     'Expected status in (200, 204), got %s' % resp.status)
        self.assertEqual(resp.getheader('X-Account-Meta-Test'), value)
        # but not temp-url-key
        self.assertEqual(resp.getheader('X-Account-Meta-Temp-Url-Key'), None)

        # grant read-write access to tester3
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        acl_json_str = format_acl(version=2, acl_dict=acl)
        headers = {'x-account-access-control': acl_json_str}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-write tester3 can read account metadata
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204),
                     'Expected status in (200, 204), got %s' % resp.status)
        self.assertEqual(resp.getheader('X-Account-Meta-Test'), value)
        # but not temp-url-key
        self.assertEqual(resp.getheader('X-Account-Meta-Temp-Url-Key'), None)

        # grant admin access to tester3
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        acl_json_str = format_acl(version=2, acl_dict=acl)
        headers = {'x-account-access-control': acl_json_str}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # admin tester3 can read account metadata
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204),
                     'Expected status in (200, 204), got %s' % resp.status)
        self.assertEqual(resp.getheader('X-Account-Meta-Test'), value)
        # including temp-url-key
        self.assertEqual(resp.getheader('X-Account-Meta-Temp-Url-Key'),
                         'secret')

        # admin tester3 can even change temp-url-key
        secret = str(uuid4())
        headers = {
            'x-account-meta-temp-url-key': secret,
        }
        resp = retry(post, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, use_account=3)
        resp.read()
        self.assert_(resp.status in (200, 204),
                     'Expected status in (200, 204), got %s' % resp.status)
        self.assertEqual(resp.getheader('X-Account-Meta-Temp-Url-Key'),
                         secret)

    @requires_acls
    def test_account_acls(self):
        if tf.skip2:
            raise SkipTest

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def put(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('PUT', parsed.path, '', new_headers)
            return check_response(conn)

        def delete(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('DELETE', parsed.path, '', new_headers)
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        try:
            # User1 can POST to their own account (and reset the ACLs)
            resp = retry(post, headers={'X-Account-Access-Control': '{}'},
                         use_account=1)
            resp.read()
            self.assertEqual(resp.status, 204)
            self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

            # User1 can GET their own empty account
            resp = retry(get, use_account=1)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

            # User2 can't GET User1's account
            resp = retry(get, use_account=2, url_account=1)
            resp.read()
            self.assertEqual(resp.status, 403)

            # User1 is swift_owner of their own account, so they can POST an
            # ACL -- let's do this and make User2 (test_user[1]) an admin
            acl_user = tf.swift_test_user[1]
            acl = {'admin': [acl_user]}
            headers = {'x-account-access-control': format_acl(
                version=2, acl_dict=acl)}
            resp = retry(post, headers=headers, use_account=1)
            resp.read()
            self.assertEqual(resp.status, 204)

            # User1 can see the new header
            resp = retry(get, use_account=1)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            data_from_headers = resp.getheader('x-account-access-control')
            expected = json.dumps(acl, separators=(',', ':'))
            self.assertEqual(data_from_headers, expected)

            # Now User2 should be able to GET the account and see the ACL
            resp = retry(head, use_account=2, url_account=1)
            resp.read()
            data_from_headers = resp.getheader('x-account-access-control')
            self.assertEqual(data_from_headers, expected)

            # Revoke User2's admin access, grant User2 read-write access
            acl = {'read-write': [acl_user]}
            headers = {'x-account-access-control': format_acl(
                version=2, acl_dict=acl)}
            resp = retry(post, headers=headers, use_account=1)
            resp.read()
            self.assertEqual(resp.status, 204)

            # User2 can still GET the account, but not see the ACL
            # (since it's privileged data)
            resp = retry(head, use_account=2, url_account=1)
            resp.read()
            self.assertEqual(resp.status, 204)
            self.assertEqual(resp.getheader('x-account-access-control'), None)

            # User2 can PUT and DELETE a container
            resp = retry(put, use_account=2, url_account=1,
                         resource='%(storage_url)s/mycontainer', headers={})
            resp.read()
            self.assertEqual(resp.status, 201)
            resp = retry(delete, use_account=2, url_account=1,
                         resource='%(storage_url)s/mycontainer', headers={})
            resp.read()
            self.assertEqual(resp.status, 204)

            # Revoke User2's read-write access, grant User2 read-only access
            acl = {'read-only': [acl_user]}
            headers = {'x-account-access-control': format_acl(
                version=2, acl_dict=acl)}
            resp = retry(post, headers=headers, use_account=1)
            resp.read()
            self.assertEqual(resp.status, 204)

            # User2 can still GET the account, but not see the ACL
            # (since it's privileged data)
            resp = retry(head, use_account=2, url_account=1)
            resp.read()
            self.assertEqual(resp.status, 204)
            self.assertEqual(resp.getheader('x-account-access-control'), None)

            # User2 can't PUT a container
            resp = retry(put, use_account=2, url_account=1,
                         resource='%(storage_url)s/mycontainer', headers={})
            resp.read()
            self.assertEqual(resp.status, 403)

        finally:
            # Make sure to clean up even if tests fail -- User2 should not
            # have access to User1's account in other functional tests!
            resp = retry(post, headers={'X-Account-Access-Control': '{}'},
                         use_account=1)
            resp.read()

    @requires_acls
    def test_swift_account_acls(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        try:
            # User1 can POST to their own account
            resp = retry(post, headers={'X-Account-Access-Control': '{}'})
            resp.read()
            self.assertEqual(resp.status, 204)
            self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

            # User1 can GET their own empty account
            resp = retry(get)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

            # User1 can POST non-empty data
            acl_json = '{"admin":["bob"]}'
            resp = retry(post, headers={'X-Account-Access-Control': acl_json})
            resp.read()
            self.assertEqual(resp.status, 204)

            # User1 can GET the non-empty data
            resp = retry(get)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            self.assertEqual(resp.getheader('X-Account-Access-Control'),
                             acl_json)

            # POST non-JSON ACL should fail
            resp = retry(post, headers={'X-Account-Access-Control': 'yuck'})
            resp.read()
            # resp.status will be 400 if tempauth or some other ACL-aware
            # auth middleware rejects it, or 200 (but silently swallowed by
            # core Swift) if ACL-unaware auth middleware approves it.

            # A subsequent GET should show the old, valid data, not the garbage
            resp = retry(get)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            self.assertEqual(resp.getheader('X-Account-Access-Control'),
                             acl_json)

        finally:
            # Make sure to clean up even if tests fail -- User2 should not
            # have access to User1's account in other functional tests!
            resp = retry(post, headers={'X-Account-Access-Control': '{}'})
            resp.read()

    def test_swift_prohibits_garbage_account_acls(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        try:
            # User1 can POST to their own account
            resp = retry(post, headers={'X-Account-Access-Control': '{}'})
            resp.read()
            self.assertEqual(resp.status, 204)
            self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

            # User1 can GET their own empty account
            resp = retry(get)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            self.assertEqual(resp.getheader('X-Account-Access-Control'), None)

            # User1 can POST non-empty data
            acl_json = '{"admin":["bob"]}'
            resp = retry(post, headers={'X-Account-Access-Control': acl_json})
            resp.read()
            self.assertEqual(resp.status, 204)
            # If this request is handled by ACL-aware auth middleware, then the
            # ACL will be persisted.  If it is handled by ACL-unaware auth
            # middleware, then the header will be thrown out.  But the request
            # should return successfully in any case.

            # User1 can GET the non-empty data
            resp = retry(get)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            # ACL will be set if some ACL-aware auth middleware (e.g. tempauth)
            # propagates it to sysmeta; if no ACL-aware auth middleware does,
            # then X-Account-Access-Control will still be empty.

            # POST non-JSON ACL should fail
            resp = retry(post, headers={'X-Account-Access-Control': 'yuck'})
            resp.read()
            # resp.status will be 400 if tempauth or some other ACL-aware
            # auth middleware rejects it, or 200 (but silently swallowed by
            # core Swift) if ACL-unaware auth middleware approves it.

            # A subsequent GET should either show the old, valid data (if
            # ACL-aware auth middleware is propagating it) or show nothing
            # (if no auth middleware in the pipeline is ACL-aware), but should
            # never return the garbage ACL.
            resp = retry(get)
            resp.read()
            self.assertEqual(resp.status // 100, 2)
            self.assertNotEqual(resp.getheader('X-Account-Access-Control'),
                                'yuck')

        finally:
            # Make sure to clean up even if tests fail -- User2 should not
            # have access to User1's account in other functional tests!
            resp = retry(post, headers={'X-Account-Access-Control': '{}'})
            resp.read()

    def test_unicode_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        uni_key = u'X-Account-Meta-uni\u0E12'
        uni_value = u'uni\u0E12'
        if (tf.web_front_end == 'integral'):
            resp = retry(post, uni_key, '1')
            resp.read()
            self.assertTrue(resp.status in (201, 204))
            resp = retry(head)
            resp.read()
            self.assert_(resp.status in (200, 204), resp.status)
            self.assertEqual(resp.getheader(uni_key.encode('utf-8')), '1')
        resp = retry(post, 'X-Account-Meta-uni', uni_value)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('X-Account-Meta-uni'),
                         uni_value.encode('utf-8'))
        if (tf.web_front_end == 'integral'):
            resp = retry(post, uni_key, uni_value)
            resp.read()
            self.assertEqual(resp.status, 204)
            resp = retry(head)
            resp.read()
            self.assert_(resp.status in (200, 204), resp.status)
            self.assertEqual(resp.getheader(uni_key.encode('utf-8')),
                             uni_value.encode('utf-8'))

    def test_multi_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(post, 'X-Account-Meta-One', '1')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-account-meta-one'), '1')
        resp = retry(post, 'X-Account-Meta-Two', '2')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-account-meta-one'), '1')
        self.assertEqual(resp.getheader('x-account-meta-two'), '2')

    def test_bad_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)

        resp = retry(post,
                     {'X-Account-Meta-' + (
                         'k' * self.max_meta_name_length): 'v'})
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(
            post,
            {'X-Account-Meta-' + ('k' * (
                self.max_meta_name_length + 1)): 'v'})
        resp.read()
        self.assertEqual(resp.status, 400)

        resp = retry(post,
                     {'X-Account-Meta-Too-Long': (
                         'k' * self.max_meta_value_length)})
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(
            post,
            {'X-Account-Meta-Too-Long': 'k' * (
                self.max_meta_value_length + 1)})
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_bad_metadata2(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)

        # TODO: Find the test that adds these and remove them.
        headers = {'x-remove-account-meta-temp-url-key': 'remove',
                   'x-remove-account-meta-temp-url-key-2': 'remove'}
        resp = retry(post, headers)

        headers = {}
        for x in xrange(self.max_meta_count):
            headers['X-Account-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        headers = {}
        for x in xrange(self.max_meta_count + 1):
            headers['X-Account-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_bad_metadata3(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)

        headers = {}
        header_value = 'k' * self.max_meta_value_length
        size = 0
        x = 0
        while size < (self.max_meta_overall_size - 4
                      - self.max_meta_value_length):
            size += 4 + self.max_meta_value_length
            headers['X-Account-Meta-%04d' % x] = header_value
            x += 1
        if self.max_meta_overall_size - size > 1:
            headers['X-Account-Meta-k'] = \
                'v' * (self.max_meta_overall_size - size - 1)
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        headers['X-Account-Meta-k'] = \
            'v' * (self.max_meta_overall_size - size)
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)


class TestAccountInNonDefaultDomain(unittest.TestCase):
    def setUp(self):
        if tf.skip or tf.skip2 or tf.skip_if_not_v3:
            raise SkipTest('AUTH VERSION 3 SPECIFIC TEST')

    def test_project_domain_id_header(self):
        # make sure account exists (assumes account auto create)
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(post, use_account=4)
        resp.read()
        self.assertEqual(resp.status, 204)

        # account in non-default domain should have a project domain id
        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(head, use_account=4)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertTrue('X-Account-Project-Domain-Id' in resp.headers)


if __name__ == '__main__':
    unittest.main()
