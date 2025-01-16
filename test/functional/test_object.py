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

import datetime
import json
import unittest
from uuid import uuid4
import time
from unittest import SkipTest
from xml.dom import minidom

from swift.common.header_key_dict import HeaderKeyDict
from test.functional import check_response, retry, requires_acls, \
    requires_policies, requires_bulk
import test.functional as tf
from swift.common.utils import md5, config_true_value


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestObject(unittest.TestCase):

    def setUp(self):
        if tf.skip or tf.skip2:
            raise SkipTest

        if tf.in_process:
            tf.skip_if_no_xattrs()
        self.container = uuid4().hex

        self.containers = []
        self._create_container(self.container)
        self._create_container(self.container, use_account=2)

        self.obj = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def _create_container(self, name=None, headers=None, use_account=1):
        if not name:
            name = uuid4().hex
        self.containers.append(name)
        headers = headers or {}

        def put(url, token, parsed, conn, name):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('PUT', parsed.path + '/' + name, '',
                         new_headers)
            return check_response(conn)
        resp = retry(put, name, use_account=use_account)
        resp.read()
        self.assertIn(resp.status, (201, 202))

        # With keystoneauth we need the accounts to have had the project
        # domain id persisted as sysmeta prior to testing ACLs. This may
        # not be the case if, for example, the account was created using
        # a request with reseller_admin role, when project domain id may
        # not have been known. So we ensure that the project domain id is
        # in sysmeta by making a POST to the accounts using an admin role.
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(post, use_account=use_account)
        resp.read()
        self.assertEqual(resp.status, 204)

        return name

    def tearDown(self):
        if tf.skip:
            raise SkipTest

        # get list of objects in container
        def get(url, token, parsed, conn, container):
            conn.request(
                'GET', parsed.path + '/' + container + '?format=json', '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # delete an object
        def delete(url, token, parsed, conn, container, obj):
            path = '/'.join([parsed.path, container, obj['name']])
            conn.request('DELETE', path, '', {'X-Auth-Token': token})
            return check_response(conn)

        for container in self.containers:
            while True:
                resp = retry(get, container)
                body = resp.read()
                if resp.status == 404:
                    break
                self.assertEqual(resp.status // 100, 2, resp.status)
                objs = json.loads(body)
                if not objs:
                    break
                for obj in objs:
                    resp = retry(delete, container, obj)
                    resp.read()
                    self.assertIn(resp.status, (204, 404))

        # delete the container
        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', parsed.path + '/' + name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        for container in self.containers:
            resp = retry(delete, container)
            resp.read()
            self.assertIn(resp.status, (204, 404))

    def test_metadata(self):
        obj = 'test_metadata'
        req_metadata = {}

        def put(url, token, parsed, conn):
            headers = {'X-Auth-Token': token}
            headers.update(req_metadata)
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, obj
            ), '', headers)
            return check_response(conn)

        def get(url, token, parsed, conn):
            conn.request(
                'GET',
                '%s/%s/%s' % (parsed.path, self.container, obj),
                '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn):
            headers = {'X-Auth-Token': token}
            headers.update(req_metadata)
            conn.request('POST', '%s/%s/%s' % (
                parsed.path, self.container, obj
            ), '', headers)
            return check_response(conn)

        def metadata(resp):
            metadata = {}
            for k, v in resp.headers.items():
                if 'meta' in k.lower():
                    metadata[k] = v
            return metadata

        # empty put
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {})
        # empty post
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 202)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {})

        # metadata put
        req_metadata = {
            'x-object-meta-Color': 'blUe',
            'X-Object-Meta-food': 'PizZa',
        }
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {
            'X-Object-Meta-Color': 'blUe',
            'X-Object-Meta-Food': 'PizZa',
        })
        # metadata post
        req_metadata = {'X-Object-Meta-color': 'oraNge'}
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 202)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {
            'X-Object-Meta-Color': 'oraNge'
        })

        # sysmeta put
        req_metadata = {
            'X-Object-Meta-Color': 'Red',
            'X-Object-Sysmeta-Color': 'Green',
            'X-Object-Transient-Sysmeta-Color': 'Blue',
        }
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {
            'X-Object-Meta-Color': 'Red',
        })
        # sysmeta post
        req_metadata = {
            'X-Object-Meta-Food': 'Burger',
            'X-Object-Meta-Animal': 'Cat',
            'X-Object-Sysmeta-Animal': 'Cow',
            'X-Object-Transient-Sysmeta-Food': 'Burger',
        }
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 202)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {
            'X-Object-Meta-Food': 'Burger',
            'X-Object-Meta-Animal': 'Cat',
        })

        # non-ascii put
        req_metadata = {
            'X-Object-Meta-Foo': u'B\u00e2r',
        }
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {
            'X-Object-Meta-Foo': 'B\xc3\xa2r',
        })
        # non-ascii post
        req_metadata = {
            'X-Object-Meta-Foo': u'B\u00e5z',
        }
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 202)
        resp = retry(get)
        self.assertEqual(b'', resp.read())
        self.assertEqual(resp.status, 200)
        self.assertEqual(metadata(resp), {
            'X-Object-Meta-Foo': 'B\xc3\xa5z',
        })

    def test_if_none_match(self):
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, self.container, 'if_none_match_test'), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, 'if_none_match_test'), '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'If-None-Match': '*'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 412)

        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, 'if_none_match_test'), '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'If-None-Match': 'somethingelse'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_too_small_x_timestamp(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                                              'too_small_x_timestamp'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Timestamp': '-1'})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', '%s/%s/%s' % (parsed.path, self.container,
                                               'too_small_x_timestamp'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0'})
            return check_response(conn)
        ts_before = time.time()
        time.sleep(0.05)
        resp = retry(put)
        body = resp.read()
        time.sleep(0.05)
        ts_after = time.time()
        if resp.status == 400:
            # shunt_inbound_x_timestamp must be false
            self.assertIn(
                'X-Timestamp should be a UNIX timestamp float value', body)
        else:
            self.assertEqual(resp.status, 201)
            self.assertEqual(body, b'')
            resp = retry(head)
            resp.read()
            self.assertGreater(float(resp.headers['x-timestamp']), ts_before)
            self.assertLess(float(resp.headers['x-timestamp']), ts_after)

    def test_too_big_x_timestamp(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                                              'too_big_x_timestamp'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Timestamp': '99999999999.9999999999'})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', '%s/%s/%s' % (parsed.path, self.container,
                                               'too_big_x_timestamp'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0'})
            return check_response(conn)
        ts_before = time.time()
        time.sleep(0.05)
        resp = retry(put)
        body = resp.read()
        time.sleep(0.05)
        ts_after = time.time()
        if resp.status == 400:
            # shunt_inbound_x_timestamp must be false
            self.assertIn(
                'X-Timestamp should be a UNIX timestamp float value', body)
        else:
            self.assertEqual(resp.status, 201)
            self.assertEqual(body, b'')
            resp = retry(head)
            resp.read()
            self.assertGreater(float(resp.headers['x-timestamp']), ts_before)
            self.assertLess(float(resp.headers['x-timestamp']), ts_after)

    def test_x_delete_after(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                                              'x_delete_after'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Delete-After': '2'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def get(url, token, parsed, conn):
            conn.request(
                'GET',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_after'),
                '',
                {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get)
        resp.read()
        count = 0
        while resp.status == 200 and count < 10:
            resp = retry(get)
            resp.read()
            count += 1
            time.sleep(0.5)

        self.assertEqual(resp.status, 404)

        # To avoid an error when the object deletion in tearDown(),
        # the object is added again.
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def test_x_delete_at(self):
        def put(url, token, parsed, conn):
            dt = datetime.datetime.now()
            epoch = time.mktime(dt.timetuple())
            delete_time = str(int(epoch) + 3)
            conn.request(
                'PUT',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'X-Delete-At': delete_time})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def get(url, token, parsed, conn):
            conn.request(
                'GET',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get)
        resp.read()
        count = 0
        while resp.status == 200 and count < 10:
            resp = retry(get)
            resp.read()
            count += 1
            time.sleep(1)

        self.assertEqual(resp.status, 404)

        # To avoid an error when the object deletion in tearDown(),
        # the object is added again.
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def test_open_expired_enabled(self):
        allow_open_expired = config_true_value(tf.cluster_info['swift'].get(
            'allow_open_expired', 'false'))

        if not allow_open_expired:
            raise SkipTest('allow_open_expired is disabled')

        def put(url, token, parsed, conn):
            dt = datetime.datetime.now()
            epoch = time.mktime(dt.timetuple())
            delete_time = str(int(epoch) + 2)
            conn.request(
                'PUT',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'X-Delete-At': delete_time})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def get(url, token, parsed, conn, extra_headers=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'GET',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                headers)
            return check_response(conn)

        def head(url, token, parsed, conn, extra_headers=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'HEAD',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                headers)
            return check_response(conn)

        def post(url, token, parsed, conn, extra_headers=None):
            dt = datetime.datetime.now()
            epoch = time.mktime(dt.timetuple())
            delete_time = str(int(epoch) + 2)
            headers = {'X-Auth-Token': token,
                       'Content-Length': '0',
                       'X-Delete-At': delete_time
                       }
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'POST',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                headers)
            return check_response(conn)

        resp = retry(get)
        resp.read()
        count = 0
        while resp.status == 200 and count < 10:
            resp = retry(get)
            resp.read()
            count += 1
            time.sleep(1)

        # check to see object has expired
        self.assertEqual(resp.status, 404)

        dt = datetime.datetime.now()
        now = str(int(time.mktime(dt.timetuple())))
        resp = retry(get, extra_headers={'X-Open-Expired': True})
        resp.read()
        headers = HeaderKeyDict(resp.getheaders())
        # read the expired object with magic x-open-expired header
        self.assertEqual(resp.status, 200)
        self.assertTrue(now > headers['X-Delete-At'])

        resp = retry(head, extra_headers={'X-Open-Expired': True})
        resp.read()
        # head expired object with magic x-open-expired header
        self.assertEqual(resp.status, 200)

        resp = retry(get)
        resp.read()
        # verify object is still expired
        self.assertEqual(resp.status, 404)

        # verify object is still expired if x-open-expire is False
        resp = retry(get, extra_headers={'X-Open-Expired': False})
        resp.read()
        self.assertEqual(resp.status, 404)

        resp = retry(get, extra_headers={'X-Open-Expired': True})
        resp.read()
        self.assertEqual(resp.status, 200)
        headers = HeaderKeyDict(resp.getheaders())
        self.assertTrue(now > headers['X-Delete-At'])

        resp = retry(head, extra_headers={'X-Open-Expired': False})
        resp.read()
        self.assertEqual(resp.status, 404)

        resp = retry(head, extra_headers={'X-Open-Expired': True})
        resp.read()
        self.assertEqual(resp.status, 200)
        headers = HeaderKeyDict(resp.getheaders())
        self.assertTrue(now > headers['X-Delete-At'])

        resp = retry(post, extra_headers={'X-Open-Expired': False})
        resp.read()
        # verify object is not updated and remains deleted
        self.assertEqual(resp.status, 404)

        # object got restored with magic x-open-expired header
        resp = retry(post, extra_headers={'X-Open-Expired': True,
                                          'X-Object-Meta-Test': 'restored!'})
        resp.read()
        self.assertEqual(resp.status, 202)

        # verify object could be restored and you can do normal GET
        resp = retry(get)
        resp.read()
        self.assertEqual(resp.status, 200)
        self.assertIn('X-Object-Meta-Test', resp.headers)
        self.assertEqual(resp.headers['x-object-meta-test'], 'restored!')

        # verify object is restored and you can do normal HEAD
        resp = retry(head)
        resp.read()
        self.assertEqual(resp.status, 200)
        # verify object is updated with advanced delete time
        self.assertIn('X-Delete-At', resp.headers)

        # To avoid an error when the object deletion in tearDown(),
        # the object is added again.
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def test_allow_open_expired_disabled(self):
        allow_open_expired = config_true_value(tf.cluster_info['swift'].get(
            'allow_open_expired', 'false'))

        if allow_open_expired:
            raise SkipTest('allow_open_expired is enabled')

        def put(url, token, parsed, conn):
            dt = datetime.datetime.now()
            epoch = time.mktime(dt.timetuple())
            delete_time = str(int(epoch) + 2)
            conn.request(
                'PUT',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'X-Delete-At': delete_time})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def get(url, token, parsed, conn, extra_headers=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'GET',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                headers)
            return check_response(conn)

        def head(url, token, parsed, conn, extra_headers=None):
            headers = {'X-Auth-Token': token}
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'HEAD',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                headers)
            return check_response(conn)

        def post(url, token, parsed, conn, extra_headers=None):
            dt = datetime.datetime.now()
            epoch = time.mktime(dt.timetuple())
            delete_time = str(int(epoch) + 2)
            headers = {'X-Auth-Token': token,
                       'Content-Length': '0',
                       'X-Delete-At': delete_time
                       }
            if extra_headers:
                headers.update(extra_headers)
            conn.request(
                'POST',
                '%s/%s/%s' % (parsed.path, self.container, 'x_delete_at'),
                '',
                headers)
            return check_response(conn)

        resp = retry(get)
        resp.read()
        count = 0
        while resp.status == 200 and count < 10:
            resp = retry(get)
            resp.read()
            count += 1
            time.sleep(1)

        # check to see object has expired
        self.assertEqual(resp.status, 404)

        resp = retry(get, extra_headers={'X-Open-Expired': True})
        resp.read()
        # read the expired object with magic x-open-expired header
        self.assertEqual(resp.status, 404)

        resp = retry(head, extra_headers={'X-Open-Expired': True})
        resp.read()
        # head expired object with magic x-open-expired header
        self.assertEqual(resp.status, 404)

        resp = retry(get)
        resp.read()
        # verify object is still expired
        self.assertEqual(resp.status, 404)

        # verify object is still expired if x-open-expire is False
        resp = retry(get, extra_headers={'X-Open-Expired': False})
        resp.read()
        self.assertEqual(resp.status, 404)

        resp = retry(get, extra_headers={'X-Open-Expired': True})
        resp.read()
        self.assertEqual(resp.status, 404)

        resp = retry(head, extra_headers={'X-Open-Expired': False})
        resp.read()
        self.assertEqual(resp.status, 404)

        resp = retry(head, extra_headers={'X-Open-Expired': True})
        resp.read()
        self.assertEqual(resp.status, 404)

        resp = retry(post, extra_headers={'X-Open-Expired': False})
        resp.read()
        # verify object is not updated and remains deleted
        self.assertEqual(resp.status, 404)

        # object cannot be restored with magic x-open-expired header
        resp = retry(post, extra_headers={'X-Open-Expired': True,
                                          'X-Object-Meta-Test': 'restored!'})
        resp.read()
        self.assertEqual(resp.status, 404)

        # To avoid an error when the object deletion in tearDown(),
        # the object is added again.
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def test_non_integer_x_delete_after(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                                              'non_integer_x_delete_after'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Delete-After': '*'})
            return check_response(conn)
        resp = retry(put)
        body = resp.read()
        self.assertEqual(resp.status, 400)
        self.assertEqual(body, b'Non-integer X-Delete-After')

    def test_non_integer_x_delete_at(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                                              'non_integer_x_delete_at'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Delete-At': '*'})
            return check_response(conn)
        resp = retry(put)
        body = resp.read()
        self.assertEqual(resp.status, 400)
        self.assertEqual(body, b'Non-integer X-Delete-At')

    def test_x_delete_at_in_the_past(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                                              'x_delete_at_in_the_past'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Delete-At': '0'})
            return check_response(conn)
        resp = retry(put)
        body = resp.read()
        self.assertEqual(resp.status, 400)
        self.assertEqual(body, b'X-Delete-At in past')

    def test_x_delete_at_in_the_far_future(self):
        def put(url, token, parsed, conn):
            path = '%s/%s/%s' % (parsed.path, self.container,
                                 'x_delete_at_in_the_far_future')
            conn.request('PUT', path, '', {
                'X-Auth-Token': token,
                'Content-Length': '0',
                'X-Delete-At': '1' * 100})
            return check_response(conn)
        resp = retry(put)
        body = resp.read()
        self.assertEqual(resp.status, 201, 'Got %s: %s' % (resp.status, body))

        def head(url, token, parsed, conn):
            path = '%s/%s/%s' % (parsed.path, self.container,
                                 'x_delete_at_in_the_far_future')
            conn.request('HEAD', path, '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(head)
        body = resp.read()
        self.assertEqual(resp.status, 200, 'Got %s: %s' % (resp.status, body))
        self.assertEqual(resp.headers['x-delete-at'], '9' * 10)

    def test_copy_object(self):
        if tf.skip:
            raise SkipTest

        source = '%s/%s' % (self.container, self.obj)
        dest = '%s/%s' % (self.container, 'test_copy')

        # get contents of source
        def get_source(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, source),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_source)
        source_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(source_contents, b'test')

        # copy source to dest with X-Copy-From
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token,
                          'Content-Length': '0',
                          'X-Copy-From': source})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        def get_dest(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, dest),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents)

        # delete the copy
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        # verify dest does not exist
        resp = retry(get_dest)
        resp.read()
        self.assertEqual(resp.status, 404)

        # copy source to dest with COPY
        def copy(url, token, parsed, conn):
            conn.request('COPY', '%s/%s' % (parsed.path, source), '',
                         {'X-Auth-Token': token,
                          'Destination': dest})
            return check_response(conn)
        resp = retry(copy)
        resp.read()
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents)

        # copy source to dest with COPY and range
        def copy(url, token, parsed, conn):
            conn.request('COPY', '%s/%s' % (parsed.path, source), '',
                         {'X-Auth-Token': token,
                          'Destination': dest,
                          'Range': 'bytes=1-2'})
            return check_response(conn)
        resp = retry(copy)
        resp.read()
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents[1:3])

        # delete the copy
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    def test_copy_between_accounts(self):
        if tf.skip2:
            raise SkipTest

        source = '%s/%s' % (self.container, self.obj)
        dest = '%s/%s' % (self.container, 'test_copy')

        # get contents of source
        def get_source(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, source),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_source)
        source_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(source_contents, b'test')

        acct = tf.parsed[0].path.split('/', 2)[2]

        # copy source to dest with X-Copy-From-Account
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token,
                          'Content-Length': '0',
                          'X-Copy-From-Account': acct,
                          'X-Copy-From': source})
            return check_response(conn)
        # try to put, will not succeed
        # user does not have permissions to read from source
        resp = retry(put, use_account=2)
        self.assertEqual(resp.status, 403)

        # add acl to allow reading from source
        def post(url, token, parsed, conn):
            conn.request('POST', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': tf.swift_test_perm[1]})
            return check_response(conn)
        resp = retry(post)
        self.assertEqual(resp.status, 204)

        # retry previous put, now should succeed
        resp = retry(put, use_account=2)
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        def get_dest(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, dest),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_dest, use_account=2)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents)

        # delete the copy
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete, use_account=2)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        # verify dest does not exist
        resp = retry(get_dest, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 404)

        acct_dest = tf.parsed[1].path.split('/', 2)[2]

        # copy source to dest with COPY
        def copy(url, token, parsed, conn):
            conn.request('COPY', '%s/%s' % (parsed.path, source), '',
                         {'X-Auth-Token': token,
                          'Destination-Account': acct_dest,
                          'Destination': dest})
            return check_response(conn)
        # try to copy, will not succeed
        # user does not have permissions to write to destination
        resp = retry(copy)
        resp.read()
        self.assertEqual(resp.status, 403)

        # add acl to allow write to destination
        def post(url, token, parsed, conn):
            conn.request('POST', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token,
                          'X-Container-Write': tf.swift_test_perm[0]})
            return check_response(conn)
        resp = retry(post, use_account=2)
        self.assertEqual(resp.status, 204)

        # now copy will succeed
        resp = retry(copy)
        resp.read()
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        resp = retry(get_dest, use_account=2)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents)

        # delete the copy
        resp = retry(delete, use_account=2)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    def test_public_object(self):
        if tf.skip:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s/%s' % (parsed.path, self.container, self.obj))
            return check_response(conn)
        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assertTrue(str(err).startswith('No result after '))

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': '.r:*'})
            return check_response(conn)
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get)
        resp.read()
        self.assertEqual(resp.status, 200)

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token, 'X-Container-Read': ''})
            return check_response(conn)
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assertTrue(str(err).startswith('No result after '))

    def test_private_object(self):
        if tf.skip or tf.skip3:
            raise SkipTest

        # Ensure we can't access the object with the third account
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # create a shared container writable by account3
        shared_container = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (
                parsed.path, shared_container), '',
                {'X-Auth-Token': token,
                 'X-Container-Read': tf.swift_test_perm[2],
                 'X-Container-Write': tf.swift_test_perm[2]})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account can not copy from private container
        def copy(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, shared_container, 'private_object'), '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'X-Copy-From': '%s/%s' % (self.container, self.obj)})
            return check_response(conn)
        resp = retry(copy, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # verify third account can write "obj1" to shared container
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account can copy "obj1" to shared container
        def copy2(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token,
                 'Destination': '%s/%s' % (shared_container, 'obj1')})
            return check_response(conn)
        resp = retry(copy2, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account STILL can not copy from private container
        def copy3(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), '',
                {'X-Auth-Token': token,
                 'Destination': '%s/%s' % (shared_container,
                                           'private_object')})
            return check_response(conn)
        resp = retry(copy3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # clean up "obj1"
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))

        # clean up shared_container
        def delete(url, token, parsed, conn):
            conn.request('DELETE',
                         parsed.path + '/' + shared_container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    def test_container_write_only(self):
        if tf.skip or tf.skip3:
            raise SkipTest

        # Ensure we can't access the object with the third account
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # create a shared container writable (but not readable) by account3
        shared_container = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (
                parsed.path, shared_container), '',
                {'X-Auth-Token': token,
                 'X-Container-Write': tf.swift_test_perm[2]})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account can write "obj1" to shared container
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account cannot copy "obj1" to shared container
        def copy(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token,
                 'Destination': '%s/%s' % (shared_container, 'obj2')})
            return check_response(conn)
        resp = retry(copy, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # verify third account can POST to "obj1" in shared container
        def post(url, token, parsed, conn):
            conn.request('POST', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token,
                 'X-Object-Meta-Color': 'blue'})
            return check_response(conn)
        resp = retry(post, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 202)

        # verify third account can DELETE from shared container
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete, use_account=3)
        resp.read()
        self.assertIn(resp.status, (204, 404))

        # clean up shared_container
        def delete(url, token, parsed, conn):
            conn.request('DELETE',
                         parsed.path + '/' + shared_container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    @requires_acls
    def test_read_only(self):
        if tf.skip3:
            raise SkipTest

        def get_listing(url, token, parsed, conn):
            conn.request('GET', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list objects
        resp = retry(get_listing, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # cannot get object
        resp = retry(get, self.obj, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-only access
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list objects
        resp = retry(get_listing, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(self.obj, listing.split('\n'))

        # can get object
        resp = retry(get, self.obj, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(body, b'test')

        # can not put an object
        obj_name = str(uuid4())
        resp = retry(put, obj_name, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 403)

        # can not delete an object
        resp = retry(delete, self.obj, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 403)

        # sanity with account1
        resp = retry(get_listing, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertNotIn(obj_name, listing.split('\n'))
        self.assertIn(self.obj, listing.split('\n'))

    @requires_acls
    def test_read_write(self):
        if tf.skip3:
            raise SkipTest

        def get_listing(url, token, parsed, conn):
            conn.request('GET', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list objects
        resp = retry(get_listing, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # cannot get object
        resp = retry(get, self.obj, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list objects
        resp = retry(get_listing, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(self.obj, listing.split('\n'))

        # can get object
        resp = retry(get, self.obj, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(body, b'test')

        # can put an object
        obj_name = str(uuid4())
        resp = retry(put, obj_name, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 201)

        # can delete an object
        resp = retry(delete, self.obj, use_account=3)
        body = resp.read()
        self.assertIn(resp.status, (204, 404))

        # sanity with account1
        resp = retry(get_listing, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(obj_name, listing.split('\n'))
        self.assertNotIn(self.obj, listing.split('\n'))

    @requires_acls
    def test_admin(self):
        if tf.skip3:
            raise SkipTest

        def get_listing(url, token, parsed, conn):
            conn.request('GET', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list objects
        resp = retry(get_listing, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # cannot get object
        resp = retry(get, self.obj, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant admin access
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list objects
        resp = retry(get_listing, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(self.obj, listing.split('\n'))

        # can get object
        resp = retry(get, self.obj, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(body, b'test')

        # can put an object
        obj_name = str(uuid4())
        resp = retry(put, obj_name, use_account=3)
        body = resp.read()
        self.assertEqual(resp.status, 201)

        # can delete an object
        resp = retry(delete, self.obj, use_account=3)
        body = resp.read()
        self.assertIn(resp.status, (204, 404))

        # sanity with account1
        resp = retry(get_listing, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(obj_name, listing.split('\n'))
        self.assertNotIn(self.obj, listing)

    def test_manifest(self):
        if tf.skip:
            raise SkipTest
        # Data for the object segments
        segments1 = [b'one', b'two', b'three', b'four', b'five']
        segments2 = [b'six', b'seven', b'eight']
        segments3 = [b'nine', b'ten', b'eleven']

        # Upload the first set of segments
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments1/%s' % (
                parsed.path, self.container, str(objnum)), segments1[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in range(len(segments1)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEqual(resp.status, 201)

        # Upload the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token,
                    'X-Object-Manifest': '%s/segments1/' % self.container,
                    'Content-Type': 'text/jibberish', 'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Get the manifest (should get all the segments as the body)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments1))
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('content-type'), 'text/jibberish')

        # Get with a range at the start of the second segment
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token, 'Range': 'bytes=3-'})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments1[1:]))
        self.assertEqual(resp.status, 206)

        # Get with a range in the middle of the second segment
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token, 'Range': 'bytes=5-'})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments1)[5:])
        self.assertEqual(resp.status, 206)

        # Get with a full start and stop range
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token, 'Range': 'bytes=5-10'})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments1)[5:11])
        self.assertEqual(resp.status, 206)

        # Upload the second set of segments
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments2/%s' % (
                parsed.path, self.container, str(objnum)), segments2[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in range(len(segments2)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEqual(resp.status, 201)

        # Get the manifest (should still be the first segments of course)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments1))
        self.assertEqual(resp.status, 200)

        # Update the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token,
                    'X-Object-Manifest': '%s/segments2/' % self.container,
                    'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Get the manifest (should be the second set of segments now)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments2))
        self.assertEqual(resp.status, 200)

        if not tf.skip3:

            # Ensure we can't access the manifest with the third account
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            resp.read()
            self.assertEqual(resp.status, 403)

            # Grant access to the third account
            def post(url, token, parsed, conn):
                conn.request('POST', '%s/%s' % (parsed.path, self.container),
                             '', {'X-Auth-Token': token,
                                  'X-Container-Read': tf.swift_test_perm[2]})
                return check_response(conn)
            resp = retry(post)
            resp.read()
            self.assertEqual(resp.status, 204)

            # The third account should be able to get the manifest now
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            self.assertEqual(resp.read(), b''.join(segments2))
            self.assertEqual(resp.status, 200)

        # Create another container for the third set of segments
        acontainer = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + acontainer, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Upload the third set of segments in the other container
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments3/%s' % (
                parsed.path, acontainer, str(objnum)), segments3[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in range(len(segments3)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEqual(resp.status, 201)

        # Update the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (
                parsed.path, self.container), '',
                {'X-Auth-Token': token,
                 'X-Object-Manifest': '%s/segments3/' % acontainer,
                 'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Get the manifest to ensure it's the third set of segments
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), b''.join(segments3))
        self.assertEqual(resp.status, 200)

        if not tf.skip3:

            # Ensure we can't access the manifest with the third account
            # (because the segments are in a protected container even if the
            # manifest itself is not).

            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            resp.read()
            self.assertEqual(resp.status, 403)

            # Grant access to the third account
            def post(url, token, parsed, conn):
                conn.request('POST', '%s/%s' % (parsed.path, acontainer),
                             '', {'X-Auth-Token': token,
                                  'X-Container-Read': tf.swift_test_perm[2]})
                return check_response(conn)
            resp = retry(post)
            resp.read()
            self.assertEqual(resp.status, 204)

            # The third account should be able to get the manifest now
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            self.assertEqual(resp.read(), b''.join(segments3))
            self.assertEqual(resp.status, 200)

        # Delete the manifest
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/manifest' % (
                parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete, objnum)
        resp.read()
        self.assertIn(resp.status, (204, 404))

        # Delete the third set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments3/%s' % (
                parsed.path, acontainer, str(objnum)), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in range(len(segments3)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertIn(resp.status, (204, 404))

        # Delete the second set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments2/%s' % (
                parsed.path, self.container, str(objnum)), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in range(len(segments2)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertIn(resp.status, (204, 404))

        # Delete the first set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments1/%s' % (
                parsed.path, self.container, str(objnum)), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in range(len(segments1)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertIn(resp.status, (204, 404))

        # Delete the extra container
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, acontainer), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    def test_delete_content_type(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/hi' % (parsed.path, self.container),
                         'there', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/hi' % (parsed.path, self.container),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/html; charset=UTF-8')

    def test_delete_if_delete_at_bad(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT',
                         '%s/%s/hi-delete-bad' % (parsed.path, self.container),
                         'there', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/hi' % (parsed.path, self.container),
                         '', {'X-Auth-Token': token,
                              'X-If-Delete-At': 'bad'})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_null_name(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/abc%%00def' % (
                parsed.path,
                self.container), 'test', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        if (tf.web_front_end == 'apache2'):
            self.assertEqual(resp.status, 404)
        else:
            self.assertEqual(resp.read(), b'Invalid UTF8 or contains NULL')
            self.assertEqual(resp.status, 412)

    def test_cors(self):
        if tf.skip:
            raise SkipTest

        try:
            strict_cors = tf.cluster_info['swift']['strict_cors_mode']
        except KeyError:
            raise SkipTest("cors mode is unknown")

        def put_cors_cont(url, token, parsed, conn, orig):
            conn.request(
                'PUT', '%s/%s' % (parsed.path, self.container),
                '', {'X-Auth-Token': token,
                     'X-Container-Meta-Access-Control-Allow-Origin': orig})
            return check_response(conn)

        def put_obj(url, token, parsed, conn, obj):
            conn.request(
                'PUT', '%s/%s/%s' % (parsed.path, self.container, obj),
                'test', {'X-Auth-Token': token, 'X-Object-Meta-Color': 'red'})
            return check_response(conn)

        def check_cors(url, token, parsed, conn,
                       method, obj, headers):
            if method != 'OPTIONS':
                headers['X-Auth-Token'] = token
            conn.request(
                method, '%s/%s/%s' % (parsed.path, self.container, obj),
                '', headers)
            return conn.getresponse()

        resp = retry(put_cors_cont, '*')
        resp.read()
        self.assertEqual(resp.status // 100, 2)

        resp = retry(put_obj, 'cat')
        resp.read()
        self.assertEqual(resp.status // 100, 2)

        resp = retry(check_cors,
                     'OPTIONS', 'cat', {'Origin': 'http://m.com'})
        self.assertEqual(resp.status, 401)

        resp = retry(check_cors,
                     'OPTIONS', 'cat',
                     {'Origin': 'http://m.com',
                      'Access-Control-Request-Method': 'GET'})

        self.assertEqual(resp.status, 200)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('access-control-allow-origin'),
                         '*')
        # Just a pre-flight; this doesn't show up yet
        self.assertNotIn('access-control-expose-headers', headers)

        resp = retry(check_cors,
                     'GET', 'cat', {'Origin': 'http://m.com'})
        self.assertEqual(resp.status, 200)
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('access-control-allow-origin'),
                         '*')
        self.assertIn('x-object-meta-color', headers.get(
            'access-control-expose-headers').split(', '))

        resp = retry(check_cors,
                     'GET', 'cat', {'Origin': 'http://m.com',
                                    'X-Web-Mode': 'True'})
        self.assertEqual(resp.status, 200)
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('access-control-allow-origin'),
                         '*')
        self.assertIn('x-object-meta-color', headers.get(
            'access-control-expose-headers').split(', '))

        ####################

        resp = retry(put_cors_cont, 'http://secret.com')
        resp.read()
        self.assertEqual(resp.status // 100, 2)

        resp = retry(check_cors,
                     'OPTIONS', 'cat',
                     {'Origin': 'http://m.com',
                      'Access-Control-Request-Method': 'GET'})
        resp.read()
        self.assertEqual(resp.status, 401)

        if strict_cors:
            resp = retry(check_cors,
                         'GET', 'cat', {'Origin': 'http://m.com'})
            resp.read()
            self.assertEqual(resp.status, 200)
            headers = dict((k.lower(), v) for k, v in resp.getheaders())
            self.assertNotIn('access-control-allow-origin', headers)

            resp = retry(check_cors,
                         'GET', 'cat', {'Origin': 'http://secret.com'})
            resp.read()
            self.assertEqual(resp.status, 200)
            headers = dict((k.lower(), v) for k, v in resp.getheaders())
            self.assertEqual(headers.get('access-control-allow-origin'),
                             'http://secret.com')
        else:
            resp = retry(check_cors,
                         'GET', 'cat', {'Origin': 'http://m.com'})
            resp.read()
            self.assertEqual(resp.status, 200)
            headers = dict((k.lower(), v) for k, v in resp.getheaders())
            self.assertEqual(headers.get('access-control-allow-origin'),
                             'http://m.com')

    @requires_policies
    def test_cross_policy_copy(self):
        # create container in first policy
        policy = self.policies.select()
        container = self._create_container(
            headers={'X-Storage-Policy': policy['name']})
        obj = uuid4().hex

        # create a container in second policy
        other_policy = self.policies.exclude(name=policy['name']).select()
        other_container = self._create_container(
            headers={'X-Storage-Policy': other_policy['name']})
        other_obj = uuid4().hex

        def put_obj(url, token, parsed, conn, container, obj):
            # to keep track of things, use the original path as the body
            content = '%s/%s' % (container, obj)
            path = '%s/%s' % (parsed.path, content)
            conn.request('PUT', path, content, {'X-Auth-Token': token})
            return check_response(conn)

        # create objects
        for c, o in zip((container, other_container), (obj, other_obj)):
            resp = retry(put_obj, c, o)
            resp.read()
            self.assertEqual(resp.status, 201)

        def put_copy_from(url, token, parsed, conn, container, obj, source):
            dest_path = '%s/%s/%s' % (parsed.path, container, obj)
            conn.request('PUT', dest_path, '',
                         {'X-Auth-Token': token,
                          'Content-Length': '0',
                          'X-Copy-From': source})
            return check_response(conn)

        copy_requests = (
            (container, other_obj, '%s/%s' % (other_container, other_obj)),
            (other_container, obj, '%s/%s' % (container, obj)),
        )

        # copy objects
        for c, o, source in copy_requests:
            resp = retry(put_copy_from, c, o, source)
            resp.read()
            self.assertEqual(resp.status, 201)

        def get_obj(url, token, parsed, conn, container, obj):
            path = '%s/%s/%s' % (parsed.path, container, obj)
            conn.request('GET', path, '', {'X-Auth-Token': token})
            return check_response(conn)

        # validate contents, contents should be source
        validate_requests = copy_requests
        for c, o, body in validate_requests:
            resp = retry(get_obj, c, o)
            self.assertEqual(resp.status, 200)
            self.assertEqual(body.encode('utf8'), resp.read())

    @requires_bulk
    def test_bulk_delete(self):

        def bulk_delete(url, token, parsed, conn):
            # try to bulk delete the object that was created during test setup
            conn.request('DELETE', '%s/%s/%s?bulk-delete' % (
                parsed.path, self.container, self.obj),
                '%s/%s' % (self.container, self.obj),
                {'X-Auth-Token': token,
                 'Accept': 'application/xml',
                 'Expect': '100-continue',
                 'Content-Type': 'text/plain'})
            return check_response(conn)
        resp = retry(bulk_delete)
        self.assertEqual(resp.status, 200)
        body = resp.read()
        tree = minidom.parseString(body)
        self.assertEqual(tree.documentElement.tagName, 'delete')

        errors = tree.getElementsByTagName('errors')
        self.assertEqual(len(errors), 1)
        errors = [c.data if c.nodeType == c.TEXT_NODE else c.childNodes[0].data
                  for c in errors[0].childNodes
                  if c.nodeType != c.TEXT_NODE or c.data.strip()]
        self.assertEqual(errors, [])

        final_status = tree.getElementsByTagName('response_status')
        self.assertEqual(len(final_status), 1)
        self.assertEqual(len(final_status[0].childNodes), 1)
        self.assertEqual(final_status[0].childNodes[0].data, '200 OK')

    def test_etag_quoter(self):
        if tf.skip:
            raise SkipTest
        if 'etag_quoter' not in tf.cluster_info:
            raise SkipTest("etag-quoter middleware is not enabled")

        def do_head(expect_quoted=None):
            def head(url, token, parsed, conn):
                conn.request('HEAD', '%s/%s/%s' % (
                    parsed.path, self.container, self.obj), '',
                    {'X-Auth-Token': token})
                return check_response(conn)

            resp = retry(head)
            resp.read()
            self.assertEqual(resp.status, 200)

            if expect_quoted is None:
                expect_quoted = tf.cluster_info.get('etag_quoter', {}).get(
                    'enable_by_default', False)

            expected_etag = md5(b'test', usedforsecurity=False).hexdigest()
            if expect_quoted:
                expected_etag = '"%s"' % expected_etag
            self.assertEqual(resp.headers['etag'], expected_etag)

        def _post(enable_flag, container_path):
            def post(url, token, parsed, conn):
                if container_path:
                    path = '%s/%s' % (parsed.path, self.container)
                    hdr = 'X-Container-Rfc-Compliant-Etags'
                else:
                    path = parsed.path
                    hdr = 'X-Account-Rfc-Compliant-Etags'
                headers = {hdr: enable_flag, 'X-Auth-Token': token}
                conn.request('POST', path, '', headers)
                return check_response(conn)

            resp = retry(post)
            resp.read()
            self.assertEqual(resp.status, 204)

        def post_account(enable_flag):
            return _post(enable_flag, False)

        def post_container(enable_flag):
            return _post(enable_flag, True)

        do_head()
        post_container('t')
        do_head(expect_quoted=True)
        try:
            post_account('t')
            post_container('')
            do_head(expect_quoted=True)
            post_container('f')
            do_head(expect_quoted=False)
        finally:
            # Don't leave a dirty account
            post_account('')


if __name__ == '__main__':
    unittest.main()
