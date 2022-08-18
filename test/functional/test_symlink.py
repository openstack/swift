#!/usr/bin/python
# Copyright (c) 2010-2015 OpenStack Foundation
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

import hmac
import unittest
import itertools
import hashlib
import time

import urllib.parse
from uuid import uuid4

from swift.common.http import is_success
from swift.common.swob import normalize_etag
from swift.common.utils import json, MD5_OF_EMPTY_STRING, md5
from swift.common.middleware.slo import SloGetContext
from test.functional import check_response, retry, requires_acls, \
    cluster_info, SkipTest
from test.functional.tests import Base, TestFileComparisonEnv, Utils, BaseEnv
from test.functional.test_slo import TestSloEnv
from test.functional.test_dlo import TestDloEnv
from test.functional.test_tempurl import TestContainerTempurlEnv, \
    TestTempurlEnv
from test.functional.swift_test_client import ResponseError
import test.functional as tf
from test.unit import group_by_byte

TARGET_BODY = b'target body'


def setUpModule():
    tf.setup_package()
    if 'symlink' not in cluster_info:
        raise SkipTest("Symlinks not enabled")


def tearDownModule():
    tf.teardown_package()


class TestSymlinkEnv(BaseEnv):
    link_cont = uuid4().hex
    tgt_cont = uuid4().hex
    tgt_obj = uuid4().hex

    @classmethod
    def setUp(cls):
        if tf.skip or tf.skip2:
            raise SkipTest

        cls._create_container(cls.tgt_cont)  # use_account=1
        cls._create_container(cls.link_cont)  # use_account=1

        # container in account 2
        cls._create_container(cls.link_cont, use_account=2)
        cls._create_tgt_object()

    @classmethod
    def containers(cls):
        return (cls.link_cont, cls.tgt_cont)

    @classmethod
    def target_content_location(cls, override_obj=None, override_account=None):
        account = override_account or tf.parsed[0].path.split('/', 2)[2]
        return '/v1/%s/%s/%s' % (account, cls.tgt_cont,
                                 override_obj or cls.tgt_obj)

    @classmethod
    def _make_request(cls, url, token, parsed, conn, method,
                      container, obj='', headers=None, body=b'',
                      query_args=None):
        headers = headers or {}
        headers.update({'X-Auth-Token': token})
        path = '%s/%s/%s' % (parsed.path, container, obj) if obj \
               else '%s/%s' % (parsed.path, container)
        if query_args:
            path += '?%s' % query_args
        conn.request(method, path, body, headers)
        resp = check_response(conn)
        # to read the buffer and keep it in the attribute, call resp.content
        resp.content
        return resp

    @classmethod
    def _create_container(cls, name, headers=None, use_account=1):
        headers = headers or {}
        resp = retry(cls._make_request, method='PUT', container=name,
                     headers=headers, use_account=use_account)
        if resp.status not in (201, 202):
            raise ResponseError(resp)
        return name

    @classmethod
    def _create_tgt_object(cls, body=TARGET_BODY):
        resp = retry(cls._make_request, method='PUT',
                     headers={'Content-Type': 'application/target'},
                     container=cls.tgt_cont, obj=cls.tgt_obj,
                     body=body)
        if resp.status != 201:
            raise ResponseError(resp)

        # sanity: successful put response has content-length 0
        cls.tgt_length = str(len(body))
        cls.tgt_etag = resp.getheader('etag')

        resp = retry(cls._make_request, method='GET',
                     container=cls.tgt_cont, obj=cls.tgt_obj)
        if resp.status != 200 and resp.content != body:
            raise ResponseError(resp)

    @classmethod
    def tearDown(cls):
        delete_containers = [
            (use_account, containers) for use_account, containers in
            enumerate([cls.containers(), [cls.link_cont]], 1)]
        # delete objects inside container
        for use_account, containers in delete_containers:
            if use_account == 2 and tf.skip2:
                continue
            for container in containers:
                while True:
                    cont = container
                    resp = retry(cls._make_request, method='GET',
                                 container=cont, query_args='format=json',
                                 use_account=use_account)
                    if resp.status == 404:
                        break
                    if not is_success(resp.status):
                        raise ResponseError(resp)
                    objs = json.loads(resp.content)
                    if not objs:
                        break
                    for obj in objs:
                        resp = retry(cls._make_request, method='DELETE',
                                     container=container, obj=obj['name'],
                                     use_account=use_account)
                        if resp.status not in (204, 404):
                            raise ResponseError(resp)

        # delete the containers
        for use_account, containers in delete_containers:
            if use_account == 2 and tf.skip2:
                continue
            for container in containers:
                resp = retry(cls._make_request, method='DELETE',
                             container=container,
                             use_account=use_account)
                if resp.status not in (204, 404):
                    raise ResponseError(resp)


class TestSymlink(Base):
    env = TestSymlinkEnv

    @classmethod
    def setUpClass(cls):
        # To skip env setup for class setup, instead setUp the env for each
        # test method
        pass

    def setUp(self):
        self.env.setUp()

        def object_name_generator():
            while True:
                yield uuid4().hex

        self.obj_name_gen = object_name_generator()
        self._account_name = None

    def tearDown(self):
        self.env.tearDown()

    @property
    def account_name(self):
        if not self._account_name:
            self._account_name = tf.parsed[0].path.split('/', 2)[2]
        return self._account_name

    def _make_request(self, url, token, parsed, conn, method,
                      container, obj='', headers=None, body=b'',
                      query_args=None, allow_redirects=True):
        headers = headers or {}
        headers.update({'X-Auth-Token': token})
        path = '%s/%s/%s' % (parsed.path, container, obj) if obj \
               else '%s/%s' % (parsed.path, container)
        if query_args:
            path += '?%s' % query_args
        conn.requests_args['allow_redirects'] = allow_redirects
        conn.request(method, path, body, headers)
        resp = check_response(conn)
        # to read the buffer and keep it in the attribute, call resp.content
        resp.content
        return resp

    def _make_request_with_symlink_get(self, url, token, parsed, conn, method,
                                       container, obj, headers=None, body=b''):
        resp = self._make_request(
            url, token, parsed, conn, method, container, obj, headers, body,
            query_args='symlink=get')
        return resp

    def _test_put_symlink(self, link_cont, link_obj, tgt_cont, tgt_obj):
        headers = {'X-Symlink-Target': '%s/%s' % (tgt_cont, tgt_obj)}
        resp = retry(self._make_request, method='PUT',
                     container=link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 201)

    def _test_put_symlink_with_etag(self, link_cont, link_obj, tgt_cont,
                                    tgt_obj, etag, headers=None):
        headers = headers or {}
        headers.update({'X-Symlink-Target': '%s/%s' % (tgt_cont, tgt_obj),
                        'X-Symlink-Target-Etag': etag})
        resp = retry(self._make_request, method='PUT',
                     container=link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 201, resp.content)

    def _test_get_as_target_object(
            self, link_cont, link_obj, expected_content_location,
            use_account=1):
        resp = retry(
            self._make_request, method='GET',
            container=link_cont, obj=link_obj, use_account=use_account)
        self.assertEqual(resp.status, 200, resp.content)
        self.assertEqual(resp.content, TARGET_BODY)
        self.assertEqual(resp.getheader('content-length'),
                         str(self.env.tgt_length))
        self.assertEqual(resp.getheader('etag'), self.env.tgt_etag)
        self.assertIn('Content-Location', resp.headers)
        self.assertEqual(expected_content_location,
                         resp.getheader('content-location'))
        return resp

    def _test_head_as_target_object(self, link_cont, link_obj, use_account=1):
        resp = retry(
            self._make_request, method='HEAD',
            container=link_cont, obj=link_obj, use_account=use_account)
        self.assertEqual(resp.status, 200)

    def _assertLinkObject(self, link_cont, link_obj, use_account=1):
        # HEAD on link_obj itself
        resp = retry(
            self._make_request_with_symlink_get, method='HEAD',
            container=link_cont, obj=link_obj, use_account=use_account)
        self.assertEqual(resp.status, 200)
        self.assertTrue(resp.getheader('x-symlink-target'))

        # GET on link_obj itself
        resp = retry(
            self._make_request_with_symlink_get, method='GET',
            container=link_cont, obj=link_obj, use_account=use_account)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, b'')
        self.assertEqual(resp.getheader('content-length'), str(0))
        self.assertTrue(resp.getheader('x-symlink-target'))

    def _assertSymlink(self, link_cont, link_obj,
                       expected_content_location=None, use_account=1):
        expected_content_location = \
            expected_content_location or self.env.target_content_location()
        # sanity: HEAD/GET on link_obj
        self._assertLinkObject(link_cont, link_obj, use_account)

        # HEAD target object via symlink
        self._test_head_as_target_object(
            link_cont=link_cont, link_obj=link_obj, use_account=use_account)

        # GET target object via symlink
        self._test_get_as_target_object(
            link_cont=link_cont, link_obj=link_obj, use_account=use_account,
            expected_content_location=expected_content_location)

    def test_symlink_with_encoded_target_name(self):
        # makes sure to test encoded characters as symlink target
        target_obj = 'dealde%2Fl04 011e%204c8df/flash.png'
        link_obj = uuid4().hex

        # create target using unnormalized path
        resp = retry(
            self._make_request, method='PUT', container=self.env.tgt_cont,
            obj=target_obj, body=TARGET_BODY)
        self.assertEqual(resp.status, 201)
        # you can get it using either name
        resp = retry(
            self._make_request, method='GET', container=self.env.tgt_cont,
            obj=target_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)
        normalized_quoted_obj = 'dealde/l04%20011e%204c8df/flash.png'
        self.assertEqual(normalized_quoted_obj, urllib.parse.quote(
            urllib.parse.unquote(target_obj)))
        resp = retry(
            self._make_request, method='GET', container=self.env.tgt_cont,
            obj=normalized_quoted_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)

        # create a symlink using the un-normalized target path
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=target_obj)
        # and it's normalized
        self._assertSymlink(
            self.env.link_cont, link_obj,
            expected_content_location=self.env.target_content_location(
                normalized_quoted_obj))

        # create a symlink using the normalized target path
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=normalized_quoted_obj)
        # and it's ALSO normalized
        self._assertSymlink(
            self.env.link_cont, link_obj,
            expected_content_location=self.env.target_content_location(
                normalized_quoted_obj))

    def test_symlink_put_head_get(self):
        link_obj = uuid4().hex

        # PUT link_obj
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        self._assertSymlink(self.env.link_cont, link_obj)

    def test_symlink_with_etag_put_head_get(self):
        link_obj = uuid4().hex

        # PUT link_obj
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=link_obj,
                                         tgt_cont=self.env.tgt_cont,
                                         tgt_obj=self.env.tgt_obj,
                                         etag=self.env.tgt_etag)

        self._assertSymlink(self.env.link_cont, link_obj)

        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj,
            headers={'If-Match': self.env.tgt_etag})
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj,
            headers={'If-Match': 'not-the-etag'})
        self.assertEqual(resp.status, 412)
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

    def test_static_symlink_with_bad_etag_put_head_get(self):
        link_obj = uuid4().hex

        # PUT link_obj
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=link_obj,
                                         tgt_cont=self.env.tgt_cont,
                                         tgt_obj=self.env.tgt_obj,
                                         etag=self.env.tgt_etag)

        # overwrite tgt object
        self.env._create_tgt_object(body='updated target body')

        resp = retry(
            self._make_request, method='HEAD',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 409)
        # but we still know where it points
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        # uses a mechanism entirely divorced from if-match
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj,
            headers={'If-Match': self.env.tgt_etag})
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj,
            headers={'If-Match': 'not-the-etag'})
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        resp = retry(
            self._make_request, method='DELETE',
            container=self.env.tgt_cont, obj=self.env.tgt_obj)

        # not-found-ness trumps if-match-ness
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 404)
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

    def test_dynamic_link_to_static_link(self):
        static_link_obj = uuid4().hex

        # PUT static_link to tgt_obj
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=static_link_obj,
                                         tgt_cont=self.env.tgt_cont,
                                         tgt_obj=self.env.tgt_obj,
                                         etag=self.env.tgt_etag)

        symlink_obj = uuid4().hex

        # PUT symlink to static_link
        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=symlink_obj,
                               tgt_cont=self.env.link_cont,
                               tgt_obj=static_link_obj)

        self._test_get_as_target_object(
            link_cont=self.env.link_cont, link_obj=symlink_obj,
            expected_content_location=self.env.target_content_location())

    def test_static_link_to_dynamic_link(self):
        symlink_obj = uuid4().hex

        # PUT symlink to tgt_obj
        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=symlink_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        static_link_obj = uuid4().hex

        # PUT a static_link to the symlink
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=static_link_obj,
                                         tgt_cont=self.env.link_cont,
                                         tgt_obj=symlink_obj,
                                         etag=MD5_OF_EMPTY_STRING)

        self._test_get_as_target_object(
            link_cont=self.env.link_cont, link_obj=static_link_obj,
            expected_content_location=self.env.target_content_location())

    def test_static_link_to_nowhere(self):
        missing_obj = uuid4().hex
        static_link_obj = uuid4().hex

        # PUT a static_link to the missing name
        headers = {
            'X-Symlink-Target': '%s/%s' % (self.env.link_cont, missing_obj),
            'X-Symlink-Target-Etag': MD5_OF_EMPTY_STRING}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=static_link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.content, b'X-Symlink-Target does not exist')

    def test_static_link_to_broken_symlink(self):
        symlink_obj = uuid4().hex

        # PUT symlink to tgt_obj
        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=symlink_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        static_link_obj = uuid4().hex

        # PUT a static_link to the symlink
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=static_link_obj,
                                         tgt_cont=self.env.link_cont,
                                         tgt_obj=symlink_obj,
                                         etag=MD5_OF_EMPTY_STRING)

        # break the symlink
        resp = retry(
            self._make_request, method='DELETE',
            container=self.env.tgt_cont, obj=self.env.tgt_obj)
        self.assertEqual(resp.status // 100, 2)

        # sanity
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=symlink_obj)
        self.assertEqual(resp.status, 404)

        # static_link is broken too!
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=static_link_obj)
        self.assertEqual(resp.status, 404)

        # interestingly you may create a static_link to a broken symlink
        broken_static_link_obj = uuid4().hex

        # PUT a static_link to the broken symlink
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=broken_static_link_obj,
                                         tgt_cont=self.env.link_cont,
                                         tgt_obj=symlink_obj,
                                         etag=MD5_OF_EMPTY_STRING)

    def test_symlink_get_ranged(self):
        link_obj = uuid4().hex

        # PUT symlink
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        headers = {'Range': 'bytes=7-10'}
        resp = retry(self._make_request, method='GET',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 206)
        self.assertEqual(resp.content, b'body')

    def test_create_symlink_before_target(self):
        link_obj = uuid4().hex
        target_obj = uuid4().hex

        # PUT link_obj before target object is written
        # PUT, GET, HEAD (on symlink) should all work ok without target object
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont, tgt_obj=target_obj)

        # Try to GET target via symlink.
        # 404 will be returned with Content-Location of target path.
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj, use_account=1)
        self.assertEqual(resp.status, 404)
        self.assertIn('Content-Location', resp.headers)
        self.assertEqual(self.env.target_content_location(target_obj),
                         resp.getheader('content-location'))

        # HEAD on target object via symlink should return a 404 since target
        # object has not yet been written
        resp = retry(
            self._make_request, method='HEAD',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 404)

        # GET on target object directly
        resp = retry(
            self._make_request, method='GET',
            container=self.env.tgt_cont, obj=target_obj)
        self.assertEqual(resp.status, 404)

        # Now let's write target object and symlink will be able to return
        # object
        resp = retry(
            self._make_request, method='PUT', container=self.env.tgt_cont,
            obj=target_obj, body=TARGET_BODY)

        self.assertEqual(resp.status, 201)
        # successful put response has content-length 0
        target_length = str(len(TARGET_BODY))
        target_etag = resp.getheader('etag')

        # sanity: HEAD/GET on link_obj itself
        self._assertLinkObject(self.env.link_cont, link_obj)

        # HEAD target object via symlink
        self._test_head_as_target_object(
            link_cont=self.env.link_cont, link_obj=link_obj)

        # GET target object via symlink
        resp = retry(self._make_request, method='GET',
                     container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)
        self.assertEqual(resp.getheader('content-length'), str(target_length))
        self.assertEqual(resp.getheader('etag'), target_etag)
        self.assertIn('Content-Location', resp.headers)
        self.assertEqual(self.env.target_content_location(target_obj),
                         resp.getheader('content-location'))

    def test_symlink_chain(self):
        # Testing to symlink chain like symlink -> symlink -> target.
        symloop_max = cluster_info['symlink']['symloop_max']

        # create symlink chain in a container. To simplify,
        # use target container for all objects (symlinks and target) here
        previous = self.env.tgt_obj
        container = self.env.tgt_cont

        for link_obj in itertools.islice(self.obj_name_gen, symloop_max):
            # PUT link_obj point to tgt_obj
            self._test_put_symlink(
                link_cont=container, link_obj=link_obj,
                tgt_cont=container, tgt_obj=previous)

            # set corrent link_obj to previous
            previous = link_obj

        # the last link is valid for symloop_max constraint
        max_chain_link = link_obj
        self._assertSymlink(link_cont=container, link_obj=max_chain_link)

        # PUT a new link_obj points to the max_chain_link
        # that will result in 409 error on the HEAD/GET.
        too_many_chain_link = next(self.obj_name_gen)
        self._test_put_symlink(
            link_cont=container, link_obj=too_many_chain_link,
            tgt_cont=container, tgt_obj=max_chain_link)

        # try to HEAD to target object via too_many_chain_link
        resp = retry(self._make_request, method='HEAD',
                     container=container,
                     obj=too_many_chain_link)
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.content, b'')

        # try to GET to target object via too_many_chain_link
        resp = retry(self._make_request, method='GET',
                     container=container,
                     obj=too_many_chain_link)
        self.assertEqual(resp.status, 409)
        self.assertEqual(
            resp.content,
            b'Too many levels of symbolic links, maximum allowed is %d' %
            symloop_max)

        # However, HEAD/GET to the (just) link is still ok
        self._assertLinkObject(container, too_many_chain_link)

    def test_symlink_chain_with_etag(self):
        # Testing to symlink chain like symlink -> symlink -> target.
        symloop_max = cluster_info['symlink']['symloop_max']

        # create symlink chain in a container. To simplify,
        # use target container for all objects (symlinks and target) here
        previous = self.env.tgt_obj
        container = self.env.tgt_cont

        for link_obj in itertools.islice(self.obj_name_gen, symloop_max):
            # PUT link_obj point to tgt_obj
            self._test_put_symlink_with_etag(link_cont=container,
                                             link_obj=link_obj,
                                             tgt_cont=container,
                                             tgt_obj=previous,
                                             etag=self.env.tgt_etag)

            # set current link_obj to previous
            previous = link_obj

        # the last link is valid for symloop_max constraint
        max_chain_link = link_obj
        self._assertSymlink(link_cont=container, link_obj=max_chain_link)

        # chained etag validation works as long as the target symlink works
        headers = {'X-Symlink-Target': '%s/%s' % (container, max_chain_link),
                   'X-Symlink-Target-Etag': 'not-the-real-etag'}
        resp = retry(self._make_request, method='PUT',
                     container=container, obj=uuid4().hex,
                     headers=headers)
        self.assertEqual(resp.status, 409)

        # PUT a new link_obj pointing to the max_chain_link can validate the
        # ETag but will result in 409 error on the HEAD/GET.
        too_many_chain_link = next(self.obj_name_gen)
        self._test_put_symlink_with_etag(
            link_cont=container, link_obj=too_many_chain_link,
            tgt_cont=container, tgt_obj=max_chain_link,
            etag=self.env.tgt_etag)

        # try to HEAD to target object via too_many_chain_link
        resp = retry(self._make_request, method='HEAD',
                     container=container,
                     obj=too_many_chain_link)
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.content, b'')

        # try to GET to target object via too_many_chain_link
        resp = retry(self._make_request, method='GET',
                     container=container,
                     obj=too_many_chain_link)
        self.assertEqual(resp.status, 409)
        self.assertEqual(
            resp.content,
            b'Too many levels of symbolic links, maximum allowed is %d' %
            symloop_max)

        # However, HEAD/GET to the (just) link is still ok
        self._assertLinkObject(container, too_many_chain_link)

    def test_symlink_and_slo_manifest_chain(self):
        if 'slo' not in cluster_info:
            raise SkipTest

        symloop_max = cluster_info['symlink']['symloop_max']

        # create symlink chain in a container. To simplify,
        # use target container for all objects (symlinks and target) here
        previous = self.env.tgt_obj
        container = self.env.tgt_cont

        # make symlink and slo manifest chain
        # e.g. slo -> symlink -> symlink -> slo -> symlink -> symlink -> target
        for _ in range(SloGetContext.max_slo_recursion_depth or 1):
            for link_obj in itertools.islice(self.obj_name_gen, symloop_max):
                # PUT link_obj point to previous object
                self._test_put_symlink(
                    link_cont=container, link_obj=link_obj,
                    tgt_cont=container, tgt_obj=previous)

                # next link will point to this link
                previous = link_obj
            else:
                # PUT a manifest with single segment to the symlink
                manifest_obj = next(self.obj_name_gen)
                manifest = json.dumps(
                    [{'path': '/%s/%s' % (container, link_obj)}])
                resp = retry(self._make_request, method='PUT',
                             container=container, obj=manifest_obj,
                             body=manifest,
                             query_args='multipart-manifest=put')
                self.assertEqual(resp.status, 201)  # sanity
                previous = manifest_obj

        # From the last manifest to the final target obj length is
        # symloop_max * max_slo_recursion_depth
        max_recursion_manifest = previous

        # Check GET to max_recursion_manifest returns valid target object
        resp = retry(
            self._make_request, method='GET', container=container,
            obj=max_recursion_manifest)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)
        self.assertEqual(resp.getheader('content-length'),
                         str(self.env.tgt_length))
        # N.B. since the last manifest is slo so it will remove
        # content-location info from the response header
        self.assertNotIn('Content-Location', resp.headers)

        # sanity: one more link to the slo can work still
        one_more_link = next(self.obj_name_gen)
        self._test_put_symlink(
            link_cont=container, link_obj=one_more_link,
            tgt_cont=container, tgt_obj=max_recursion_manifest)

        resp = retry(
            self._make_request, method='GET', container=container,
            obj=one_more_link)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)
        self.assertEqual(resp.getheader('content-length'),
                         str(self.env.tgt_length))
        self.assertIn('Content-Location', resp.headers)
        self.assertIn('%s/%s' % (container, max_recursion_manifest),
                      resp.getheader('content-location'))

        # PUT a new slo manifest point to the max_recursion_manifest
        # Symlink and slo manifest chain from the new manifest to the final
        # target has (max_slo_recursion_depth + 1) manifests.
        too_many_recursion_manifest = next(self.obj_name_gen)
        manifest = json.dumps(
            [{'path': '/%s/%s' % (container, max_recursion_manifest)}])

        resp = retry(self._make_request, method='PUT',
                     container=container, obj=too_many_recursion_manifest,
                     body=manifest.encode('ascii'),
                     query_args='multipart-manifest=put')
        self.assertEqual(resp.status, 201)  # sanity

        # Check GET to too_many_recursion_mani returns 409 error
        resp = retry(self._make_request, method='GET',
                     container=container, obj=too_many_recursion_manifest)
        self.assertEqual(resp.status, 409)
        # N.B. This error message is from slo middleware that uses default.
        self.assertEqual(
            resp.content,
            b'<html><h1>Conflict</h1><p>There was a conflict when trying to'
            b' complete your request.</p></html>')

    def test_symlink_put_missing_target_container(self):
        link_obj = uuid4().hex

        # set only object, no container in the prefix
        headers = {'X-Symlink-Target': self.env.tgt_obj}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 412)
        self.assertEqual(resp.content,
                         b'X-Symlink-Target header must be of the form'
                         b' <container name>/<object name>')

    def test_symlink_put_non_zero_length(self):
        link_obj = uuid4().hex
        headers = {'X-Symlink-Target':
                   '%s/%s' % (self.env.tgt_cont, self.env.tgt_obj)}
        resp = retry(
            self._make_request, method='PUT', container=self.env.link_cont,
            obj=link_obj, body=b'non-zero-length', headers=headers)

        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.content,
                         b'Symlink requests require a zero byte body')

    def test_symlink_target_itself(self):
        link_obj = uuid4().hex
        headers = {
            'X-Symlink-Target': '%s/%s' % (self.env.link_cont, link_obj)}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.content, b'Symlink cannot target itself')

    def test_symlink_target_each_other(self):
        symloop_max = cluster_info['symlink']['symloop_max']

        link_obj1 = uuid4().hex
        link_obj2 = uuid4().hex

        # PUT two links which targets each other
        self._test_put_symlink(
            link_cont=self.env.link_cont, link_obj=link_obj1,
            tgt_cont=self.env.link_cont, tgt_obj=link_obj2)
        self._test_put_symlink(
            link_cont=self.env.link_cont, link_obj=link_obj2,
            tgt_cont=self.env.link_cont, tgt_obj=link_obj1)

        for obj in (link_obj1, link_obj2):
            # sanity: HEAD/GET on the link itself is ok
            self._assertLinkObject(self.env.link_cont, obj)

        for obj in (link_obj1, link_obj2):
            resp = retry(self._make_request, method='HEAD',
                         container=self.env.link_cont, obj=obj)
            self.assertEqual(resp.status, 409)

            resp = retry(self._make_request, method='GET',
                         container=self.env.link_cont, obj=obj)
            self.assertEqual(resp.status, 409)
            self.assertEqual(
                resp.content,
                b'Too many levels of symbolic links, maximum allowed is %d' %
                symloop_max)

    def test_symlink_put_copy_from(self):
        link_obj1 = uuid4().hex
        link_obj2 = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=link_obj1,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        copy_src = '%s/%s' % (self.env.link_cont, link_obj1)

        # copy symlink
        headers = {'X-Copy-From': copy_src}
        resp = retry(self._make_request_with_symlink_get,
                     method='PUT',
                     container=self.env.link_cont, obj=link_obj2,
                     headers=headers)
        self.assertEqual(resp.status, 201)

        self._assertSymlink(link_cont=self.env.link_cont, link_obj=link_obj2)

    @requires_acls
    def test_symlink_put_copy_from_cross_account(self):
        link_obj1 = uuid4().hex
        link_obj2 = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=link_obj1,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        copy_src = '%s/%s' % (self.env.link_cont, link_obj1)
        perm_two = tf.swift_test_perm[1]

        # add X-Content-Read to account 1 link_cont and tgt_cont
        # permit account 2 to read account 1 link_cont to perform copy_src
        # and tgt_cont so that link_obj2 can refer to tgt_object
        # this ACL allows the copy to succeed
        headers = {'X-Container-Read': perm_two}
        resp = retry(
            self._make_request, method='POST',
            container=self.env.link_cont, headers=headers)
        self.assertEqual(resp.status, 204)

        # this ACL allows link_obj in account 2 to target object in account 1
        resp = retry(self._make_request, method='POST',
                     container=self.env.tgt_cont, headers=headers)
        self.assertEqual(resp.status, 204)

        # copy symlink itself to a different account w/o
        # X-Symlink-Target-Account. This operation will result in copying
        # symlink to the account 2 container that points to the
        # container/object in the account 2.
        # (the container/object is not prepared)
        headers = {'X-Copy-From-Account': self.account_name,
                   'X-Copy-From': copy_src}
        resp = retry(self._make_request_with_symlink_get, method='PUT',
                     container=self.env.link_cont, obj=link_obj2,
                     headers=headers, use_account=2)
        self.assertEqual(resp.status, 201)

        # sanity: HEAD/GET on link_obj itself
        self._assertLinkObject(self.env.link_cont, link_obj2, use_account=2)

        account_two = tf.parsed[1].path.split('/', 2)[2]
        # no target object in the account 2
        for method in ('HEAD', 'GET'):
            resp = retry(
                self._make_request, method=method,
                container=self.env.link_cont, obj=link_obj2, use_account=2)
            self.assertEqual(resp.status, 404)
            self.assertIn('content-location', resp.headers)
            self.assertEqual(
                self.env.target_content_location(override_account=account_two),
                resp.getheader('content-location'))

        # copy symlink itself to a different account with target account
        # the target path will be in account 1
        # the target path will have an object
        headers = {'X-Symlink-target-Account': self.account_name,
                   'X-Copy-From-Account': self.account_name,
                   'X-Copy-From': copy_src}
        resp = retry(
            self._make_request_with_symlink_get, method='PUT',
            container=self.env.link_cont, obj=link_obj2,
            headers=headers, use_account=2)
        self.assertEqual(resp.status, 201)

        self._assertSymlink(link_cont=self.env.link_cont, link_obj=link_obj2,
                            use_account=2)

    def test_symlink_copy_from_target(self):
        link_obj1 = uuid4().hex
        obj2 = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=link_obj1,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        copy_src = '%s/%s' % (self.env.link_cont, link_obj1)

        # issuing a COPY request to a symlink w/o symlink=get, should copy
        # the target object, not the symlink itself
        headers = {'X-Copy-From': copy_src}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.tgt_cont, obj=obj2,
                     headers=headers)
        self.assertEqual(resp.status, 201)

        # HEAD to the copied object
        resp = retry(self._make_request, method='HEAD',
                     container=self.env.tgt_cont, obj=obj2)
        self.assertEqual(200, resp.status)
        self.assertNotIn('Content-Location', resp.headers)
        # GET to the copied object
        resp = retry(self._make_request, method='GET',
                     container=self.env.tgt_cont, obj=obj2)
        # But... this is a raw object (not a symlink)
        self.assertEqual(200, resp.status)
        self.assertNotIn('Content-Location', resp.headers)
        self.assertEqual(TARGET_BODY, resp.content)

    def test_symlink_copy(self):
        link_obj1 = uuid4().hex
        link_obj2 = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=link_obj1,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        copy_dst = '%s/%s' % (self.env.link_cont, link_obj2)

        # copy symlink
        headers = {'Destination': copy_dst}
        resp = retry(
            self._make_request_with_symlink_get, method='COPY',
            container=self.env.link_cont, obj=link_obj1, headers=headers)
        self.assertEqual(resp.status, 201)

        self._assertSymlink(link_cont=self.env.link_cont, link_obj=link_obj2)

    def test_symlink_copy_target(self):
        link_obj1 = uuid4().hex
        obj2 = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=link_obj1,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        copy_dst = '%s/%s' % (self.env.tgt_cont, obj2)

        # copy target object
        headers = {'Destination': copy_dst}
        resp = retry(self._make_request, method='COPY',
                     container=self.env.link_cont, obj=link_obj1,
                     headers=headers)
        self.assertEqual(resp.status, 201)

        # HEAD to target object via symlink
        resp = retry(self._make_request, method='HEAD',
                     container=self.env.tgt_cont, obj=obj2)
        self.assertEqual(resp.status, 200)
        self.assertNotIn('Content-Location', resp.headers)
        # GET to the copied object that should be a raw object (not symlink)
        resp = retry(self._make_request, method='GET',
                     container=self.env.tgt_cont, obj=obj2)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)
        self.assertNotIn('Content-Location', resp.headers)

    def test_post_symlink(self):
        link_obj = uuid4().hex
        value1 = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont,
                               link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        # POSTing to a symlink is not allowed and should return a 307
        headers = {'X-Object-Meta-Alpha': 'apple'}
        resp = retry(
            self._make_request, method='POST', container=self.env.link_cont,
            obj=link_obj, headers=headers, allow_redirects=False)
        self.assertEqual(resp.status, 307)
        # we are using account 0 in this test
        expected_location_hdr = "%s/%s/%s" % (
            tf.parsed[0].path, self.env.tgt_cont, self.env.tgt_obj)
        self.assertEqual(resp.getheader('Location'), expected_location_hdr)

        # Read header from symlink itself. The metadata is applied to symlink
        resp = retry(self._make_request_with_symlink_get, method='GET',
                     container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('X-Object-Meta-Alpha'), 'apple')

        # Post the target object directly
        headers = {'x-object-meta-test': value1}
        resp = retry(
            self._make_request, method='POST', container=self.env.tgt_cont,
            obj=self.env.tgt_obj, headers=headers)
        self.assertEqual(resp.status, 202)
        resp = retry(self._make_request, method='GET',
                     container=self.env.tgt_cont, obj=self.env.tgt_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('X-Object-Meta-Test'), value1)

        # Read header from target object via symlink, should exist now.
        resp = retry(
            self._make_request, method='GET', container=self.env.link_cont,
            obj=link_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('X-Object-Meta-Test'), value1)
        # sanity: no X-Object-Meta-Alpha exists in the response header
        self.assertNotIn('X-Object-Meta-Alpha', resp.headers)

    def test_post_to_broken_dynamic_symlink(self):
        # create a symlink to nowhere
        link_obj = '%s-the-link' % uuid4().hex
        tgt_obj = '%s-no-where' % uuid4().hex
        headers = {'X-Symlink-Target': '%s/%s' % (self.env.tgt_cont, tgt_obj)}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 201)
        # it's a real link!
        self._assertLinkObject(self.env.link_cont, link_obj)
        # ... it's just broken
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 404)
        target_path = '/v1/%s/%s/%s' % (
            self.account_name, self.env.tgt_cont, tgt_obj)
        self.assertEqual(target_path, resp.headers['Content-Location'])

        # we'll redirect with the Location header to the (invalid) target
        headers = {'X-Object-Meta-Alpha': 'apple'}
        resp = retry(
            self._make_request, method='POST', container=self.env.link_cont,
            obj=link_obj, headers=headers, allow_redirects=False)
        self.assertEqual(resp.status, 307)
        self.assertEqual(target_path, resp.headers['Location'])

        # and of course metadata *is* applied to the link
        resp = retry(
            self._make_request_with_symlink_get, method='HEAD',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 200)
        self.assertTrue(resp.getheader('X-Object-Meta-Alpha'), 'apple')

    def test_post_to_broken_static_symlink(self):
        link_obj = uuid4().hex

        # PUT link_obj
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=link_obj,
                                         tgt_cont=self.env.tgt_cont,
                                         tgt_obj=self.env.tgt_obj,
                                         etag=self.env.tgt_etag)

        # overwrite tgt object
        old_tgt_etag = normalize_etag(self.env.tgt_etag)
        self.env._create_tgt_object(body='updated target body')

        # sanity
        resp = retry(
            self._make_request, method='HEAD',
            container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 409)

        # but POST will still 307
        headers = {'X-Object-Meta-Alpha': 'apple'}
        resp = retry(
            self._make_request, method='POST', container=self.env.link_cont,
            obj=link_obj, headers=headers, allow_redirects=False)
        self.assertEqual(resp.status, 307)
        target_path = '/v1/%s/%s/%s' % (
            self.account_name, self.env.tgt_cont, self.env.tgt_obj)
        self.assertEqual(target_path, resp.headers['Location'])
        # but we give you the Etag just like... FYI?
        self.assertEqual(old_tgt_etag, resp.headers['X-Symlink-Target-Etag'])

    def test_post_with_symlink_header(self):
        # POSTing to a symlink is not allowed and should return a 307
        # updating the symlink target with a POST should always fail
        headers = {'X-Symlink-Target': 'container/new_target'}
        resp = retry(
            self._make_request, method='POST', container=self.env.tgt_cont,
            obj=self.env.tgt_obj, headers=headers, allow_redirects=False)
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.content,
                         b'A PUT request is required to set a symlink target')

    def test_overwrite_symlink(self):
        link_obj = uuid4().hex
        new_tgt_obj = "new_target_object_name"
        new_tgt = '%s/%s' % (self.env.tgt_cont, new_tgt_obj)
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        # sanity
        self._assertSymlink(self.env.link_cont, link_obj)

        # Overwrite symlink with PUT
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=new_tgt_obj)

        # head symlink to check X-Symlink-Target header
        resp = retry(self._make_request_with_symlink_get, method='HEAD',
                     container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 200)
        # target should remain with old target
        self.assertEqual(resp.getheader('X-Symlink-Target'), new_tgt)

    def test_delete_symlink(self):
        link_obj = uuid4().hex

        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        resp = retry(self._make_request, method='DELETE',
                     container=self.env.link_cont, obj=link_obj)
        self.assertEqual(resp.status, 204)

        # make sure target object was not deleted and is still reachable
        resp = retry(self._make_request, method='GET',
                     container=self.env.tgt_cont, obj=self.env.tgt_obj)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)

    @requires_acls
    def test_symlink_put_target_account(self):
        if tf.skip or tf.skip2:
            raise SkipTest
        link_obj = uuid4().hex

        # create symlink in account 2
        # pointing to account 1
        headers = {'X-Symlink-Target-Account': self.account_name,
                   'X-Symlink-Target':
                   '%s/%s' % (self.env.tgt_cont, self.env.tgt_obj)}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers, use_account=2)
        self.assertEqual(resp.status, 201)
        perm_two = tf.swift_test_perm[1]

        # sanity test:
        # it should be ok to get the symlink itself, but not the target object
        # because the read acl has not been configured yet
        self._assertLinkObject(self.env.link_cont, link_obj, use_account=2)
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj, use_account=2)

        self.assertEqual(resp.status, 403)
        # still know where it's pointing
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        # add X-Content-Read to account 1 tgt_cont
        # permit account 2 to read account 1 tgt_cont
        # add acl to allow reading from source
        headers = {'X-Container-Read': perm_two}
        resp = retry(self._make_request, method='POST',
                     container=self.env.tgt_cont, headers=headers)
        self.assertEqual(resp.status, 204)

        # GET on link_obj itself
        self._assertLinkObject(self.env.link_cont, link_obj, use_account=2)

        # GET to target object via symlink
        resp = self._test_get_as_target_object(
            self.env.link_cont, link_obj,
            expected_content_location=self.env.target_content_location(),
            use_account=2)

    @requires_acls
    def test_symlink_with_etag_put_target_account(self):
        if tf.skip or tf.skip2:
            raise SkipTest
        link_obj = uuid4().hex

        # try to create a symlink in account 2 pointing to account 1
        symlink_headers = {
            'X-Symlink-Target-Account': self.account_name,
            'X-Symlink-Target':
            '%s/%s' % (self.env.tgt_cont, self.env.tgt_obj),
            'X-Symlink-Target-Etag': self.env.tgt_etag}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=symlink_headers, use_account=2)
        # since we don't have read access to verify the object we get the
        # permissions error
        self.assertEqual(resp.status, 403)
        perm_two = tf.swift_test_perm[1]

        # add X-Content-Read to account 1 tgt_cont
        # permit account 2 to read account 1 tgt_cont
        # add acl to allow reading from source
        acl_headers = {'X-Container-Read': perm_two}
        resp = retry(self._make_request, method='POST',
                     container=self.env.tgt_cont, headers=acl_headers)
        self.assertEqual(resp.status, 204)

        # now we can create the symlink
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=symlink_headers, use_account=2)
        self.assertEqual(resp.status, 201)
        self._assertLinkObject(self.env.link_cont, link_obj, use_account=2)

        # GET to target object via symlink
        resp = self._test_get_as_target_object(
            self.env.link_cont, link_obj,
            expected_content_location=self.env.target_content_location(),
            use_account=2)

        # Overwrite target
        resp = retry(self._make_request, method='PUT',
                     container=self.env.tgt_cont, obj=self.env.tgt_obj,
                     body='some other content')
        self.assertEqual(resp.status, 201)

        # link is now broken
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj, use_account=2)
        self.assertEqual(resp.status, 409)

        # but we still know where it points
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

        # sanity test, remove permissions
        headers = {'X-Remove-Container-Read': 'remove'}
        resp = retry(self._make_request, method='POST',
                     container=self.env.tgt_cont, headers=headers)
        self.assertEqual(resp.status, 204)
        # it should be ok to get the symlink itself, but not the target object
        # because the read acl has been revoked
        self._assertLinkObject(self.env.link_cont, link_obj, use_account=2)
        resp = retry(
            self._make_request, method='GET',
            container=self.env.link_cont, obj=link_obj, use_account=2)
        self.assertEqual(resp.status, 403)
        # Still know where it is, though
        self.assertEqual(resp.getheader('content-location'),
                         self.env.target_content_location())

    def test_symlink_invalid_etag(self):
        link_obj = uuid4().hex
        headers = {'X-Symlink-Target': '%s/%s' % (self.env.tgt_cont,
                                                  self.env.tgt_obj),
                   'X-Symlink-Target-Etag': 'not-the-real-etag'}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.content,
                         b"Object Etag 'ab706c400731332bffa67ed4bc15dcac' "
                         b"does not match X-Symlink-Target-Etag header "
                         b"'not-the-real-etag'")

    def test_symlink_object_listing(self):
        link_obj = uuid4().hex
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)
        # sanity
        self._assertSymlink(self.env.link_cont, link_obj)
        resp = retry(self._make_request, method='GET',
                     container=self.env.link_cont,
                     query_args='format=json')
        self.assertEqual(resp.status, 200)
        object_list = json.loads(resp.content)
        self.assertEqual(len(object_list), 1)
        obj_info = object_list[0]
        self.assertIn('symlink_path', obj_info)
        self.assertEqual(self.env.target_content_location(),
                         obj_info['symlink_path'])
        self.assertNotIn('symlink_etag', obj_info)

    def test_static_link_object_listing(self):
        link_obj = uuid4().hex
        self._test_put_symlink_with_etag(link_cont=self.env.link_cont,
                                         link_obj=link_obj,
                                         tgt_cont=self.env.tgt_cont,
                                         tgt_obj=self.env.tgt_obj,
                                         etag=self.env.tgt_etag)
        # sanity
        self._assertSymlink(self.env.link_cont, link_obj)
        resp = retry(self._make_request, method='GET',
                     container=self.env.link_cont,
                     query_args='format=json')
        self.assertEqual(resp.status, 200)
        object_list = json.loads(resp.content)
        self.assertEqual(len(object_list), 1)
        self.assertIn('symlink_path', object_list[0])
        self.assertEqual(self.env.target_content_location(),
                         object_list[0]['symlink_path'])
        obj_info = object_list[0]
        self.assertIn('symlink_etag', obj_info)
        self.assertEqual(normalize_etag(self.env.tgt_etag),
                         obj_info['symlink_etag'])
        self.assertEqual(int(self.env.tgt_length),
                         obj_info['symlink_bytes'])
        self.assertEqual(obj_info['content_type'], 'application/target')

        # POSTing to a static_link can change the listing Content-Type
        headers = {'Content-Type': 'application/foo'}
        resp = retry(
            self._make_request, method='POST', container=self.env.link_cont,
            obj=link_obj, headers=headers, allow_redirects=False)
        self.assertEqual(resp.status, 307)

        resp = retry(self._make_request, method='GET',
                     container=self.env.link_cont,
                     query_args='format=json')
        self.assertEqual(resp.status, 200)
        object_list = json.loads(resp.content)
        self.assertEqual(len(object_list), 1)
        obj_info = object_list[0]
        self.assertEqual(obj_info['content_type'], 'application/foo')


class TestCrossPolicySymlinkEnv(TestSymlinkEnv):
    multiple_policies_enabled = None

    @classmethod
    def setUp(cls):
        if tf.skip or tf.skip2:
            raise SkipTest

        if cls.multiple_policies_enabled is None:
            try:
                cls.policies = tf.FunctionalStoragePolicyCollection.from_info()
            except AssertionError:
                pass

        if cls.policies and len(cls.policies) > 1:
            cls.multiple_policies_enabled = True
        else:
            cls.multiple_policies_enabled = False
            return

        link_policy = cls.policies.select()
        tgt_policy = cls.policies.exclude(name=link_policy['name']).select()
        link_header = {'X-Storage-Policy': link_policy['name']}
        tgt_header = {'X-Storage-Policy': tgt_policy['name']}

        cls._create_container(cls.link_cont, headers=link_header)
        cls._create_container(cls.tgt_cont, headers=tgt_header)

        # container in account 2
        cls._create_container(cls.link_cont, headers=link_header,
                              use_account=2)
        cls._create_tgt_object()


class TestCrossPolicySymlink(TestSymlink):
    env = TestCrossPolicySymlinkEnv

    def setUp(self):
        super(TestCrossPolicySymlink, self).setUp()
        if self.env.multiple_policies_enabled is False:
            raise SkipTest('Cross policy test requires multiple policies')
        elif self.env.multiple_policies_enabled is not True:
            # just some sanity checking
            raise Exception("Expected multiple_policies_enabled "
                            "to be True/False, got %r" % (
                                self.env.multiple_policies_enabled,))

    def tearDown(self):
        self.env.tearDown()


class TestSymlinkSlo(Base):
    """
    Just some sanity testing of SLO + symlinks.
    It is basically a copy of SLO tests in test_slo, but the tested object is
    a symlink to the manifest (instead of the manifest itself)
    """
    env = TestSloEnv

    def setUp(self):
        super(TestSymlinkSlo, self).setUp()
        if self.env.slo_enabled is False:
            raise SkipTest("SLO not enabled")
        elif self.env.slo_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected slo_enabled to be True/False, got %r" %
                (self.env.slo_enabled,))
        self.file_symlink = self.env.container.file(uuid4().hex)
        self.account_name = self.env.container.conn.storage_path.rsplit(
            '/', 1)[-1]

    def test_symlink_target_slo_manifest(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde')})
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
            (b'c', 1024 * 1024),
            (b'd', 1024 * 1024),
            (b'e', 1),
        ], group_by_byte(self.file_symlink.read()))

        manifest_body = self.file_symlink.read(parms={
            'multipart-manifest': 'get'})
        self.assertEqual(
            [seg['hash'] for seg in json.loads(manifest_body)],
            [self.env.seg_info['seg_%s' % c]['etag'] for c in 'abcde'])

        for obj_info in self.env.container.files(parms={'format': 'json'}):
            if obj_info['name'] == self.file_symlink.name:
                break
        else:
            self.fail('Unable to find file_symlink in listing.')
        obj_info.pop('last_modified')
        self.assertEqual(obj_info, {
            'name': self.file_symlink.name,
            'content_type': 'application/octet-stream',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': 0,
            'symlink_path': '/v1/%s/%s/manifest-abcde' % (
                self.account_name, self.env.container.name),
        })

    def test_static_link_target_slo_manifest(self):
        manifest_info = self.env.container2.file(
            "manifest-abcde").info(parms={
                'multipart-manifest': 'get'})
        manifest_etag = manifest_info['etag']
        self.file_symlink.write(hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container2.name, 'manifest-abcde'),
            'X-Symlink-Target-Etag': manifest_etag,
        })
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
            (b'c', 1024 * 1024),
            (b'd', 1024 * 1024),
            (b'e', 1),
        ], group_by_byte(self.file_symlink.read()))

        manifest_body = self.file_symlink.read(parms={
            'multipart-manifest': 'get'})
        self.assertEqual(
            [seg['hash'] for seg in json.loads(manifest_body)],
            [self.env.seg_info['seg_%s' % c]['etag'] for c in 'abcde'])

        # check listing
        for obj_info in self.env.container.files(parms={'format': 'json'}):
            if obj_info['name'] == self.file_symlink.name:
                break
        else:
            self.fail('Unable to find file_symlink in listing.')
        obj_info.pop('last_modified')
        self.maxDiff = None
        slo_info = self.env.container2.file("manifest-abcde").info()
        self.assertEqual(obj_info, {
            'name': self.file_symlink.name,
            'content_type': 'application/octet-stream',
            'hash': u'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': 0,
            'slo_etag': slo_info['etag'],
            'symlink_path': '/v1/%s/%s/manifest-abcde' % (
                self.account_name, self.env.container2.name),
            'symlink_bytes': 4 * 2 ** 20 + 1,
            'symlink_etag': normalize_etag(manifest_etag),
        })

    def test_static_link_target_slo_manifest_wrong_etag(self):
        # try the slo "etag"
        slo_etag = self.env.container2.file(
            "manifest-abcde").info()['etag']
        self.assertRaises(ResponseError, self.file_symlink.write, hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container2.name, 'manifest-abcde'),
            'X-Symlink-Target-Etag': slo_etag,
        })
        self.assert_status(409)  # quotes OK, but doesn't match

        # try the slo etag w/o the quotes
        slo_etag = slo_etag.strip('"')
        self.assertRaises(ResponseError, self.file_symlink.write, hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container2.name, 'manifest-abcde'),
            'X-Symlink-Target-Etag': slo_etag,
        })
        self.assert_status(409)  # that still doesn't match

    def test_static_link_target_symlink_to_slo_manifest(self):
        # write symlink
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde')})
        # write static_link
        file_static_link = self.env.container.file(uuid4().hex)
        file_static_link.write(hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.file_symlink.container, self.file_symlink.name),
            'X-Symlink-Target-Etag': MD5_OF_EMPTY_STRING,
        })

        # validate reads
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
            (b'c', 1024 * 1024),
            (b'd', 1024 * 1024),
            (b'e', 1),
        ], group_by_byte(file_static_link.read()))

        manifest_body = file_static_link.read(parms={
            'multipart-manifest': 'get'})
        self.assertEqual(
            [seg['hash'] for seg in json.loads(manifest_body)],
            [self.env.seg_info['seg_%s' % c]['etag'] for c in 'abcde'])

        # check listing
        for obj_info in self.env.container.files(parms={'format': 'json'}):
            if obj_info['name'] == file_static_link.name:
                break
        else:
            self.fail('Unable to find file_symlink in listing.')
        obj_info.pop('last_modified')
        self.maxDiff = None
        self.assertEqual(obj_info, {
            'name': file_static_link.name,
            'content_type': 'application/octet-stream',
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': 0,
            'symlink_path': u'/v1/%s/%s/%s' % (
                self.account_name, self.file_symlink.container,
                self.file_symlink.name),
            # the only time bytes/etag aren't the target object are when they
            # validate through another static_link
            'symlink_bytes': 0,
            'symlink_etag': MD5_OF_EMPTY_STRING,
        })

    def test_symlink_target_slo_nested_manifest(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde-submanifest')})
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
            (b'c', 1024 * 1024),
            (b'd', 1024 * 1024),
            (b'e', 1),
        ], group_by_byte(self.file_symlink.read()))

    def test_slo_get_ranged_manifest(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'ranged-manifest')})
        self.assertEqual([
            (b'c', 1),
            (b'd', 1024 * 1024),
            (b'e', 1),
            (b'a', 512 * 1024),
            (b'b', 512 * 1024),
            (b'c', 1),
            (b'd', 1),
        ], group_by_byte(self.file_symlink.read()))

    def test_slo_ranged_get(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde')})
        file_contents = self.file_symlink.read(size=1024 * 1024 + 2,
                                               offset=1024 * 1024 - 1)
        self.assertEqual([
            (b'a', 1),
            (b'b', 1024 * 1024),
            (b'c', 1),
        ], group_by_byte(file_contents))


class TestSymlinkSloEnv(TestSloEnv):

    @classmethod
    def create_links_to_segments(cls, container):
        seg_info = {}
        for letter in ('a', 'b'):
            seg_name = "linkto_seg_%s" % letter
            file_item = container.file(seg_name)
            sym_hdr = {'X-Symlink-Target': '%s/seg_%s' % (container.name,
                                                          letter)}
            file_item.write(hdrs=sym_hdr)
            seg_info[seg_name] = {
                'path': '/%s/%s' % (container.name, seg_name)}
        return seg_info

    @classmethod
    def setUp(cls):
        super(TestSymlinkSloEnv, cls).setUp()

        cls.link_seg_info = cls.create_links_to_segments(cls.container)
        file_item = cls.container.file("manifest-linkto-ab")
        file_item.write(
            json.dumps([cls.link_seg_info['linkto_seg_a'],
                        cls.link_seg_info['linkto_seg_b']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})


class TestSymlinkToSloSegments(Base):
    """
    This test class will contain various tests where the segments of the SLO
    manifest are symlinks to the actual segments. Again the tests are basicaly
    a copy/paste of the tests in test_slo, only the manifest has been modified
    to contain symlinks as the segments.
    """
    env = TestSymlinkSloEnv

    def setUp(self):
        super(TestSymlinkToSloSegments, self).setUp()
        if self.env.slo_enabled is False:
            raise SkipTest("SLO not enabled")
        elif self.env.slo_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected slo_enabled to be True/False, got %r" %
                (self.env.slo_enabled,))

    def test_slo_get_simple_manifest_with_links(self):
        file_item = self.env.container.file("manifest-linkto-ab")
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
        ], group_by_byte(file_item.read()))

    def test_slo_container_listing(self):
        # the listing object size should equal the sum of the size of the
        # segments, not the size of the manifest body
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(
            json.dumps([
                self.env.link_seg_info['linkto_seg_a']]).encode('ascii'),
            parms={'multipart-manifest': 'put'})

        # The container listing has the etag of the actual manifest object
        # contents which we get using multipart-manifest=get. New enough swift
        # also exposes the etag that we get when NOT using
        # multipart-manifest=get. Verify that both remain consistent when the
        # object is updated with a POST.
        file_item.initialize()
        slo_etag = file_item.etag
        file_item.initialize(parms={'multipart-manifest': 'get'})
        manifest_etag = file_item.etag

        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual('application/octet-stream',
                                 f_dict['content_type'])
                if tf.cluster_info.get('etag_quoter', {}).get(
                        'enable_by_default'):
                    self.assertEqual(manifest_etag, '"%s"' % f_dict['hash'])
                else:
                    self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

        # now POST updated content-type file
        file_item.content_type = 'image/jpeg'
        file_item.sync_metadata({'X-Object-Meta-Test': 'blah'})
        file_item.initialize()
        self.assertEqual('image/jpeg', file_item.content_type)  # sanity

        # verify that the container listing is consistent with the file
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual(file_item.content_type,
                                 f_dict['content_type'])
                if tf.cluster_info.get('etag_quoter', {}).get(
                        'enable_by_default'):
                    self.assertEqual(manifest_etag, '"%s"' % f_dict['hash'])
                else:
                    self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

        # now POST with no change to content-type
        file_item.sync_metadata({'X-Object-Meta-Test': 'blah'},
                                cfg={'no_content_type': True})
        file_item.initialize()
        self.assertEqual('image/jpeg', file_item.content_type)  # sanity

        # verify that the container listing is consistent with the file
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual(file_item.content_type,
                                 f_dict['content_type'])
                if tf.cluster_info.get('etag_quoter', {}).get(
                        'enable_by_default'):
                    self.assertEqual(manifest_etag, '"%s"' % f_dict['hash'])
                else:
                    self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

    def test_slo_etag_is_hash_of_etags(self):
        expected_hash = md5(usedforsecurity=False)
        expected_hash.update((
            md5(b'a' * 1024 * 1024, usedforsecurity=False)
            .hexdigest().encode('ascii')))
        expected_hash.update((
            md5(b'b' * 1024 * 1024, usedforsecurity=False)
            .hexdigest().encode('ascii')))
        expected_etag = expected_hash.hexdigest()

        file_item = self.env.container.file('manifest-linkto-ab')
        self.assertEqual('"%s"' % expected_etag, file_item.info()['etag'])

    def test_slo_copy(self):
        file_item = self.env.container.file("manifest-linkto-ab")
        file_item.copy(self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        self.assertEqual([
            (b'a', 1024 * 1024),
            (b'b', 1024 * 1024),
        ], group_by_byte(copied.read(parms={'multipart-manifest': 'get'})))

    def test_slo_copy_the_manifest(self):
        # first just perform some tests of the contents of the manifest itself
        source = self.env.container.file("manifest-linkto-ab")
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        manifest_etag = md5(source_contents, usedforsecurity=False).hexdigest()
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            manifest_etag = '"%s"' % manifest_etag

        source.initialize()
        slo_etag = source.etag
        self.assertEqual('application/octet-stream', source.content_type)

        source.initialize(parms={'multipart-manifest': 'get'})
        self.assertEqual(manifest_etag, source.etag)
        self.assertEqual('application/json; charset=utf-8',
                         source.content_type)

        # now, copy the manifest
        self.assertTrue(source.copy(self.env.container.name,
                                    "copied-ab-manifest-only",
                                    parms={'multipart-manifest': 'get'}))

        copied = self.env.container.file("copied-ab-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")

        # make sure content of copied manifest is the same as original man.
        self.assertEqual(source_json, copied_json)
        copied.initialize()
        self.assertEqual(copied.etag, slo_etag)
        self.assertEqual('application/octet-stream', copied.content_type)

        copied.initialize(parms={'multipart-manifest': 'get'})
        self.assertEqual(source_contents, copied_contents)
        self.assertEqual(copied.etag, manifest_etag)
        self.assertEqual('application/json; charset=utf-8',
                         copied.content_type)

        # verify the listing metadata
        listing = self.env.container.files(parms={'format': 'json'})
        names = {}
        for f_dict in listing:
            if f_dict['name'] in ('manifest-linkto-ab',
                                  'copied-ab-manifest-only'):
                names[f_dict['name']] = f_dict

        self.assertIn('manifest-linkto-ab', names)
        actual = names['manifest-linkto-ab']
        self.assertEqual(2 * 1024 * 1024, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            self.assertEqual(manifest_etag, '"%s"' % actual['hash'])
        else:
            self.assertEqual(manifest_etag, actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])

        self.assertIn('copied-ab-manifest-only', names)
        actual = names['copied-ab-manifest-only']
        self.assertEqual(2 * 1024 * 1024, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            self.assertEqual(manifest_etag, '"%s"' % actual['hash'])
        else:
            self.assertEqual(manifest_etag, actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])


class TestSymlinkDlo(Base):
    env = TestDloEnv

    def test_get_manifest(self):
        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man1')})

        self.assertEqual([
            (b'a', 10),
            (b'b', 10),
            (b'c', 10),
            (b'd', 10),
            (b'e', 10),
        ], group_by_byte(file_symlink.read()))

        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man2')})
        self.assertEqual([
            (b'A', 10),
            (b'B', 10),
            (b'C', 10),
            (b'D', 10),
            (b'E', 10),
        ], group_by_byte(file_symlink.read()))

        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'manall')})
        self.assertEqual([
            (b'a', 10),
            (b'b', 10),
            (b'c', 10),
            (b'd', 10),
            (b'e', 10),
            (b'A', 10),
            (b'B', 10),
            (b'C', 10),
            (b'D', 10),
            (b'E', 10),
        ], group_by_byte(file_symlink.read()))

    def test_get_manifest_document_itself(self):
        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man1')})
        file_contents = file_symlink.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(file_contents, b"man1-contents")
        self.assertEqual(file_symlink.info()['x_object_manifest'],
                         "%s/%s/seg_lower" %
                         (self.env.container.name, self.env.segment_prefix))

    def test_get_range(self):
        link_obj = uuid4().hex + "_symlink"
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man1')})
        self.assertEqual([
            (b'a', 2),
            (b'b', 10),
            (b'c', 10),
            (b'd', 3),
        ], group_by_byte(file_symlink.read(size=25, offset=8)))

        file_contents = file_symlink.read(size=1, offset=47)
        self.assertEqual(file_contents, b"e")

    def test_get_range_out_of_range(self):
        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man1')})

        self.assertRaises(ResponseError, file_symlink.read, size=7, offset=50)
        self.assert_status(416)


class TestSymlinkTargetObjectComparisonEnv(TestFileComparisonEnv):
    @classmethod
    def setUp(cls):
        super(TestSymlinkTargetObjectComparisonEnv, cls).setUp()
        cls.parms = None
        cls.expect_empty_etag = False
        cls.expect_body = True


class TestSymlinkComparisonEnv(TestFileComparisonEnv):
    @classmethod
    def setUp(cls):
        super(TestSymlinkComparisonEnv, cls).setUp()
        cls.parms = {'symlink': 'get'}
        cls.expect_empty_etag = True
        cls.expect_body = False


class TestSymlinkTargetObjectComparison(Base):
    env = TestSymlinkTargetObjectComparisonEnv

    def setUp(self):
        super(TestSymlinkTargetObjectComparison, self).setUp()
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)
            file_symlink.write(hdrs={'X-Symlink-Target':
                               '%s/%s' % (self.env.container.name,
                                          file_item.name)})

    def testIfMatch(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)

            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5
            hdrs = {'If-Match': md5}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)

            hdrs = {'If-Match': 'bogus'}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_etag(md5)

    def testIfMatchMultipleEtags(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)

            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5
            hdrs = {'If-Match': '"bogus1", "%s", "bogus2"' % md5}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)

            hdrs = {'If-Match': '"bogus1", "bogus2", "bogus3"'}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_etag(md5)

    def testIfNoneMatch(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)
            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5

            hdrs = {'If-None-Match': 'bogus'}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)

            hdrs = {'If-None-Match': md5}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_etag(md5)
            self.assert_header('accept-ranges', 'bytes')

    def testIfNoneMatchMultipleEtags(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)
            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5

            hdrs = {'If-None-Match': '"bogus1", "bogus2", "bogus3"'}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)

            hdrs = {'If-None-Match':
                    '"bogus1", "bogus2", "%s"' % md5}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_etag(md5)
            self.assert_header('accept-ranges', 'bytes')

    def testIfModifiedSince(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)
            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5

            hdrs = {'If-Modified-Since': self.env.time_old_f1}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)
            self.assertTrue(file_symlink.info(hdrs=hdrs, parms=self.env.parms))

            hdrs = {'If-Modified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_etag(md5)
            self.assert_header('accept-ranges', 'bytes')
            self.assertRaises(ResponseError, file_symlink.info, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_etag(md5)
            self.assert_header('accept-ranges', 'bytes')

    def testIfUnmodifiedSince(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)
            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5

            hdrs = {'If-Unmodified-Since': self.env.time_new}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)
            self.assertTrue(file_symlink.info(hdrs=hdrs, parms=self.env.parms))

            hdrs = {'If-Unmodified-Since': self.env.time_old_f2}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_etag(md5)
            self.assertRaises(ResponseError, file_symlink.info, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_etag(md5)

    def testIfMatchAndUnmodified(self):
        for file_item in self.env.files:
            link_obj = file_item.name + '_symlink'
            file_symlink = self.env.container.file(link_obj)
            md5 = MD5_OF_EMPTY_STRING if self.env.expect_empty_etag else \
                file_item.md5

            hdrs = {'If-Match': md5,
                    'If-Unmodified-Since': self.env.time_new}
            body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
            if self.env.expect_body:
                self.assertTrue(body)
            else:
                self.assertEqual(b'', body)
            self.assert_status(200)
            self.assert_etag(md5)

            hdrs = {'If-Match': 'bogus',
                    'If-Unmodified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_etag(md5)

            hdrs = {'If-Match': md5,
                    'If-Unmodified-Since': self.env.time_old_f3}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_etag(md5)

    def testLastModified(self):
        file_item = self.env.container.file(Utils.create_name())
        file_item.content_type = Utils.create_name()
        resp = file_item.write_random_return_resp(self.env.file_size)
        put_last_modified = resp.getheader('last-modified')
        md5 = file_item.md5

        # create symlink
        link_obj = file_item.name + '_symlink'
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      file_item.name)})

        info = file_symlink.info()
        self.assertIn('last_modified', info)
        last_modified = info['last_modified']
        self.assertEqual(put_last_modified, info['last_modified'])

        hdrs = {'If-Modified-Since': last_modified}
        self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs)
        self.assert_status(304)
        self.assert_etag(md5)
        self.assert_header('accept-ranges', 'bytes')

        hdrs = {'If-Unmodified-Since': last_modified}
        self.assertTrue(file_symlink.read(hdrs=hdrs))


class TestSymlinkComparison(TestSymlinkTargetObjectComparison):
    env = TestSymlinkComparisonEnv

    def setUp(self):
        super(TestSymlinkComparison, self).setUp()

    def testLastModified(self):
        file_item = self.env.container.file(Utils.create_name())
        file_item.content_type = Utils.create_name()
        resp = file_item.write_random_return_resp(self.env.file_size)
        put_target_last_modified = resp.getheader('last-modified')
        md5 = MD5_OF_EMPTY_STRING

        # get different last-modified between file and symlink
        time.sleep(1)

        # create symlink
        link_obj = file_item.name + '_symlink'
        file_symlink = self.env.container.file(link_obj)
        resp = file_symlink.write(return_resp=True,
                                  hdrs={'X-Symlink-Target':
                                        '%s/%s' % (self.env.container.name,
                                                   file_item.name)})
        put_sym_last_modified = resp.getheader('last-modified')

        info = file_symlink.info(parms=self.env.parms)
        self.assertIn('last_modified', info)
        last_modified = info['last_modified']
        self.assertEqual(put_sym_last_modified, info['last_modified'])

        hdrs = {'If-Modified-Since': put_target_last_modified}
        body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
        self.assertEqual(b'', body)
        self.assert_status(200)
        self.assert_etag(md5)

        hdrs = {'If-Modified-Since': last_modified}
        self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                          parms=self.env.parms)
        self.assert_status(304)
        self.assert_etag(md5)
        self.assert_header('accept-ranges', 'bytes')

        hdrs = {'If-Unmodified-Since': last_modified}
        body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
        self.assertEqual(b'', body)
        self.assert_status(200)
        self.assert_etag(md5)


class TestSymlinkAccountTempurl(Base):
    env = TestTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        super(TestSymlinkAccountTempurl, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        if self.digest_name not in cluster_info['tempurl'].get(
                'allowed_digests', ['sha1']):
            raise SkipTest("tempurl does not support %s signatures" %
                           self.digest_name)

        self.digest = getattr(hashlib, self.digest_name)
        self.expires = int(time.time()) + 86400
        self.obj_tempurl_parms = self.tempurl_parms(
            'GET', self.expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)

    def tempurl_parms(self, method, expires, path, key):
        path = urllib.parse.unquote(path)
        method = method.encode('utf8')
        path = path.encode('utf8')
        key = key.encode('utf8')
        sig = hmac.new(
            key,
            b'%s\n%d\n%s' % (method, expires, path),
            self.digest).hexdigest()
        return {'temp_url_sig': sig, 'temp_url_expires': str(expires)}

    def test_PUT_symlink(self):
        new_sym = self.env.container.file(Utils.create_name())

        # give out a signature which allows a PUT to new_obj
        expires = int(time.time()) + 86400
        put_parms = self.tempurl_parms(
            'PUT', expires, self.env.conn.make_path(new_sym.path),
            self.env.tempurl_key)

        # try to create symlink object
        try:
            new_sym.write(
                b'', {'x-symlink-target': 'cont/foo'}, parms=put_parms,
                cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

    def test_GET_symlink_inside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        sym = self.env.container.file(Utils.create_name())
        tgt_obj.write(b"target object body")
        sym.write(
            b'',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        get_parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)

        contents = sym.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"target object body")

    def test_GET_symlink_outside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        tgt_obj.write(b"target object body")

        container2 = self.env.account.container(Utils.create_name())
        container2.create()

        sym = container2.file(Utils.create_name())
        sym.write(
            b'',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        get_parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)

        # cross container tempurl works fine for account tempurl key
        contents = sym.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"target object body")


class TestSymlinkContainerTempurl(Base):
    env = TestContainerTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        super(TestSymlinkContainerTempurl, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        if self.digest_name not in cluster_info['tempurl'].get(
                'allowed_digests', ['sha1']):
            raise SkipTest("tempurl does not support %s signatures" %
                           self.digest_name)

        self.digest = getattr(hashlib, self.digest_name)
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        self.obj_tempurl_parms = {'temp_url_sig': sig,
                                  'temp_url_expires': str(expires)}

    def tempurl_sig(self, method, expires, path, key):
        path = urllib.parse.unquote(path)
        method = method.encode('utf8')
        path = path.encode('utf8')
        key = key.encode('utf8')
        return hmac.new(
            key,
            b'%s\n%d\n%s' % (method, expires, path),
            self.digest).hexdigest()

    def test_PUT_symlink(self):
        new_sym = self.env.container.file(Utils.create_name())

        # give out a signature which allows a PUT to new_obj
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'PUT', expires, self.env.conn.make_path(new_sym.path),
            self.env.tempurl_key)
        put_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        # try to create symlink object, should fail
        try:
            new_sym.write(
                b'', {'x-symlink-target': 'cont/foo'}, parms=put_parms,
                cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

    def test_GET_symlink_inside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        sym = self.env.container.file(Utils.create_name())
        tgt_obj.write(b"target object body")
        sym.write(
            b'',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = sym.read(parms=parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"target object body")

    def test_GET_symlink_outside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        tgt_obj.write(b"target object body")

        container2 = self.env.account.container(Utils.create_name())
        container2.create()

        sym = container2.file(Utils.create_name())
        sym.write(
            b'',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        # cross container tempurl does not work for container tempurl key
        try:
            sym.read(parms=parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 401)
        else:
            self.fail('request did not error')
        try:
            sym.info(parms=parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 401)
        else:
            self.fail('request did not error')


if __name__ == '__main__':
    unittest.main()
