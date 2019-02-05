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
import unittest2
import itertools
import hashlib
import time

from six.moves import urllib
from uuid import uuid4

from swift.common.http import is_success
from swift.common.utils import json, MD5_OF_EMPTY_STRING
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

TARGET_BODY = 'target body'


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
    def target_content_location(cls):
        return '%s/%s' % (cls.tgt_cont, cls.tgt_obj)

    @classmethod
    def _make_request(cls, url, token, parsed, conn, method,
                      container, obj='', headers=None, body='',
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
    def _create_tgt_object(cls):
        resp = retry(cls._make_request, method='PUT',
                     container=cls.tgt_cont, obj=cls.tgt_obj,
                     body=TARGET_BODY)
        if resp.status != 201:
            raise ResponseError(resp)

        # sanity: successful put response has content-length 0
        cls.tgt_length = str(len(TARGET_BODY))
        cls.tgt_etag = resp.getheader('etag')

        resp = retry(cls._make_request, method='GET',
                     container=cls.tgt_cont, obj=cls.tgt_obj)
        if resp.status != 200 and resp.content != TARGET_BODY:
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

    def tearDown(self):
        self.env.tearDown()

    def _make_request(self, url, token, parsed, conn, method,
                      container, obj='', headers=None, body='',
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
                                       container, obj, headers=None, body=''):
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

    def _test_get_as_target_object(
            self, link_cont, link_obj, expected_content_location,
            use_account=1):
        resp = retry(
            self._make_request, method='GET',
            container=link_cont, obj=link_obj, use_account=use_account)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content, TARGET_BODY)
        self.assertEqual(resp.getheader('content-length'),
                         str(self.env.tgt_length))
        self.assertEqual(resp.getheader('etag'), self.env.tgt_etag)
        self.assertIn('Content-Location', resp.headers)
        # TODO: content-location is a full path so it's better to assert
        # with the value, instead of assertIn
        self.assertIn(expected_content_location,
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
        self.assertEqual(resp.content, '')
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

        # Now let's write a new target object and symlink will be able to
        # return object
        resp = retry(
            self._make_request, method='PUT', container=self.env.tgt_cont,
            obj=target_obj, body=TARGET_BODY)

        self.assertEqual(resp.status, 201)

        # PUT symlink
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=target_obj)

        self._assertSymlink(
            self.env.link_cont, link_obj,
            expected_content_location="%s/%s" % (self.env.tgt_cont,
                                                 target_obj))

    def test_symlink_put_head_get(self):
        link_obj = uuid4().hex

        # PUT link_obj
        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
                               tgt_cont=self.env.tgt_cont,
                               tgt_obj=self.env.tgt_obj)

        self._assertSymlink(self.env.link_cont, link_obj)

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
        self.assertEqual(resp.content, 'body')

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
        expected_location_hdr = "%s/%s" % (self.env.tgt_cont, target_obj)
        self.assertIn(expected_location_hdr,
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
        self.assertIn(expected_location_hdr,
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
        self.assertEqual(resp.content, '')

        # try to GET to target object via too_many_chain_link
        resp = retry(self._make_request, method='GET',
                     container=container,
                     obj=too_many_chain_link)
        self.assertEqual(resp.status, 409)
        self.assertEqual(
            resp.content,
            'Too many levels of symbolic links, maximum allowed is %d' %
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
                     body=manifest,
                     query_args='multipart-manifest=put')
        self.assertEqual(resp.status, 201)  # sanity

        # Check GET to too_many_recursion_mani returns 409 error
        resp = retry(self._make_request, method='GET',
                     container=container, obj=too_many_recursion_manifest)
        self.assertEqual(resp.status, 409)
        # N.B. This error message is from slo middleware that uses default.
        self.assertEqual(
            resp.content,
            '<html><h1>Conflict</h1><p>There was a conflict when trying to'
            ' complete your request.</p></html>')

    def test_symlink_put_missing_target_container(self):
        link_obj = uuid4().hex

        # set only object, no container in the prefix
        headers = {'X-Symlink-Target': self.env.tgt_obj}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 412)
        self.assertEqual(resp.content,
                         'X-Symlink-Target header must be of the form'
                         ' <container name>/<object name>')

    def test_symlink_put_non_zero_length(self):
        link_obj = uuid4().hex
        headers = {'X-Symlink-Target':
                   '%s/%s' % (self.env.tgt_cont, self.env.tgt_obj)}
        resp = retry(
            self._make_request, method='PUT', container=self.env.link_cont,
            obj=link_obj, body='non-zero-length', headers=headers)

        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.content,
                         'Symlink requests require a zero byte body')

    def test_symlink_target_itself(self):
        link_obj = uuid4().hex
        headers = {
            'X-Symlink-Target': '%s/%s' % (self.env.link_cont, link_obj)}
        resp = retry(self._make_request, method='PUT',
                     container=self.env.link_cont, obj=link_obj,
                     headers=headers)
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.content, 'Symlink cannot target itself')

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
                'Too many levels of symbolic links, maximum allowed is %d' %
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
        account_one = tf.parsed[0].path.split('/', 2)[2]
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
        headers = {'X-Copy-From-Account': account_one,
                   'X-Copy-From': copy_src}
        resp = retry(self._make_request_with_symlink_get, method='PUT',
                     container=self.env.link_cont, obj=link_obj2,
                     headers=headers, use_account=2)
        self.assertEqual(resp.status, 201)

        # sanity: HEAD/GET on link_obj itself
        self._assertLinkObject(self.env.link_cont, link_obj2, use_account=2)

        # no target object in the account 2
        for method in ('HEAD', 'GET'):
            resp = retry(
                self._make_request, method=method,
                container=self.env.link_cont, obj=link_obj2, use_account=2)
            self.assertEqual(resp.status, 404)
            self.assertIn('content-location', resp.headers)
            self.assertIn(self.env.target_content_location(),
                          resp.getheader('content-location'))

        # copy symlink itself to a different account with target account
        # the target path will be in account 1
        # the target path will have an object
        headers = {'X-Symlink-target-Account': account_one,
                   'X-Copy-From-Account': account_one,
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

        self._test_put_symlink(link_cont=self.env.link_cont, link_obj=link_obj,
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

    def test_post_with_symlink_header(self):
        # POSTing to a symlink is not allowed and should return a 307
        # updating the symlink target with a POST should always fail
        headers = {'X-Symlink-Target': 'container/new_target'}
        resp = retry(
            self._make_request, method='POST', container=self.env.tgt_cont,
            obj=self.env.tgt_obj, headers=headers, allow_redirects=False)
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.content,
                         'A PUT request is required to set a symlink target')

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

        account_one = tf.parsed[0].path.split('/', 2)[2]

        # create symlink in account 2
        # pointing to account 1
        headers = {'X-Symlink-Target-Account': account_one,
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
        self.assertIn(account_one, resp.getheader('content-location'))

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
        self.assertIn('symlink_path', object_list[0])
        self.assertIn(self.env.target_content_location(),
                      object_list[0]['symlink_path'])


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

    def test_symlink_target_slo_manifest(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde')})
        file_contents = self.file_symlink.read()
        self.assertEqual(4 * 1024 * 1024 + 1, len(file_contents))
        self.assertEqual('a', file_contents[0])
        self.assertEqual('a', file_contents[1024 * 1024 - 1])
        self.assertEqual('b', file_contents[1024 * 1024])
        self.assertEqual('d', file_contents[-2])
        self.assertEqual('e', file_contents[-1])

    def test_symlink_target_slo_nested_manifest(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde-submanifest')})
        file_contents = self.file_symlink.read()
        self.assertEqual(4 * 1024 * 1024 + 1, len(file_contents))
        self.assertEqual('a', file_contents[0])
        self.assertEqual('a', file_contents[1024 * 1024 - 1])
        self.assertEqual('b', file_contents[1024 * 1024])
        self.assertEqual('d', file_contents[-2])
        self.assertEqual('e', file_contents[-1])

    def test_slo_get_ranged_manifest(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'ranged-manifest')})
        grouped_file_contents = [
            (char, sum(1 for _char in grp))
            for char, grp in itertools.groupby(self.file_symlink.read())]
        self.assertEqual([
            ('c', 1),
            ('d', 1024 * 1024),
            ('e', 1),
            ('a', 512 * 1024),
            ('b', 512 * 1024),
            ('c', 1),
            ('d', 1)], grouped_file_contents)

    def test_slo_ranged_get(self):
        self.file_symlink.write(hdrs={'X-Symlink-Target':
                                '%s/%s' % (self.env.container.name,
                                           'manifest-abcde')})
        file_contents = self.file_symlink.read(size=1024 * 1024 + 2,
                                               offset=1024 * 1024 - 1)
        self.assertEqual('a', file_contents[0])
        self.assertEqual('b', file_contents[1])
        self.assertEqual('b', file_contents[-2])
        self.assertEqual('c', file_contents[-1])


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
                        cls.link_seg_info['linkto_seg_b']]),
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
        file_contents = file_item.read()
        self.assertEqual(2 * 1024 * 1024, len(file_contents))
        self.assertEqual('a', file_contents[0])
        self.assertEqual('a', file_contents[1024 * 1024 - 1])
        self.assertEqual('b', file_contents[1024 * 1024])

    def test_slo_container_listing(self):
        # the listing object size should equal the sum of the size of the
        # segments, not the size of the manifest body
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(
            json.dumps([self.env.link_seg_info['linkto_seg_a']]),
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
                self.assertEqual(manifest_etag, f_dict['hash'])
                self.assertEqual(slo_etag, f_dict['slo_etag'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

    def test_slo_etag_is_hash_of_etags(self):
        expected_hash = hashlib.md5()
        expected_hash.update(hashlib.md5('a' * 1024 * 1024).hexdigest())
        expected_hash.update(hashlib.md5('b' * 1024 * 1024).hexdigest())
        expected_etag = expected_hash.hexdigest()

        file_item = self.env.container.file('manifest-linkto-ab')
        self.assertEqual('"%s"' % expected_etag, file_item.info()['etag'])

    def test_slo_copy(self):
        file_item = self.env.container.file("manifest-linkto-ab")
        file_item.copy(self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(2 * 1024 * 1024, len(copied_contents))

    def test_slo_copy_the_manifest(self):
        # first just perform some tests of the contents of the manifest itself
        source = self.env.container.file("manifest-linkto-ab")
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        manifest_etag = hashlib.md5(source_contents).hexdigest()

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
        self.assertEqual(manifest_etag, actual['hash'])
        self.assertEqual(slo_etag, actual['slo_etag'])

        self.assertIn('copied-ab-manifest-only', names)
        actual = names['copied-ab-manifest-only']
        self.assertEqual(2 * 1024 * 1024, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
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

        file_contents = file_symlink.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")

        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man2')})
        file_contents = file_symlink.read()
        self.assertEqual(
            file_contents,
            "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE")

        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'manall')})
        file_contents = file_symlink.read()
        self.assertEqual(
            file_contents,
            ("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee" +
             "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE"))

    def test_get_manifest_document_itself(self):
        link_obj = uuid4().hex
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man1')})
        file_contents = file_symlink.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(file_contents, "man1-contents")
        self.assertEqual(file_symlink.info()['x_object_manifest'],
                         "%s/%s/seg_lower" %
                         (self.env.container.name, self.env.segment_prefix))

    def test_get_range(self):
        link_obj = uuid4().hex + "_symlink"
        file_symlink = self.env.container.file(link_obj)
        file_symlink.write(hdrs={'X-Symlink-Target':
                           '%s/%s' % (self.env.container.name,
                                      'man1')})
        file_contents = file_symlink.read(size=25, offset=8)
        self.assertEqual(file_contents, "aabbbbbbbbbbccccccccccddd")

        file_contents = file_symlink.read(size=1, offset=47)
        self.assertEqual(file_contents, "e")

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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)

            hdrs = {'If-Match': 'bogus'}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_header('etag', md5)

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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)

            hdrs = {'If-Match': '"bogus1", "bogus2", "bogus3"'}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_header('etag', md5)

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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)

            hdrs = {'If-None-Match': md5}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_header('etag', md5)
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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)

            hdrs = {'If-None-Match':
                    '"bogus1", "bogus2", "%s"' % md5}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_header('etag', md5)
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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)
            self.assertTrue(file_symlink.info(hdrs=hdrs, parms=self.env.parms))

            hdrs = {'If-Modified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_header('etag', md5)
            self.assert_header('accept-ranges', 'bytes')
            self.assertRaises(ResponseError, file_symlink.info, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(304)
            self.assert_header('etag', md5)
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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)
            self.assertTrue(file_symlink.info(hdrs=hdrs, parms=self.env.parms))

            hdrs = {'If-Unmodified-Since': self.env.time_old_f2}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_header('etag', md5)
            self.assertRaises(ResponseError, file_symlink.info, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_header('etag', md5)

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
                self.assertEqual('', body)
            self.assert_status(200)
            self.assert_header('etag', md5)

            hdrs = {'If-Match': 'bogus',
                    'If-Unmodified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_header('etag', md5)

            hdrs = {'If-Match': md5,
                    'If-Unmodified-Since': self.env.time_old_f3}
            self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                              parms=self.env.parms)
            self.assert_status(412)
            self.assert_header('etag', md5)

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
        self.assert_header('etag', md5)
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
        self.assertEqual('', body)
        self.assert_status(200)
        self.assert_header('etag', md5)

        hdrs = {'If-Modified-Since': last_modified}
        self.assertRaises(ResponseError, file_symlink.read, hdrs=hdrs,
                          parms=self.env.parms)
        self.assert_status(304)
        self.assert_header('etag', md5)
        self.assert_header('accept-ranges', 'bytes')

        hdrs = {'If-Unmodified-Since': last_modified}
        body = file_symlink.read(hdrs=hdrs, parms=self.env.parms)
        self.assertEqual('', body)
        self.assert_status(200)
        self.assert_header('etag', md5)


class TestSymlinkAccountTempurl(Base):
    env = TestTempurlEnv
    digest_name = 'sha1'

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
        sig = hmac.new(
            key,
            '%s\n%s\n%s' % (method, expires, urllib.parse.unquote(path)),
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
                '', {'x-symlink-target': 'cont/foo'}, parms=put_parms,
                cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

    def test_GET_symlink_inside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        sym = self.env.container.file(Utils.create_name())
        tgt_obj.write("target object body")
        sym.write(
            '',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        get_parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)

        contents = sym.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, "target object body")

    def test_GET_symlink_outside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        tgt_obj.write("target object body")

        container2 = self.env.account.container(Utils.create_name())
        container2.create()

        sym = container2.file(Utils.create_name())
        sym.write(
            '',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        get_parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)

        # cross container tempurl works fine for account tempurl key
        contents = sym.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, "target object body")


class TestSymlinkContainerTempurl(Base):
    env = TestContainerTempurlEnv
    digest_name = 'sha1'

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
        return hmac.new(
            key,
            '%s\n%s\n%s' % (method, expires, urllib.parse.unquote(path)),
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
                '', {'x-symlink-target': 'cont/foo'}, parms=put_parms,
                cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

    def test_GET_symlink_inside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        sym = self.env.container.file(Utils.create_name())
        tgt_obj.write("target object body")
        sym.write(
            '',
            {'x-symlink-target': '%s/%s' % (self.env.container.name, tgt_obj)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(sym.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = sym.read(parms=parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, "target object body")

    def test_GET_symlink_outside_container(self):
        tgt_obj = self.env.container.file(Utils.create_name())
        tgt_obj.write("target object body")

        container2 = self.env.account.container(Utils.create_name())
        container2.create()

        sym = container2.file(Utils.create_name())
        sym.write(
            '',
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
    unittest2.main()
