# -*- coding: utf-8 -*-
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

import operator
import os
import posix
import mock
import unittest
import itertools
from contextlib import contextmanager
from shutil import rmtree
from tempfile import mkdtemp
from test.unit import make_timestamp_iter, mock_timestamp_now
from time import gmtime
from xml.dom import minidom
import time
import random

from eventlet import spawn, Timeout
import json
import six
from six import BytesIO
from six import StringIO

from swift import __version__ as swift_version
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import (Request, WsgiBytesIO, HTTPNoContent,
                               bytes_to_wsgi)
import swift.container
from swift.container import server as container_server
from swift.common import constraints
from swift.common.utils import (Timestamp, mkdirs, public, replication,
                                storage_directory, lock_parent_directory,
                                ShardRange)
from test.unit import fake_http_connect, debug_logger, mock_check_drive
from swift.common.storage_policy import (POLICIES, StoragePolicy)
from swift.common.request_helpers import get_sys_meta_prefix

from test import listen_zero, annotate_failure
from test.unit import patch_policies


@contextmanager
def save_globals():
    orig_http_connect = getattr(swift.container.server, 'http_connect',
                                None)
    try:
        yield True
    finally:
        swift.container.server.http_connect = orig_http_connect


@patch_policies
class TestContainerController(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(
            mkdtemp(), 'tmp_test_container_server_ContainerController')
        mkdirs(self.testdir)
        rmtree(self.testdir)
        mkdirs(os.path.join(self.testdir, 'sda1'))
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self.logger = debug_logger()
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false'},
            logger=self.logger)
        # some of the policy tests want at least two policies
        self.assertTrue(len(POLICIES) > 1)

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def _update_object_put_headers(self, req):
        """
        Override this method in test subclasses to test post upgrade
        behavior.
        """
        pass

    def _put_shard_range(self, shard_range):
        put_timestamp = shard_range.timestamp.internal
        headers = {'X-Backend-Record-Type': 'shard',
                   'X-Timestamp': put_timestamp}
        body = json.dumps([dict(shard_range)])
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers,
                            body=body)
        resp = req.get_response(self.controller)
        self.assertIn(resp.status_int, (201, 202))

    def _check_put_container_storage_policy(self, req, policy_index):
        resp = req.get_response(self.controller)
        self.assertEqual(201, resp.status_int)
        req = Request.blank(req.path, method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(str(policy_index),
                         resp.headers['X-Backend-Storage-Policy-Index'])

    def _assert_shard_ranges_equal(self, x, y):
        # ShardRange.__eq__ only compares lower and upper; here we generate
        # dict representations to compare all attributes
        self.assertEqual([dict(sr) for sr in x], [dict(sr) for sr in y])

    def test_creation(self):
        # later config should be extended to assert more config options
        replicator = container_server.ContainerController(
            {'node_timeout': '3.5'})
        self.assertEqual(replicator.node_timeout, 3.5)

    def test_get_and_validate_policy_index(self):
        # no policy is OK
        req = Request.blank('/sda1/p/a/container_default', method='PUT',
                            headers={'X-Timestamp': '0'})
        self._check_put_container_storage_policy(req, POLICIES.default.idx)

        # bogus policies
        for policy in ('nada', 999):
            req = Request.blank('/sda1/p/a/c_%s' % policy, method='PUT',
                                headers={
                                    'X-Timestamp': '0',
                                    'X-Backend-Storage-Policy-Index': policy
                                })
            resp = req.get_response(self.controller)
            self.assertEqual(400, resp.status_int)
            self.assertIn(b'invalid', resp.body.lower())

        # good policies
        for policy in POLICIES:
            req = Request.blank('/sda1/p/a/c_%s' % policy.name, method='PUT',
                                headers={
                                    'X-Timestamp': '0',
                                    'X-Backend-Storage-Policy-Index':
                                    policy.idx,
                                })
            self._check_put_container_storage_policy(req, policy.idx)

    def test_acl_container(self):
        # Ensure no acl by default
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0'})
        resp = req.get_response(self.controller)
        self.assertTrue(resp.status.startswith('201'))
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assertTrue(response.status.startswith('204'))
        self.assertNotIn('x-container-read', response.headers)
        self.assertNotIn('x-container-write', response.headers)
        # Ensure POSTing acls works
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '1', 'X-Container-Read': '.r:*',
                     'X-Container-Write': 'account:user'})
        resp = req.get_response(self.controller)
        self.assertTrue(resp.status.startswith('204'))
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assertTrue(response.status.startswith('204'))
        self.assertEqual(response.headers.get('x-container-read'), '.r:*')
        self.assertEqual(response.headers.get('x-container-write'),
                         'account:user')
        # Ensure we can clear acls on POST
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '3', 'X-Container-Read': '',
                     'X-Container-Write': ''})
        resp = req.get_response(self.controller)
        self.assertTrue(resp.status.startswith('204'))
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assertTrue(response.status.startswith('204'))
        self.assertNotIn('x-container-read', response.headers)
        self.assertNotIn('x-container-write', response.headers)
        # Ensure PUT acls works
        req = Request.blank(
            '/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '4', 'X-Container-Read': '.r:*',
                     'X-Container-Write': 'account:user'})
        resp = req.get_response(self.controller)
        self.assertTrue(resp.status.startswith('201'))
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assertTrue(response.status.startswith('204'))
        self.assertEqual(response.headers.get('x-container-read'), '.r:*')
        self.assertEqual(response.headers.get('x-container-write'),
                         'account:user')

    def _test_head(self, start, ts):
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        response = req.get_response(self.controller)
        self.assertEqual(response.status_int, 204)
        self.assertEqual(response.headers['x-container-bytes-used'], '0')
        self.assertEqual(response.headers['x-container-object-count'], '0')
        obj_put_request = Request.blank(
            '/sda1/p/a/c/o', method='PUT', headers={
                'x-timestamp': next(ts),
                'x-size': 42,
                'x-content-type': 'text/plain',
                'x-etag': 'x',
            })
        self._update_object_put_headers(obj_put_request)
        obj_put_resp = obj_put_request.get_response(self.controller)
        self.assertEqual(obj_put_resp.status_int // 100, 2)
        # re-issue HEAD request
        response = req.get_response(self.controller)
        self.assertEqual(response.status_int // 100, 2)
        self.assertEqual(response.headers['x-container-bytes-used'], '42')
        self.assertEqual(response.headers['x-container-object-count'], '1')
        # created at time...
        created_at_header = Timestamp(response.headers['x-timestamp'])
        self.assertEqual(response.headers['x-timestamp'],
                         created_at_header.normal)
        self.assertTrue(created_at_header >= start)
        self.assertEqual(response.headers['x-put-timestamp'],
                         Timestamp(start).normal)
        self.assertEqual(
            response.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(start)))

        # backend headers
        self.assertEqual(int(response.headers
                             ['X-Backend-Storage-Policy-Index']),
                         int(POLICIES.default))
        self.assertTrue(
            Timestamp(response.headers['x-backend-timestamp']) >= start)
        self.assertEqual(response.headers['x-backend-put-timestamp'],
                         Timestamp(start).internal)
        self.assertEqual(response.headers['x-backend-delete-timestamp'],
                         Timestamp(0).internal)
        self.assertEqual(response.headers['x-backend-status-changed-at'],
                         Timestamp(start).internal)

    def test_HEAD(self):
        start = int(time.time())
        ts = (Timestamp(t).internal for t in itertools.count(start))
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'x-timestamp': next(ts)})
        req.get_response(self.controller)
        self._test_head(Timestamp(start), ts)

    def test_HEAD_timestamp_with_offset(self):
        start = int(time.time())
        ts = (Timestamp(t, offset=1).internal for t in itertools.count(start))
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'x-timestamp': next(ts)})
        req.get_response(self.controller)
        self._test_head(Timestamp(start, offset=1), ts)

    def test_HEAD_not_found(self):
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(int(resp.headers['X-Backend-Storage-Policy-Index']),
                         0)
        self.assertEqual(resp.headers['x-backend-timestamp'],
                         Timestamp(0).internal)
        self.assertEqual(resp.headers['x-backend-put-timestamp'],
                         Timestamp(0).internal)
        self.assertEqual(resp.headers['x-backend-status-changed-at'],
                         Timestamp(0).internal)
        self.assertEqual(resp.headers['x-backend-delete-timestamp'],
                         Timestamp(0).internal)
        self.assertIsNone(resp.last_modified)

        for header in ('x-container-object-count', 'x-container-bytes-used',
                       'x-timestamp', 'x-put-timestamp'):
            self.assertIsNone(resp.headers[header])

    def test_deleted_headers(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        request_method_times = {
            'PUT': next(ts),
            'DELETE': next(ts),
        }
        # setup a deleted container
        for method in ('PUT', 'DELETE'):
            x_timestamp = request_method_times[method]
            req = Request.blank('/sda1/p/a/c', method=method,
                                headers={'x-timestamp': x_timestamp})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)

        for method in ('GET', 'HEAD'):
            req = Request.blank('/sda1/p/a/c', method=method)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 404)
            self.assertIsNone(resp.last_modified)
            # backend headers
            self.assertEqual(int(resp.headers[
                                 'X-Backend-Storage-Policy-Index']),
                             int(POLICIES.default))
            self.assertTrue(Timestamp(resp.headers['x-backend-timestamp']) >=
                            Timestamp(request_method_times['PUT']))
            self.assertEqual(resp.headers['x-backend-put-timestamp'],
                             request_method_times['PUT'])
            self.assertEqual(resp.headers['x-backend-delete-timestamp'],
                             request_method_times['DELETE'])
            self.assertEqual(resp.headers['x-backend-status-changed-at'],
                             request_method_times['DELETE'])
            for header in ('x-container-object-count',
                           'x-container-bytes-used', 'x-timestamp',
                           'x-put-timestamp'):
                self.assertIsNone(resp.headers[header])

    def test_HEAD_invalid_partition(self):
        req = Request.blank('/sda1/./a/c', environ={'REQUEST_METHOD': 'HEAD',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_HEAD_invalid_content_type(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'Accept': 'application/plain'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 406)

    def test_HEAD_invalid_accept(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'Accept': 'application/plain;q'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual(resp.body, b'')

    def test_HEAD_invalid_format(self):
        format = '%D1%BD%8A9'  # invalid UTF-8; should be %E1%BD%8A9 (E -> D)
        req = Request.blank(
            '/sda1/p/a/c?format=' + format,
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_OPTIONS(self):
        server_handler = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false'})
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0
        resp = server_handler.OPTIONS(req)
        self.assertEqual(200, resp.status_int)
        for verb in 'OPTIONS GET POST PUT DELETE HEAD REPLICATE'.split():
            self.assertTrue(
                verb in resp.headers['Allow'].split(', '))
        self.assertEqual(len(resp.headers['Allow'].split(', ')), 7)
        self.assertEqual(resp.headers['Server'],
                         (self.controller.server_type + '/' + swift_version))

    def test_insufficient_storage_mount_check_true(self):
        conf = {'devices': self.testdir, 'mount_check': 'true'}
        container_controller = container_server.ContainerController(conf)
        self.assertTrue(container_controller.mount_check)
        for method in container_controller.allowed_methods:
            if method == 'OPTIONS':
                continue
            path = '/sda1/p/'
            if method == 'REPLICATE':
                path += 'suff'
            else:
                path += 'a/c'
            req = Request.blank(path, method=method,
                                headers={'x-timestamp': '1'})
            with mock_check_drive() as mocks:
                try:
                    resp = req.get_response(container_controller)
                    self.assertEqual(resp.status_int, 507)
                    mocks['ismount'].return_value = True
                    resp = req.get_response(container_controller)
                    self.assertNotEqual(resp.status_int, 507)
                    # feel free to rip out this last assertion...
                    expected = 2 if method == 'PUT' else 4
                    self.assertEqual(resp.status_int // 100, expected)
                except AssertionError as e:
                    self.fail('%s for %s' % (e, method))

    def test_insufficient_storage_mount_check_false(self):
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        container_controller = container_server.ContainerController(conf)
        self.assertFalse(container_controller.mount_check)
        for method in container_controller.allowed_methods:
            if method == 'OPTIONS':
                continue
            path = '/sda1/p/'
            if method == 'REPLICATE':
                path += 'suff'
            else:
                path += 'a/c'
            req = Request.blank(path, method=method,
                                headers={'x-timestamp': '1'})
            with mock_check_drive() as mocks:
                try:
                    resp = req.get_response(container_controller)
                    self.assertEqual(resp.status_int, 507)
                    mocks['isdir'].return_value = True
                    resp = req.get_response(container_controller)
                    self.assertNotEqual(resp.status_int, 507)
                    # feel free to rip out this last assertion...
                    expected = 2 if method == 'PUT' else 4
                    self.assertEqual(resp.status_int // 100, expected)
                except AssertionError as e:
                    self.fail('%s for %s' % (e, method))

    def test_PUT(self):
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)

    def test_PUT_insufficient_space(self):
        conf = {'devices': self.testdir,
                'mount_check': 'false',
                'fallocate_reserve': '2%'}
        container_controller = container_server.ContainerController(conf)

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '1517617825.74832'})
        statvfs_result = posix.statvfs_result([
            4096,     # f_bsize
            4096,     # f_frsize
            2854907,  # f_blocks
            59000,    # f_bfree
            57000,    # f_bavail  (just under 2% free)
            1280000,  # f_files
            1266040,  # f_ffree,
            1266040,  # f_favail,
            4096,     # f_flag
            255,      # f_namemax
        ])
        with mock.patch('os.statvfs',
                        return_value=statvfs_result) as mock_statvfs:
            resp = req.get_response(container_controller)
        self.assertEqual(resp.status_int, 507)
        self.assertEqual(mock_statvfs.mock_calls,
                         [mock.call(os.path.join(self.testdir, 'sda1'))])

    def test_PUT_simulated_create_race(self):
        state = ['initial']

        from swift.container.backend import ContainerBroker as OrigCoBr

        class InterceptedCoBr(OrigCoBr):

            def __init__(self, *args, **kwargs):
                super(InterceptedCoBr, self).__init__(*args, **kwargs)
                if state[0] == 'initial':
                    # Do nothing initially
                    pass
                elif state[0] == 'race':
                    # Save the original db_file attribute value
                    self._saved_db_file = self.db_file
                    self._db_file += '.doesnotexist'

            def initialize(self, *args, **kwargs):
                if state[0] == 'initial':
                    # Do nothing initially
                    pass
                elif state[0] == 'race':
                    # Restore the original db_file attribute to get the race
                    # behavior
                    self._db_file = self._saved_db_file
                return super(InterceptedCoBr, self).initialize(*args, **kwargs)

        with mock.patch("swift.container.server.ContainerBroker",
                        InterceptedCoBr):
            req = Request.blank(
                '/sda1/p/a/c',
                environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
            state[0] = "race"
            req = Request.blank(
                '/sda1/p/a/c',
                environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 202)

    def test_PUT_obj_not_found(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '1', 'X-Size': '0',
                     'X-Content-Type': 'text/plain', 'X-ETag': 'e'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_PUT_good_policy_specified(self):
        policy = random.choice(list(POLICIES))
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', method='PUT',
                            headers={'X-Timestamp': Timestamp(1).internal,
                                     'X-Backend-Storage-Policy-Index':
                                     policy.idx})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

        # now make sure we read it back
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

    def test_PUT_no_policy_specified(self):
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(1).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(POLICIES.default.idx))

        # now make sure the default was used (pol 1)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(POLICIES.default.idx))

    def test_PUT_bad_policy_specified(self):
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(1).internal,
                                     'X-Backend-Storage-Policy-Index': 'nada'})
        resp = req.get_response(self.controller)
        # make sure we get bad response
        self.assertEqual(resp.status_int, 400)
        self.assertFalse('X-Backend-Storage-Policy-Index' in resp.headers)

    def test_PUT_no_policy_change(self):
        ts = (Timestamp(t).internal for t in itertools.count(time.time()))
        policy = random.choice(list(POLICIES))
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Index': policy.idx})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # make sure we get the right index back
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

        # now try to update w/o changing the policy
        for method in ('POST', 'PUT'):
            req = Request.blank('/sda1/p/a/c', method=method, headers={
                'X-Timestamp': next(ts),
                'X-Backend-Storage-Policy-Index': policy.idx
            })
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)
        # make sure we get the right index back
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

    def test_PUT_bad_policy_change(self):
        ts = (Timestamp(t).internal for t in itertools.count(time.time()))
        policy = random.choice(list(POLICIES))
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Index': policy.idx})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # make sure we get the right index back
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

        other_policies = [p for p in POLICIES if p != policy]
        for other_policy in other_policies:
            # now try to change it and make sure we get a conflict
            req = Request.blank('/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': next(ts),
                'X-Backend-Storage-Policy-Index': other_policy.idx
            })
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 409)
            self.assertEqual(
                resp.headers.get('X-Backend-Storage-Policy-Index'),
                str(policy.idx))

        # and make sure there is no change!
        req = Request.blank('/sda1/p/a/c')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # make sure we get the right index back
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

    def test_POST_ignores_policy_change(self):
        ts = (Timestamp(t).internal for t in itertools.count(time.time()))
        policy = random.choice(list(POLICIES))
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Index': policy.idx})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # make sure we get the right index back
        self.assertEqual(resp.headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy.idx))

        other_policies = [p for p in POLICIES if p != policy]
        for other_policy in other_policies:
            # now try to change it and make sure we get a conflict
            req = Request.blank('/sda1/p/a/c', method='POST', headers={
                'X-Timestamp': next(ts),
                'X-Backend-Storage-Policy-Index': other_policy.idx
            })
            resp = req.get_response(self.controller)
            # valid request
            self.assertEqual(resp.status_int // 100, 2)

            # but it does nothing
            req = Request.blank('/sda1/p/a/c')
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 204)
            # make sure we get the right index back
            self.assertEqual(resp.headers.get
                             ('X-Backend-Storage-Policy-Index'),
                             str(policy.idx))

    def test_PUT_no_policy_for_existing_default(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        # create a container with the default storage policy
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity check

        # check the policy index
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Backend-Storage-Policy-Index'],
                         str(POLICIES.default.idx))

        # put again without specifying the storage policy
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)  # sanity check

        # policy index is unchanged
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Backend-Storage-Policy-Index'],
                         str(POLICIES.default.idx))

    def test_PUT_proxy_default_no_policy_for_existing_default(self):
        # make it look like the proxy has a different default than we do, like
        # during a config change restart across a multi node cluster.
        proxy_default = random.choice([p for p in POLICIES if not
                                       p.is_default])
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        # create a container with the default storage policy
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Default': int(proxy_default),
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity check

        # check the policy index
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(int(resp.headers['X-Backend-Storage-Policy-Index']),
                         int(proxy_default))

        # put again without proxy specifying the different default
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Default': int(POLICIES.default),
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)  # sanity check

        # policy index is unchanged
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(int(resp.headers['X-Backend-Storage-Policy-Index']),
                         int(proxy_default))

    def test_PUT_no_policy_for_existing_non_default(self):
        ts = (Timestamp(t).internal for t in itertools.count(time.time()))
        non_default_policy = [p for p in POLICIES if not p.is_default][0]
        # create a container with the non-default storage policy
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Index': non_default_policy.idx,
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity check

        # check the policy index
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Backend-Storage-Policy-Index'],
                         str(non_default_policy.idx))

        # put again without specifying the storage policy
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)  # sanity check

        # policy index is unchanged
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Backend-Storage-Policy-Index'],
                         str(non_default_policy.idx))

    def test_PUT_non_utf8_metadata(self):
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Container-Meta-Test': b'\xff'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)
        # Set sysmeta header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Container-Sysmeta-Test': b'\xff'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)
        # Set ACL
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Container-Read': b'\xff'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)
        # Send other
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Will-Not-Be-Saved': b'\xff'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)

    def test_PUT_GET_metadata(self):
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Container-Meta-Test': 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Container-Meta-Test2': 'Value2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'), 'Value')
        self.assertEqual(resp.headers.get('x-container-meta-test2'), 'Value2')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(3).internal,
                     'X-Container-Meta-Test': 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'),
                         'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(2).internal,
                     'X-Container-Meta-Test': 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'),
                         'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(4).internal,
                     'X-Container-Meta-Test': ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertNotIn('x-container-meta-test', resp.headers)

    def test_PUT_GET_sys_metadata(self):
        prefix = get_sys_meta_prefix('container')
        key = '%sTest' % prefix
        key2 = '%sTest2' % prefix
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(1).internal,
                                     key: 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': Timestamp(1).internal,
                                     key2: 'Value2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()), 'Value')
        self.assertEqual(resp.headers.get(key2.lower()), 'Value2')
        # Update metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(3).internal,
                                     key: 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()),
                         'New Value')
        # Send old update to metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(2).internal,
                                     key: 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()),
                         'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(4).internal,
                                     key: ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertNotIn(key.lower(), resp.headers)

    def test_PUT_invalid_partition(self):
        req = Request.blank('/sda1/./a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_HEAD_metadata(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Container-Meta-Test': 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'), 'Value')
        self.assertEqual(resp.headers.get('x-put-timestamp'),
                         '0000000001.00000')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': Timestamp(3).internal,
                     'X-Container-Meta-Test': 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'),
                         'New Value')
        self.assertEqual(resp.headers.get('x-put-timestamp'),
                         '0000000003.00000')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': Timestamp(2).internal,
                     'X-Container-Meta-Test': 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-container-meta-test'),
                         'New Value')
        self.assertEqual(resp.headers.get('x-put-timestamp'),
                         '0000000003.00000')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': Timestamp(4).internal,
                     'X-Container-Meta-Test': ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertNotIn('x-container-meta-test', resp.headers)
        self.assertEqual(resp.headers.get('x-put-timestamp'),
                         '0000000004.00000')

    def test_POST_HEAD_sys_metadata(self):
        prefix = get_sys_meta_prefix('container')
        key = '%sTest' % prefix
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': Timestamp(1).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': Timestamp(1).internal,
                                     key: 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()), 'Value')
        # Update metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': Timestamp(3).internal,
                                     key: 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()),
                         'New Value')
        # Send old update to metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': Timestamp(2).internal,
                                     key: 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(key.lower()),
                         'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': Timestamp(4).internal,
                                     key: ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertNotIn(key.lower(), resp.headers)

    def test_POST_invalid_partition(self):
        req = Request.blank('/sda1/./a/c', environ={'REQUEST_METHOD': 'POST',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_insufficient_space(self):
        conf = {'devices': self.testdir,
                'mount_check': 'false',
                'fallocate_reserve': '2%'}
        container_controller = container_server.ContainerController(conf)

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '1517618035.469202'})
        statvfs_result = posix.statvfs_result([
            4096,     # f_bsize
            4096,     # f_frsize
            2854907,  # f_blocks
            59000,    # f_bfree
            57000,    # f_bavail  (just under 2% free)
            1280000,  # f_files
            1266040,  # f_ffree,
            1266040,  # f_favail,
            4096,     # f_flag
            255,      # f_namemax
        ])
        with mock.patch('os.statvfs',
                        return_value=statvfs_result) as mock_statvfs:
            resp = req.get_response(container_controller)
        self.assertEqual(resp.status_int, 507)
        self.assertEqual(mock_statvfs.mock_calls,
                         [mock.call(os.path.join(self.testdir, 'sda1'))])

    def test_POST_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_invalid_container_sync_to(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'POST',
                                        'HTTP_X_TIMESTAMP': '1'},
            headers={'x-container-sync-to': '192.168.0.1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_after_DELETE_not_found(self):
        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c/',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': '3'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_DELETE_obj_not_found(self):
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_DELETE_container_not_found(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'DELETE',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_PUT_utf8(self):
        snowman = u'\u2603'
        container_name = snowman.encode('utf-8')
        req = Request.blank(
            '/sda1/p/a/%s' % container_name,
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

    def test_account_update_mismatched_host_device(self):
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_X_TIMESTAMP': '1'},
            headers={'X-Timestamp': '0000000001.00000',
                     'X-Account-Host': '127.0.0.1:0',
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1,sda2'})
        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        resp = self.controller.account_update(req, 'a', 'c', broker)
        self.assertEqual(resp.status_int, 400)

    def test_account_update_account_override_deleted(self):
        bindsock = listen_zero()
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_X_TIMESTAMP': '1'},
            headers={'X-Timestamp': '0000000001.00000',
                     'X-Account-Host': '%s:%s' %
                     bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1',
                     'X-Account-Override-Deleted': 'yes'})
        with save_globals():
            new_connect = fake_http_connect(200, count=123)
            swift.container.server.http_connect = new_connect
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)

    def test_PUT_account_update(self):
        bindsock = listen_zero()

        def accept(return_code, expected_timestamp):
            if not isinstance(expected_timestamp, bytes):
                expected_timestamp = expected_timestamp.encode('ascii')
            try:
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write(b'HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     b'PUT /sda1/123/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0].lower()] = \
                            line.split(b':')[1].strip()
                        line = inc.readline()
                    self.assertEqual(headers[b'x-put-timestamp'],
                                     expected_timestamp)
            except BaseException as err:
                return err
            return None

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(1).internal,
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 201, Timestamp(1).internal)
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEqual(resp.status_int, 201)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(3).internal,
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 404, Timestamp(3).internal)
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEqual(resp.status_int, 404)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': Timestamp(5).internal,
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 503, Timestamp(5).internal)
        got_exc = False
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
        except BaseException as err:
            got_exc = True
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        self.assertTrue(not got_exc)

    def test_PUT_reset_container_sync(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], -1)
        self.assertEqual(info['x_container_sync_point2'], -1)
        db.set_x_container_sync_points(123, 456)
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], 123)
        self.assertEqual(info['x_container_sync_point2'], 456)
        # Set to same value
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], 123)
        self.assertEqual(info['x_container_sync_point2'], 456)
        # Set to new value
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], -1)
        self.assertEqual(info['x_container_sync_point2'], -1)

    def test_POST_reset_container_sync(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], -1)
        self.assertEqual(info['x_container_sync_point2'], -1)
        db.set_x_container_sync_points(123, 456)
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], 123)
        self.assertEqual(info['x_container_sync_point2'], 456)
        # Set to same value
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], 123)
        self.assertEqual(info['x_container_sync_point2'], 456)
        # Set to new value
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEqual(info['x_container_sync_point1'], -1)
        self.assertEqual(info['x_container_sync_point2'], -1)

    def test_update_sync_store_on_PUT(self):
        # Create a synced container and validate a link is created
        self._create_synced_container_and_validate_sync_store('PUT')
        # remove the sync using PUT and validate the link is deleted
        self._remove_sync_and_validate_sync_store('PUT')

    def test_update_sync_store_on_POST(self):
        # Create a container and validate a link is not created
        self._create_container_and_validate_sync_store()
        # Update the container to be synced and validate a link is created
        self._create_synced_container_and_validate_sync_store('POST')
        # remove the sync using POST and validate the link is deleted
        self._remove_sync_and_validate_sync_store('POST')

    def test_update_sync_store_on_DELETE(self):
        # Create a synced container and validate a link is created
        self._create_synced_container_and_validate_sync_store('PUT')
        # Remove the container and validate the link is deleted
        self._remove_sync_and_validate_sync_store('DELETE')

    def _create_container_and_validate_sync_store(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '0'})
        req.get_response(self.controller)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        sync_store = self.controller.sync_store
        db_path = db.db_file
        db_link = sync_store._container_to_synced_container_path(db_path)
        self.assertFalse(os.path.exists(db_link))
        sync_containers = [c for c in sync_store.synced_containers_generator()]
        self.assertFalse(sync_containers)

    def _create_synced_container_and_validate_sync_store(self, method):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': method},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c',
                     'x-container-sync-key': '1234'})
        req.get_response(self.controller)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        sync_store = self.controller.sync_store
        db_path = db.db_file
        db_link = sync_store._container_to_synced_container_path(db_path)
        self.assertTrue(os.path.exists(db_link))
        sync_containers = [c for c in sync_store.synced_containers_generator()]
        self.assertEqual(1, len(sync_containers))
        self.assertEqual(db_path, sync_containers[0])

    def _remove_sync_and_validate_sync_store(self, method):
        if method == 'DELETE':
            headers = {'x-timestamp': '2'}
        else:
            headers = {'x-timestamp': '2',
                       'x-container-sync-to': '',
                       'x-container-sync-key': '1234'}

        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': method},
            headers=headers)
        req.get_response(self.controller)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        sync_store = self.controller.sync_store
        db_path = db.db_file
        db_link = sync_store._container_to_synced_container_path(db_path)
        self.assertFalse(os.path.exists(db_link))
        sync_containers = [c for c in sync_store.synced_containers_generator()]
        self.assertFalse(sync_containers)

    def test_REPLICATE_rsync_then_merge_works(self):
        def fake_rsync_then_merge(self, drive, db_file, args):
            return HTTPNoContent()

        with mock.patch("swift.container.replicator.ContainerReplicatorRpc."
                        "rsync_then_merge", fake_rsync_then_merge):
            req = Request.blank('/sda1/p/a/',
                                environ={'REQUEST_METHOD': 'REPLICATE'},
                                headers={})
            json_string = b'["rsync_then_merge", "a.db"]'
            inbuf = WsgiBytesIO(json_string)
            req.environ['wsgi.input'] = inbuf
            resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)

    def test_REPLICATE_complete_rsync_works(self):
        def fake_complete_rsync(self, drive, db_file, args):
            return HTTPNoContent()
        with mock.patch("swift.container.replicator.ContainerReplicatorRpc."
                        "complete_rsync", fake_complete_rsync):
            req = Request.blank('/sda1/p/a/',
                                environ={'REQUEST_METHOD': 'REPLICATE'},
                                headers={})
            json_string = b'["complete_rsync", "a.db"]'
            inbuf = WsgiBytesIO(json_string)
            req.environ['wsgi.input'] = inbuf
            resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)

    def test_REPLICATE_value_error_works(self):
        req = Request.blank('/sda1/p/a/',
                            environ={'REQUEST_METHOD': 'REPLICATE'},
                            headers={})
        # check valuerror
        wsgi_input_valuerror = b'["sync" : sync, "-1"]'
        inbuf1 = WsgiBytesIO(wsgi_input_valuerror)
        req.environ['wsgi.input'] = inbuf1
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_REPLICATE_unknown_sync(self):
        # First without existing DB file
        req = Request.blank('/sda1/p/a/',
                            environ={'REQUEST_METHOD': 'REPLICATE'},
                            headers={})
        json_string = b'["unknown_sync", "a.db"]'
        inbuf = WsgiBytesIO(json_string)
        req.environ['wsgi.input'] = inbuf
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        mkdirs(os.path.join(self.testdir, 'sda1', 'containers', 'p', 'a', 'a'))
        db_file = os.path.join(self.testdir, 'sda1',
                               storage_directory('containers', 'p', 'a'),
                               'a' + '.db')
        open(db_file, 'w')
        req = Request.blank('/sda1/p/a/',
                            environ={'REQUEST_METHOD': 'REPLICATE'},
                            headers={})
        json_string = b'["unknown_sync", "a.db"]'
        inbuf = WsgiBytesIO(json_string)
        req.environ['wsgi.input'] = inbuf
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 500)

    def test_REPLICATE_insufficient_space(self):
        conf = {'devices': self.testdir,
                'mount_check': 'false',
                'fallocate_reserve': '2%'}
        container_controller = container_server.ContainerController(conf)

        req = Request.blank(
            '/sda1/p/a/',
            environ={'REQUEST_METHOD': 'REPLICATE'})
        statvfs_result = posix.statvfs_result([
            4096,     # f_bsize
            4096,     # f_frsize
            2854907,  # f_blocks
            59000,    # f_bfree
            57000,    # f_bavail  (just under 2% free)
            1280000,  # f_files
            1266040,  # f_ffree,
            1266040,  # f_favail,
            4096,     # f_flag
            255,      # f_namemax
        ])
        with mock.patch('os.statvfs',
                        return_value=statvfs_result) as mock_statvfs:
            resp = req.get_response(container_controller)
        self.assertEqual(resp.status_int, 507)
        self.assertEqual(mock_statvfs.mock_calls,
                         [mock.call(os.path.join(self.testdir, 'sda1'))])

    def test_DELETE(self):
        ts_iter = make_timestamp_iter()
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': next(ts_iter).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # PUT an *empty* shard range
        sr = ShardRange('.shards_a/c', next(ts_iter), 'l', 'u', 0, 0,
                        state=ShardRange.ACTIVE)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': next(ts_iter).internal,
                     'X-Backend-Record-Type': 'shard'},
            body=json.dumps([dict(sr)]))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': next(ts_iter).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': next(ts_iter).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': next(ts_iter).internal,
                     'X-Backend-Record-Type': 'shard'},
            params={'format': 'json'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        # the override-deleted header is ignored for object records
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': next(ts_iter).internal,
                     'X-Backend-Override-Deleted': 'true'},
            params={'format': 'json'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        # but override-deleted header makes shard ranges available after DELETE
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': next(ts_iter).internal,
                     'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Deleted': 'true'},
            params={'format': 'json'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual([dict(sr, last_modified=sr.timestamp.isoformat)],
                         json.loads(resp.body))
        self.assertIn('X-Backend-Record-Type', resp.headers)
        self.assertEqual('shard', resp.headers['X-Backend-Record-Type'])

        # ... unless the override header equates to False
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': next(ts_iter).internal,
                     'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Deleted': 'no'},
            params={'format': 'json'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('X-Backend-Record-Type', resp.headers)

        # ...or the db file is unlinked
        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        self.assertTrue(os.path.exists(broker.db_file))
        os.unlink(broker.db_file)
        self.assertFalse(os.path.exists(broker.db_file))
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': next(ts_iter).internal,
                     'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Deleted': 'true'},
            params={'format': 'json'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('X-Backend-Record-Type', resp.headers)

    def test_DELETE_PUT_recreate(self):
        path = '/sda1/p/a/c'
        req = Request.blank(path, method='PUT',
                            headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(path, method='DELETE',
                            headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank(path, method='GET')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)  # sanity
        # backend headers
        expectations = {
            'x-backend-put-timestamp': Timestamp(1).internal,
            'x-backend-delete-timestamp': Timestamp(2).internal,
            'x-backend-status-changed-at': Timestamp(2).internal,
        }
        for header, value in expectations.items():
            self.assertEqual(resp.headers[header], value,
                             'response header %s was %s not %s' % (
                                 header, resp.headers[header], value))
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        self.assertEqual(True, db.is_deleted())
        info = db.get_info()
        self.assertEqual(info['put_timestamp'], Timestamp('1').internal)
        self.assertEqual(info['delete_timestamp'], Timestamp('2').internal)
        self.assertEqual(info['status_changed_at'], Timestamp('2').internal)
        # recreate
        req = Request.blank(path, method='PUT',
                            headers={'X-Timestamp': '4'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        self.assertEqual(False, db.is_deleted())
        info = db.get_info()
        self.assertEqual(info['put_timestamp'], Timestamp('4').internal)
        self.assertEqual(info['delete_timestamp'], Timestamp('2').internal)
        self.assertEqual(info['status_changed_at'], Timestamp('4').internal)
        for method in ('GET', 'HEAD'):
            req = Request.blank(path)
            resp = req.get_response(self.controller)
            expectations = {
                'x-put-timestamp': Timestamp(4).normal,
                'x-backend-put-timestamp': Timestamp(4).internal,
                'x-backend-delete-timestamp': Timestamp(2).internal,
                'x-backend-status-changed-at': Timestamp(4).internal,
            }
            for header, expected in expectations.items():
                self.assertEqual(resp.headers[header], expected,
                                 'header %s was %s is not expected %s' % (
                                     header, resp.headers[header], expected))

    def test_DELETE_PUT_recreate_replication_race(self):
        path = '/sda1/p/a/c'
        # create a deleted db
        req = Request.blank(path, method='PUT',
                            headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        req = Request.blank(path, method='DELETE',
                            headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank(path, method='GET')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)  # sanity
        self.assertEqual(True, db.is_deleted())
        # now save a copy of this db (and remove it from the "current node")
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        db_path = db._db_file
        other_path = os.path.join(self.testdir, 'othernode.db')
        os.rename(db_path, other_path)
        # that should make it missing on this node
        req = Request.blank(path, method='GET')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)  # sanity

        # setup the race in os.path.exists (first time no, then yes)
        mock_called = []
        _real_exists = os.path.exists

        def mock_exists(db_path):
            rv = _real_exists(db_path)
            if db_path != db._db_file:
                return rv
            if not mock_called:
                # be as careful as we might hope backend replication can be...
                with lock_parent_directory(db_path, timeout=1):
                    os.rename(other_path, db_path)
            mock_called.append((rv, db_path))
            return rv

        req = Request.blank(path, method='PUT',
                            headers={'X-Timestamp': '4'})
        with mock.patch.object(container_server.os.path, 'exists',
                               mock_exists):
            resp = req.get_response(self.controller)
        # db was successfully created
        self.assertEqual(resp.status_int // 100, 2)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        self.assertEqual(False, db.is_deleted())
        # mock proves the race
        self.assertEqual(mock_called[:2],
                         [(exists, db.db_file) for exists in (False, True)])
        # info was updated
        info = db.get_info()
        self.assertEqual(info['put_timestamp'], Timestamp('4').internal)
        self.assertEqual(info['delete_timestamp'], Timestamp('2').internal)

    def test_DELETE_not_found(self):
        # Even if the container wasn't previously heard of, the container
        # server will accept the delete and replicate it to where it belongs
        # later.
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE', 'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_change_storage_policy_via_DELETE_then_PUT(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        policy = random.choice(list(POLICIES))
        req = Request.blank(
            '/sda1/p/a/c', method='PUT',
            headers={'X-Timestamp': next(ts),
                     'X-Backend-Storage-Policy-Index': policy.idx})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity check

        # try re-recreate with other policies
        other_policies = [p for p in POLICIES if p != policy]
        for other_policy in other_policies:
            # first delete the existing container
            req = Request.blank('/sda1/p/a/c', method='DELETE', headers={
                'X-Timestamp': next(ts)})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 204)  # sanity check

            # at this point, the DB should still exist but be in a deleted
            # state, so changing the policy index is perfectly acceptable
            req = Request.blank('/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': next(ts),
                'X-Backend-Storage-Policy-Index': other_policy.idx})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)  # sanity check

            req = Request.blank(
                '/sda1/p/a/c', method='HEAD')
            resp = req.get_response(self.controller)
            self.assertEqual(resp.headers['X-Backend-Storage-Policy-Index'],
                             str(other_policy.idx))

    def test_change_to_default_storage_policy_via_DELETE_then_PUT(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        non_default_policy = random.choice([p for p in POLICIES
                                            if not p.is_default])
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts),
            'X-Backend-Storage-Policy-Index': non_default_policy.idx,
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity check

        req = Request.blank(
            '/sda1/p/a/c', method='DELETE',
            headers={'X-Timestamp': next(ts)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)  # sanity check

        # at this point, the DB should still exist but be in a deleted state,
        # so changing the policy index is perfectly acceptable
        req = Request.blank(
            '/sda1/p/a/c', method='PUT',
            headers={'X-Timestamp': next(ts)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity check

        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.headers['X-Backend-Storage-Policy-Index'],
                         str(POLICIES.default.idx))

    def test_DELETE_object(self):
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': Timestamp(2).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/o', method='PUT', headers={
                'X-Timestamp': Timestamp(0).internal, 'X-Size': 1,
                'X-Content-Type': 'text/plain', 'X-Etag': 'x'})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        ts = (Timestamp(t).internal for t in
              itertools.count(3))
        req = Request.blank('/sda1/p/a/c', method='DELETE', headers={
            'X-Timestamp': next(ts)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 409)
        req = Request.blank('/sda1/p/a/c/o', method='DELETE', headers={
            'X-Timestamp': next(ts)})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', method='DELETE', headers={
            'X-Timestamp': next(ts)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', method='GET', headers={
            'X-Timestamp': next(ts)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_object_update_with_offset(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        # create container
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': next(ts)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # check status
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(int(resp.headers['X-Backend-Storage-Policy-Index']),
                         int(POLICIES.default))
        # create object
        obj_timestamp = next(ts)
        req = Request.blank(
            '/sda1/p/a/c/o', method='PUT', headers={
                'X-Timestamp': obj_timestamp, 'X-Size': 1,
                'X-Content-Type': 'text/plain', 'X-Etag': 'x'})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # check listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 1)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 1)
            self.assertEqual(obj['hash'], 'x')
            self.assertEqual(obj['content_type'], 'text/plain')
        # send an update with an offset
        offset_timestamp = Timestamp(obj_timestamp, offset=1).internal
        req = Request.blank(
            '/sda1/p/a/c/o', method='PUT', headers={
                'X-Timestamp': offset_timestamp, 'X-Size': 2,
                'X-Content-Type': 'text/html', 'X-Etag': 'y'})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # check updated listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 2)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 2)
            self.assertEqual(obj['hash'], 'y')
            self.assertEqual(obj['content_type'], 'text/html')
        # now overwrite with a newer time
        delete_timestamp = next(ts)
        req = Request.blank(
            '/sda1/p/a/c/o', method='DELETE', headers={
                'X-Timestamp': delete_timestamp})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # check empty listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 0)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 0)
        listing_data = json.loads(resp.body)
        self.assertEqual(0, len(listing_data))
        # recreate with an offset
        offset_timestamp = Timestamp(delete_timestamp, offset=1).internal
        req = Request.blank(
            '/sda1/p/a/c/o', method='PUT', headers={
                'X-Timestamp': offset_timestamp, 'X-Size': 3,
                'X-Content-Type': 'text/enriched', 'X-Etag': 'z'})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # check un-deleted listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 3)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 3)
            self.assertEqual(obj['hash'], 'z')
            self.assertEqual(obj['content_type'], 'text/enriched')
        # delete offset with newer offset
        delete_timestamp = Timestamp(offset_timestamp, offset=1).internal
        req = Request.blank(
            '/sda1/p/a/c/o', method='DELETE', headers={
                'X-Timestamp': delete_timestamp})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # check empty listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 0)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 0)
        listing_data = json.loads(resp.body)
        self.assertEqual(0, len(listing_data))

    def test_object_update_with_multiple_timestamps(self):

        def do_update(t_data, etag, size, content_type,
                      t_type=None, t_meta=None):
            """
            Make a PUT request to container controller to update an object
            """
            headers = {'X-Timestamp': t_data.internal,
                       'X-Size': size,
                       'X-Content-Type': content_type,
                       'X-Etag': etag}
            if t_type:
                headers['X-Content-Type-Timestamp'] = t_type.internal
            if t_meta:
                headers['X-Meta-Timestamp'] = t_meta.internal
            req = Request.blank(
                '/sda1/p/a/c/o', method='PUT', headers=headers)
            self._update_object_put_headers(req)
            return req.get_response(self.controller)

        ts = (Timestamp(t) for t in itertools.count(int(time.time())))
        t0 = next(ts)

        # create container
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': t0.internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # check status
        req = Request.blank('/sda1/p/a/c', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)

        # create object at t1
        t1 = next(ts)
        resp = do_update(t1, 'etag_at_t1', 1, 'ctype_at_t1')
        self.assertEqual(resp.status_int, 201)

        # check listing, expect last_modified = t1
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 1)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 1)
            self.assertEqual(obj['hash'], 'etag_at_t1')
            self.assertEqual(obj['content_type'], 'ctype_at_t1')
            self.assertEqual(obj['last_modified'], t1.isoformat)

        # send an update with a content type timestamp at t4
        t2 = next(ts)
        t3 = next(ts)
        t4 = next(ts)
        resp = do_update(t1, 'etag_at_t1', 1, 'ctype_at_t4', t_type=t4)
        self.assertEqual(resp.status_int, 201)

        # check updated listing, expect last_modified = t4
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 1)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 1)
            self.assertEqual(obj['hash'], 'etag_at_t1')
            self.assertEqual(obj['content_type'], 'ctype_at_t4')
            self.assertEqual(obj['last_modified'], t4.isoformat)

        # now overwrite with an in-between data timestamp at t2
        resp = do_update(t2, 'etag_at_t2', 2, 'ctype_at_t2', t_type=t2)
        self.assertEqual(resp.status_int, 201)

        # check updated listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 2)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 2)
            self.assertEqual(obj['hash'], 'etag_at_t2')
            self.assertEqual(obj['content_type'], 'ctype_at_t4')
            self.assertEqual(obj['last_modified'], t4.isoformat)

        # now overwrite with an in-between content-type timestamp at t3
        resp = do_update(t2, 'etag_at_t2', 2, 'ctype_at_t3', t_type=t3)
        self.assertEqual(resp.status_int, 201)

        # check updated listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 2)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 2)
            self.assertEqual(obj['hash'], 'etag_at_t2')
            self.assertEqual(obj['content_type'], 'ctype_at_t4')
            self.assertEqual(obj['last_modified'], t4.isoformat)

        # now update with an in-between meta timestamp at t5
        t5 = next(ts)
        resp = do_update(t2, 'etag_at_t2', 2, 'ctype_at_t3', t_type=t3,
                         t_meta=t5)
        self.assertEqual(resp.status_int, 201)

        # check updated listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 2)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 2)
            self.assertEqual(obj['hash'], 'etag_at_t2')
            self.assertEqual(obj['content_type'], 'ctype_at_t4')
            self.assertEqual(obj['last_modified'], t5.isoformat)

        # delete object at t6
        t6 = next(ts)
        req = Request.blank(
            '/sda1/p/a/c/o', method='DELETE', headers={
                'X-Timestamp': t6.internal})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)

        # check empty listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 0)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 0)
        listing_data = json.loads(resp.body)
        self.assertEqual(0, len(listing_data))

        # subsequent content type timestamp at t8 should leave object deleted
        t7 = next(ts)
        t8 = next(ts)
        t9 = next(ts)
        resp = do_update(t2, 'etag_at_t2', 2, 'ctype_at_t8', t_type=t8,
                         t_meta=t9)
        self.assertEqual(resp.status_int, 201)

        # check empty listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 0)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 0)
        listing_data = json.loads(resp.body)
        self.assertEqual(0, len(listing_data))

        # object recreated at t7 should pick up existing, later content-type
        resp = do_update(t7, 'etag_at_t7', 7, 'ctype_at_t7')
        self.assertEqual(resp.status_int, 201)

        # check listing
        req = Request.blank('/sda1/p/a/c', method='GET',
                            query_string='format=json')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(int(resp.headers['X-Container-Object-Count']), 1)
        self.assertEqual(int(resp.headers['X-Container-Bytes-Used']), 7)
        listing_data = json.loads(resp.body)
        self.assertEqual(1, len(listing_data))
        for obj in listing_data:
            self.assertEqual(obj['name'], 'o')
            self.assertEqual(obj['bytes'], 7)
            self.assertEqual(obj['hash'], 'etag_at_t7')
            self.assertEqual(obj['content_type'], 'ctype_at_t8')
            self.assertEqual(obj['last_modified'], t9.isoformat)

    def test_DELETE_account_update(self):
        bindsock = listen_zero()

        def accept(return_code, expected_timestamp):
            if not isinstance(expected_timestamp, bytes):
                expected_timestamp = expected_timestamp.encode('ascii')
            try:
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write(b'HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEqual(inc.readline(),
                                     b'PUT /sda1/123/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0].lower()] = \
                            line.split(b':')[1].strip()
                        line = inc.readline()
                    self.assertEqual(headers[b'x-delete-timestamp'],
                                     expected_timestamp)
            except BaseException as err:
                import traceback
                traceback.print_exc()
                return err
            return None

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': Timestamp(2).internal,
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 204, Timestamp(2).internal)
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEqual(resp.status_int, 204)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': Timestamp(2).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': Timestamp(3).internal,
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 404, Timestamp(3).internal)
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEqual(resp.status_int, 404)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': Timestamp(4).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': Timestamp(5).internal,
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 503, Timestamp(5).internal)
        got_exc = False
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
        except BaseException as err:
            got_exc = True
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        self.assertTrue(not got_exc)

    def test_DELETE_invalid_partition(self):
        req = Request.blank(
            '/sda1/./a/c', environ={'REQUEST_METHOD': 'DELETE',
                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_DELETE_timestamp_not_float(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_GET_over_limit(self):
        req = Request.blank(
            '/sda1/p/a/c?limit=%d' %
            (constraints.CONTAINER_LISTING_LIMIT + 1),
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 412)

    def test_PUT_shard_range_autocreates_shard_container(self):
        ts_iter = make_timestamp_iter()
        shard_range = ShardRange('.shards_a/shard_c', next(ts_iter))
        put_timestamp = next(ts_iter).internal
        headers = {'X-Backend-Record-Type': 'shard',
                   'X-Timestamp': put_timestamp,
                   'X-Container-Sysmeta-Test': 'set',
                   'X-Container-Meta-Test': 'persisted'}

        # PUT shard range to non-existent container with non-autocreate prefix
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers,
                            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(404, resp.status_int)

        # PUT shard range to non-existent container with autocreate prefix,
        # missing storage policy
        headers['X-Timestamp'] = next(ts_iter).internal
        req = Request.blank(
            '/sda1/p/.shards_a/shard_c', method='PUT', headers=headers,
            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(400, resp.status_int)
        self.assertIn(b'X-Backend-Storage-Policy-Index header is required',
                      resp.body)

        # PUT shard range to non-existent container with autocreate prefix
        headers['X-Timestamp'] = next(ts_iter).internal
        policy_index = random.choice(POLICIES).idx
        headers['X-Backend-Storage-Policy-Index'] = str(policy_index)
        req = Request.blank(
            '/sda1/p/.shards_a/shard_c', method='PUT', headers=headers,
            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(201, resp.status_int)

        # repeat PUT of shard range to autocreated container - 204 response
        headers['X-Timestamp'] = next(ts_iter).internal
        headers.pop('X-Backend-Storage-Policy-Index')  # no longer required
        req = Request.blank(
            '/sda1/p/.shards_a/shard_c', method='PUT', headers=headers,
            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)

        # regular PUT to autocreated container - 204 response
        headers['X-Timestamp'] = next(ts_iter).internal
        req = Request.blank(
            '/sda1/p/.shards_a/shard_c', method='PUT',
            headers={'X-Timestamp': next(ts_iter).internal},
            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)

    def test_PUT_shard_range_to_deleted_container(self):
        ts_iter = make_timestamp_iter()
        put_time = next(ts_iter).internal
        # create a container, get it to sharded state and then delete it
        req = Request.blank('/sda1/p/a/c', method='PUT',
                            headers={'X-Timestamp': put_time})
        resp = req.get_response(self.controller)
        self.assertEqual(201, resp.status_int)

        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())

        delete_time = next(ts_iter).internal
        req = Request.blank('/sda1/p/a/c', method='DELETE',
                            headers={'X-Timestamp': delete_time})
        resp = req.get_response(self.controller)
        self.assertEqual(204, resp.status_int)
        self.assertTrue(broker.is_deleted())
        self.assertEqual(delete_time, broker.get_info()['delete_timestamp'])
        self.assertEqual(put_time, broker.get_info()['put_timestamp'])
        req = Request.blank('/sda1/p/a/c', method='GET')
        resp = req.get_response(self.controller)
        self.assertEqual(404, resp.status_int)

        # shard range PUT is accepted but container remains deleted
        shard_range = ShardRange('.shards_a/shard_c', next(ts_iter),
                                 state=ShardRange.ACTIVE)
        headers = {'X-Backend-Record-Type': 'shard',
                   'X-Timestamp': next(ts_iter).internal,
                   'X-Container-Sysmeta-Test': 'set',
                   'X-Container-Meta-Test': 'persisted'}

        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers,
                            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)
        self.assertTrue(broker.get_info_is_deleted()[1])
        self.assertEqual(delete_time, broker.get_info()['delete_timestamp'])
        self.assertEqual(put_time, broker.get_info()['put_timestamp'])
        req = Request.blank('/sda1/p/a/c', method='GET')
        resp = req.get_response(self.controller)
        self.assertEqual(404, resp.status_int)

        # unless shard range has non-zero stats, then container is revived
        shard_range.update_meta(99, 1234, meta_timestamp=next(ts_iter))
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers,
                            body=json.dumps([dict(shard_range)]))
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)
        self.assertFalse(broker.get_info_is_deleted()[1])
        self.assertEqual(delete_time, broker.get_info()['delete_timestamp'])
        self.assertEqual(put_time, broker.get_info()['put_timestamp'])
        req = Request.blank('/sda1/p/a/c', method='GET')
        resp = req.get_response(self.controller)
        self.assertEqual(204, resp.status_int)
        self.assertEqual('99', resp.headers['X-Container-Object-Count'])

    def test_PUT_shard_range_json_in_body(self):
        ts_iter = make_timestamp_iter()
        oldest_ts = next(ts_iter)  # used for stale shard range PUT later
        shard_bounds = [('', 'ham', ShardRange.ACTIVE),
                        ('ham', 'salami', ShardRange.ACTIVE),
                        ('salami', '', ShardRange.CREATED)]
        shard_ranges = [
            ShardRange('.shards_a/_%s' % upper, next(ts_iter),
                       lower, upper,
                       i * 100, i * 1000, meta_timestamp=next(ts_iter),
                       state=state, state_timestamp=next(ts_iter))
            for i, (lower, upper, state) in enumerate(shard_bounds)]

        put_timestamp = next(ts_iter).internal
        headers = {'X-Backend-Record-Type': 'shard',
                   'X-Timestamp': put_timestamp,
                   'X-Container-Sysmeta-Test': 'set',
                   'X-Container-Meta-Test': 'persisted'}
        body = json.dumps([dict(sr) for sr in shard_ranges[:2]])

        # PUT some shard ranges to non-existent container
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers,
                            body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(404, resp.status_int)

        # create the container with a regular PUT
        req = Request.blank(
            '/sda1/p/a/c', method='PUT',
            headers={'X-Timestamp': put_timestamp}, body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(201, resp.status_int)

        # now we can PUT shard ranges
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers,
                            body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)

        # check broker
        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        # sysmeta and user meta is updated
        exp_meta = {'X-Container-Sysmeta-Test': 'set',
                    'X-Container-Meta-Test': 'persisted'}
        self.assertEqual(
            exp_meta, dict((k, v[0]) for k, v in broker.metadata.items()))
        self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])
        self._assert_shard_ranges_equal(shard_ranges[:2],
                                        broker.get_shard_ranges())

        # empty json dict
        body = json.dumps({})
        headers['X-Timestamp'] = next(ts_iter).internal
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)
        self.assertEqual(
            exp_meta, dict((k, v[0]) for k, v in broker.metadata.items()))
        self._assert_shard_ranges_equal(shard_ranges[:2],
                                        broker.get_shard_ranges())
        self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])

        older_ts = next(ts_iter)  # used for stale shard range PUT later
        # updated and new shard ranges
        shard_ranges[1].bytes_used += 100
        shard_ranges[1].meta_timestamp = next(ts_iter)
        body = json.dumps([dict(sr) for sr in shard_ranges[1:]])
        headers['X-Timestamp'] = next(ts_iter).internal
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)
        self.assertEqual(
            exp_meta, dict((k, v[0]) for k, v in broker.metadata.items()))
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())
        self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])

        # stale shard range
        stale_shard_range = shard_ranges[1].copy()
        stale_shard_range.bytes_used = 0
        stale_shard_range.object_count = 0
        stale_shard_range.meta_timestamp = older_ts
        stale_shard_range.state = ShardRange.CREATED
        stale_shard_range.state_timestamp = oldest_ts
        body = json.dumps([dict(stale_shard_range)])
        headers['X-Timestamp'] = next(ts_iter).internal
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)
        self.assertEqual(
            exp_meta, dict((k, v[0]) for k, v in broker.metadata.items()))
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())
        self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])

        # deleted shard range
        shard_ranges[0].deleted = 1
        shard_ranges[0].timestamp = next(ts_iter)
        body = json.dumps([dict(shard_ranges[0])])
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        resp = req.get_response(self.controller)
        self.assertEqual(202, resp.status_int)
        self.assertEqual(
            exp_meta, dict((k, v[0]) for k, v in broker.metadata.items()))
        self._assert_shard_ranges_equal(
            shard_ranges, broker.get_shard_ranges(include_deleted=True))
        self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])

        def check_bad_body(body):
            bad_put_timestamp = next(ts_iter).internal
            headers['X-Timestamp'] = bad_put_timestamp
            req = Request.blank(
                '/sda1/p/a/c', method='PUT', headers=headers, body=body)
            resp = req.get_response(self.controller)
            self.assertEqual(400, resp.status_int)
            self.assertIn(b'Invalid body', resp.body)
            self.assertEqual(
                exp_meta, dict((k, v[0]) for k, v in broker.metadata.items()))
            self._assert_shard_ranges_equal(
                shard_ranges, broker.get_shard_ranges(include_deleted=True))
            self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])

        check_bad_body('not json')
        check_bad_body('')
        check_bad_body('["not a shard range"]')
        check_bad_body('[[]]')
        bad_shard_range = dict(ShardRange('a/c', next(ts_iter)))
        bad_shard_range.pop('timestamp')
        check_bad_body(json.dumps([bad_shard_range]))

        def check_not_shard_record_type(headers):
            # body ignored
            body = json.dumps([dict(sr) for sr in shard_ranges])
            # note, regular PUT so put timestamp is updated
            put_timestamp = next(ts_iter).internal
            headers['X-Timestamp'] = put_timestamp
            req = Request.blank(
                '/sda1/p/a/c', method='PUT', headers=headers, body=body)
            resp = req.get_response(self.controller)
            self.assertEqual(202, resp.status_int)
            self._assert_shard_ranges_equal(
                shard_ranges, broker.get_shard_ranges(include_deleted=True))
            self.assertEqual(put_timestamp, broker.get_info()['put_timestamp'])

        check_not_shard_record_type({'X-Backend-Record-Type': 'object',
                                     'X-Timestamp': next(ts_iter).internal})

        check_not_shard_record_type({'X-Timestamp': next(ts_iter).internal})

    def test_PUT_GET_shard_ranges(self):
        # make a container
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()  # used when mocking Timestamp.now()
        headers = {'X-Timestamp': next(ts_iter).normal}
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers)
        self.assertEqual(201, req.get_response(self.controller).status_int)
        # PUT some objects
        objects = [{'name': 'obj_%d' % i,
                    'x-timestamp': next(ts_iter).normal,
                    'x-content-type': 'text/plain',
                    'x-etag': 'etag_%d' % i,
                    'x-size': 1024 * i
                    } for i in range(2)]
        for obj in objects:
            req = Request.blank('/sda1/p/a/c/%s' % obj['name'], method='PUT',
                                headers=obj)
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(201, resp.status_int)
        # PUT some shard ranges
        shard_bounds = [('', 'apple', ShardRange.SHRINKING),
                        ('apple', 'ham', ShardRange.CLEAVED),
                        ('ham', 'salami', ShardRange.ACTIVE),
                        ('salami', 'yoghurt', ShardRange.CREATED),
                        ('yoghurt', '', ShardRange.FOUND),
                        ]
        shard_ranges = [
            ShardRange('.sharded_a/_%s' % upper, next(ts_iter),
                       lower, upper,
                       i * 100, i * 1000, meta_timestamp=next(ts_iter),
                       state=state, state_timestamp=next(ts_iter))
            for i, (lower, upper, state) in enumerate(shard_bounds)]
        for shard_range in shard_ranges:
            self._put_shard_range(shard_range)

        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        self.assertTrue(broker.is_root_container())  # sanity
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())

        # sanity check - no shard ranges when GET is only for objects
        def check_object_GET(path):
            req = Request.blank(path, method='GET')
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            expected = [
                dict(hash=obj['x-etag'], bytes=obj['x-size'],
                     content_type=obj['x-content-type'],
                     last_modified=Timestamp(obj['x-timestamp']).isoformat,
                     name=obj['name']) for obj in objects]
            self.assertEqual(expected, json.loads(resp.body))
            self.assertIn('X-Backend-Record-Type', resp.headers)
            self.assertEqual('object', resp.headers['X-Backend-Record-Type'])

        check_object_GET('/sda1/p/a/c?format=json')

        # GET only shard ranges
        def check_shard_GET(expected_shard_ranges, path, params=''):
            req = Request.blank('/sda1/p/%s?format=json%s' %
                                (path, params), method='GET',
                                headers={'X-Backend-Record-Type': 'shard'})
            with mock_timestamp_now(ts_now):
                resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            expected = [
                dict(sr, last_modified=Timestamp(sr.timestamp).isoformat)
                for sr in expected_shard_ranges]
            self.assertEqual(expected, json.loads(resp.body))
            self.assertIn('X-Backend-Record-Type', resp.headers)
            self.assertEqual('shard', resp.headers['X-Backend-Record-Type'])

        # all shards
        check_shard_GET(shard_ranges, 'a/c')
        check_shard_GET(reversed(shard_ranges), 'a/c', params='&reverse=true')
        # only created shards
        check_shard_GET(shard_ranges[3:4], 'a/c', params='&states=created')
        # only found shards
        check_shard_GET(shard_ranges[4:5], 'a/c', params='&states=found')
        # only cleaved shards
        check_shard_GET(shard_ranges[1:2], 'a/c',
                        params='&states=cleaved')
        # only active shards
        check_shard_GET(shard_ranges[2:3], 'a/c',
                        params='&states=active&end_marker=pickle')
        # only cleaved or active shards, reversed
        check_shard_GET(
            reversed(shard_ranges[1:3]), 'a/c',
            params='&states=cleaved,active&reverse=true&marker=pickle')
        # only shrinking shards
        check_shard_GET(shard_ranges[:1], 'a/c',
                        params='&states=shrinking&end_marker=pickle')
        check_shard_GET(shard_ranges[:1], 'a/c',
                        params='&states=shrinking&reverse=true&marker=pickle')
        # only active or shrinking shards
        check_shard_GET([shard_ranges[0], shard_ranges[2]], 'a/c',
                        params='&states=shrinking,active&end_marker=pickle')
        check_shard_GET(
            [shard_ranges[2], shard_ranges[0]], 'a/c',
            params='&states=active,shrinking&reverse=true&marker=pickle')
        # only active or shrinking shards using listing alias
        check_shard_GET(shard_ranges[:3], 'a/c',
                        params='&states=listing&end_marker=pickle')
        check_shard_GET(
            reversed(shard_ranges[:3]), 'a/c',
            params='&states=listing&reverse=true&marker=pickle')
        # only created, cleaved, active, shrinking shards using updating alias
        check_shard_GET(shard_ranges[1:4], 'a/c',
                        params='&states=updating&end_marker=treacle')
        check_shard_GET(
            reversed(shard_ranges[1:4]), 'a/c',
            params='&states=updating&reverse=true&marker=treacle')

        # listing shards don't cover entire namespace so expect an extra filler
        extra_shard_range = ShardRange(
            'a/c', ts_now, shard_ranges[2].upper, ShardRange.MAX, 2, 1024,
            state=ShardRange.ACTIVE)
        expected = shard_ranges[:3] + [extra_shard_range]
        check_shard_GET(expected, 'a/c', params='&states=listing')
        check_shard_GET(reversed(expected), 'a/c',
                        params='&states=listing&reverse=true')
        expected = [shard_ranges[2], extra_shard_range]
        check_shard_GET(expected, 'a/c',
                        params='&states=listing&marker=pickle')
        check_shard_GET(
            reversed(expected), 'a/c',
            params='&states=listing&reverse=true&end_marker=pickle')
        # updating shards don't cover entire namespace so expect a filler
        extra_shard_range = ShardRange(
            'a/c', ts_now, shard_ranges[3].upper, ShardRange.MAX, 2, 1024,
            state=ShardRange.ACTIVE)
        expected = shard_ranges[1:4] + [extra_shard_range]
        check_shard_GET(expected, 'a/c', params='&states=updating')
        check_shard_GET(reversed(expected), 'a/c',
                        params='&states=updating&reverse=true')
        # when no listing shard ranges cover the requested namespace range then
        # filler is for entire requested namespace
        extra_shard_range = ShardRange(
            'a/c', ts_now, 'treacle', ShardRange.MAX, 2, 1024,
            state=ShardRange.ACTIVE)
        check_shard_GET([extra_shard_range], 'a/c',
                        params='&states=listing&marker=treacle')
        check_shard_GET(
            [extra_shard_range], 'a/c',
            params='&states=listing&reverse=true&end_marker=treacle')
        extra_shard_range = ShardRange(
            'a/c', ts_now, 'treacle', 'walnut', 2, 1024,
            state=ShardRange.ACTIVE)
        params = '&states=listing&marker=treacle&end_marker=walnut'
        check_shard_GET([extra_shard_range], 'a/c', params=params)
        params = '&states=listing&reverse=true&marker=walnut' + \
                 '&end_marker=treacle'
        check_shard_GET([extra_shard_range], 'a/c', params=params)
        # specific object
        check_shard_GET(shard_ranges[1:2], 'a/c', params='&includes=cheese')
        check_shard_GET(shard_ranges[1:2], 'a/c', params='&includes=ham')
        check_shard_GET(shard_ranges[2:3], 'a/c', params='&includes=pickle')
        check_shard_GET(shard_ranges[2:3], 'a/c', params='&includes=salami')
        check_shard_GET(shard_ranges[3:4], 'a/c', params='&includes=walnut')
        check_shard_GET(shard_ranges[3:4], 'a/c',
                        params='&includes=walnut&reverse=true')
        # with marker
        check_shard_GET(shard_ranges[1:], 'a/c', params='&marker=cheese')
        check_shard_GET(reversed(shard_ranges[:2]), 'a/c',
                        params='&marker=cheese&reverse=true')
        check_shard_GET(shard_ranges[2:], 'a/c', params='&marker=ham')
        check_shard_GET(reversed(shard_ranges[:2]), 'a/c',
                        params='&marker=ham&reverse=true')
        check_shard_GET(shard_ranges[2:], 'a/c', params='&marker=pickle')
        check_shard_GET(reversed(shard_ranges[:3]), 'a/c',
                        params='&marker=pickle&reverse=true')
        check_shard_GET(shard_ranges[3:], 'a/c', params='&marker=salami')
        check_shard_GET(reversed(shard_ranges[:3]), 'a/c',
                        params='&marker=salami&reverse=true')
        check_shard_GET(shard_ranges[3:], 'a/c', params='&marker=walnut')
        check_shard_GET(reversed(shard_ranges[:4]), 'a/c',
                        params='&marker=walnut&reverse=true')
        # with end marker
        check_shard_GET(shard_ranges[:2], 'a/c', params='&end_marker=cheese')
        check_shard_GET(reversed(shard_ranges[1:]), 'a/c',
                        params='&end_marker=cheese&reverse=true')
        # everything in range 'apple' - 'ham' is <= end_marker of 'ham' so that
        # range is not included because end_marker is non-inclusive
        check_shard_GET(shard_ranges[:2], 'a/c', params='&end_marker=ham')
        check_shard_GET(reversed(shard_ranges[2:]), 'a/c',
                        params='&end_marker=ham&reverse=true')
        check_shard_GET(shard_ranges[:3], 'a/c', params='&end_marker=pickle')
        check_shard_GET(reversed(shard_ranges[2:]), 'a/c',
                        params='&end_marker=pickle&reverse=true')
        check_shard_GET(shard_ranges[:3], 'a/c', params='&end_marker=salami')
        check_shard_GET(reversed(shard_ranges[3:]), 'a/c',
                        params='&end_marker=salami&reverse=true')
        check_shard_GET(shard_ranges[:4], 'a/c', params='&end_marker=walnut')
        check_shard_GET(reversed(shard_ranges[3:]), 'a/c',
                        params='&end_marker=walnut&reverse=true')
        # with marker and end marker
        check_shard_GET(shard_ranges[1:2], 'a/c',
                        params='&marker=cheese&end_marker=egg')
        check_shard_GET(shard_ranges[1:2], 'a/c',
                        params='&end_marker=cheese&marker=egg&reverse=true')
        check_shard_GET(shard_ranges[1:3], 'a/c',
                        params='&marker=egg&end_marker=jam')
        check_shard_GET(reversed(shard_ranges[1:3]), 'a/c',
                        params='&end_marker=egg&marker=jam&reverse=true')
        check_shard_GET(shard_ranges[1:4], 'a/c',
                        params='&marker=cheese&end_marker=walnut')
        check_shard_GET(reversed(shard_ranges[1:4]), 'a/c',
                        params='&end_marker=cheese&marker=walnut&reverse=true')
        check_shard_GET(shard_ranges[2:4], 'a/c',
                        params='&marker=jam&end_marker=walnut')
        check_shard_GET(reversed(shard_ranges[2:4]), 'a/c',
                        params='&end_marker=jam&marker=walnut&reverse=true')
        check_shard_GET(shard_ranges[3:4], 'a/c',
                        params='&marker=toast&end_marker=walnut')
        check_shard_GET(shard_ranges[3:4], 'a/c',
                        params='&end_marker=toast&marker=walnut&reverse=true')
        check_shard_GET([], 'a/c',
                        params='&marker=egg&end_marker=cheese')
        check_shard_GET([], 'a/c',
                        params='&marker=cheese&end_marker=egg&reverse=true')

        # delete a shard range
        shard_range = shard_ranges[1]
        shard_range.set_deleted(timestamp=next(ts_iter))
        self._put_shard_range(shard_range)

        self._assert_shard_ranges_equal(shard_ranges[:1] + shard_ranges[2:],
                                        broker.get_shard_ranges())

        check_shard_GET(shard_ranges[:1] + shard_ranges[2:], 'a/c')
        check_shard_GET(shard_ranges[2:3], 'a/c', params='&includes=jam')
        # specify obj, marker or end_marker not in any shard range
        check_shard_GET([], 'a/c', params='&includes=cheese')
        check_shard_GET([], 'a/c', params='&includes=cheese&reverse=true')
        check_shard_GET([], 'a/c', params='&includes=ham')
        check_shard_GET(shard_ranges[2:], 'a/c/', params='&marker=cheese')
        check_shard_GET(shard_ranges[:1], 'a/c/',
                        params='&marker=cheese&reverse=true')
        check_shard_GET(shard_ranges[:1], 'a/c/', params='&end_marker=cheese')
        check_shard_GET(reversed(shard_ranges[2:]), 'a/c/',
                        params='&end_marker=cheese&reverse=true')

        self.assertFalse(self.controller.logger.get_lines_for_level('warning'))
        self.assertFalse(self.controller.logger.get_lines_for_level('error'))

    def test_GET_shard_ranges_using_state_aliases(self):
        # make a shard container
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()  # used when mocking Timestamp.now()
        shard_ranges = []
        lower = ''
        for state in sorted(ShardRange.STATES.keys()):
            upper = str(state)
            shard_ranges.append(
                ShardRange('.shards_a/c_%s' % upper, next(ts_iter),
                           lower, upper, state * 100, state * 1000,
                           meta_timestamp=next(ts_iter),
                           state=state, state_timestamp=next(ts_iter)))
            lower = upper

        def do_test(root_path, path, params, expected_states):
            expected = [
                sr for sr in shard_ranges if sr.state in expected_states]
            own_shard_range = ShardRange(path, next(ts_iter), '', '',
                                         state=ShardRange.ACTIVE)
            expected.append(own_shard_range.copy(
                lower=expected[-1].upper, meta_timestamp=ts_now))
            expected = [dict(sr, last_modified=sr.timestamp.isoformat)
                        for sr in expected]
            headers = {'X-Timestamp': next(ts_iter).normal}

            # create container
            req = Request.blank(
                '/sda1/p/%s' % path, method='PUT', headers=headers)
            self.assertIn(
                req.get_response(self.controller).status_int, (201, 202))
            # PUT some shard ranges
            headers = {'X-Timestamp': next(ts_iter).normal,
                       'X-Container-Sysmeta-Shard-Root': root_path,
                       'X-Backend-Record-Type': 'shard'}
            body = json.dumps(
                [dict(sr) for sr in shard_ranges + [own_shard_range]])
            req = Request.blank(
                '/sda1/p/%s' % path, method='PUT', headers=headers, body=body)
            self.assertEqual(202, req.get_response(self.controller).status_int)

            req = Request.blank('/sda1/p/%s?format=json%s' %
                                (path, params), method='GET',
                                headers={'X-Backend-Record-Type': 'shard'})
            with mock_timestamp_now(ts_now):
                resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            self.assertEqual(expected, json.loads(resp.body))
            self.assertIn('X-Backend-Record-Type', resp.headers)
            self.assertEqual('shard', resp.headers['X-Backend-Record-Type'])

        # root's shard ranges for listing
        root_path = container_path = 'a/c'
        params = '&states=listing'
        expected_states = [
            ShardRange.CLEAVED, ShardRange.ACTIVE, ShardRange.SHARDING,
            ShardRange.SHRINKING]
        do_test(root_path, container_path, params, expected_states)

        # shard's shard ranges for listing
        container_path = '.shards_a/c'
        params = '&states=listing'
        do_test(root_path, container_path, params, expected_states)

        # root's shard ranges for updating
        params = '&states=updating'
        expected_states = [
            ShardRange.CREATED, ShardRange.CLEAVED, ShardRange.ACTIVE,
            ShardRange.SHARDING]
        container_path = root_path
        do_test(root_path, container_path, params, expected_states)

        # shard's shard ranges for updating
        container_path = '.shards_a/c'
        do_test(root_path, container_path, params, expected_states)

    def test_GET_shard_ranges_include_deleted(self):
        # make a shard container
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()  # used when mocking Timestamp.now()
        shard_ranges = []
        lower = ''
        for state in sorted(ShardRange.STATES.keys()):
            upper = str(state)
            shard_ranges.append(
                ShardRange('.shards_a/c_%s' % upper, next(ts_iter),
                           lower, upper, state * 100, state * 1000,
                           meta_timestamp=next(ts_iter),
                           state=state, state_timestamp=next(ts_iter)))
            lower = upper
        # create container
        headers = {'X-Timestamp': next(ts_iter).normal}
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers)
        self.assertIn(
            req.get_response(self.controller).status_int, (201, 202))
        # PUT some shard ranges
        headers = {'X-Timestamp': next(ts_iter).normal,
                   'X-Backend-Record-Type': 'shard'}
        body = json.dumps([dict(sr) for sr in shard_ranges])
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        self.assertEqual(202, req.get_response(self.controller).status_int)

        def do_test(include_deleted, expected):
            expected = [dict(sr, last_modified=sr.timestamp.isoformat)
                        for sr in expected]
            headers = {'X-Backend-Record-Type': 'shard',
                       'X-Backend-Include-Deleted': str(include_deleted)}
            req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                                headers=headers)
            with mock_timestamp_now(ts_now):
                resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            self.assertEqual(expected, json.loads(resp.body))
            self.assertIn('X-Backend-Record-Type', resp.headers)
            self.assertEqual('shard', resp.headers['X-Backend-Record-Type'])

        do_test(False, shard_ranges)
        do_test(True, shard_ranges)

        headers = {'X-Timestamp': next(ts_iter).normal,
                   'X-Backend-Record-Type': 'shard'}
        for sr in shard_ranges[::2]:
            sr.set_deleted(timestamp=next(ts_iter))
        body = json.dumps([dict(sr) for sr in shard_ranges])
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        self.assertEqual(202, req.get_response(self.controller).status_int)
        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        self._assert_shard_ranges_equal(
            shard_ranges[1::2], broker.get_shard_ranges())
        do_test(False, shard_ranges[1::2])
        do_test(True, shard_ranges)

        headers = {'X-Timestamp': next(ts_iter).normal,
                   'X-Backend-Record-Type': 'shard'}
        for sr in shard_ranges[1::2]:
            sr.set_deleted(timestamp=next(ts_iter))
        body = json.dumps([dict(sr) for sr in shard_ranges])
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        self.assertEqual(202, req.get_response(self.controller).status_int)
        self.assertFalse(broker.get_shard_ranges())
        do_test(False, [])
        do_test(True, shard_ranges)

    def test_GET_shard_ranges_errors(self):
        # verify that x-backend-record-type is not included in error responses
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()  # used when mocking Timestamp.now()
        shard_ranges = []
        lower = ''
        for state in sorted(ShardRange.STATES.keys()):
            upper = str(state)
            shard_ranges.append(
                ShardRange('.shards_a/c_%s' % upper, next(ts_iter),
                           lower, upper, state * 100, state * 1000,
                           meta_timestamp=next(ts_iter),
                           state=state, state_timestamp=next(ts_iter)))
            lower = upper
        # create container
        headers = {'X-Timestamp': next(ts_iter).normal}
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers)
        self.assertIn(
            req.get_response(self.controller).status_int, (201, 202))
        # PUT some shard ranges
        headers = {'X-Timestamp': next(ts_iter).normal,
                   'X-Backend-Record-Type': 'shard'}
        body = json.dumps([dict(sr) for sr in shard_ranges])
        req = Request.blank(
            '/sda1/p/a/c', method='PUT', headers=headers, body=body)
        self.assertEqual(202, req.get_response(self.controller).status_int)

        def do_test(params):
            params['format'] = 'json'
            headers = {'X-Backend-Record-Type': 'shard'}
            req = Request.blank('/sda1/p/a/c', method='GET',
                                headers=headers, params=params)
            with mock_timestamp_now(ts_now):
                resp = req.get_response(self.controller)
            self.assertEqual(resp.content_type, 'text/html')
            self.assertNotIn('X-Backend-Record-Type', resp.headers)
            self.assertNotIn('X-Backend-Sharding-State', resp.headers)
            self.assertNotIn('X-Container-Object-Count', resp.headers)
            self.assertNotIn('X-Container-Bytes-Used', resp.headers)
            self.assertNotIn('X-Timestamp', resp.headers)
            self.assertNotIn('X-PUT-Timestamp', resp.headers)
            return resp

        resp = do_test({'states': 'bad'})
        self.assertEqual(resp.status_int, 400)
        resp = do_test({'delimiter': 'bad'})
        self.assertEqual(resp.status_int, 412)
        resp = do_test({'limit': str(constraints.CONTAINER_LISTING_LIMIT + 1)})
        self.assertEqual(resp.status_int, 412)
        with mock.patch('swift.container.server.check_drive',
                        side_effect=ValueError('sda1 is not mounted')):
            resp = do_test({})
        self.assertEqual(resp.status_int, 507)

        # delete the container
        req = Request.blank('/sda1/p/a/c', method='DELETE',
                            headers={'X-Timestamp': next(ts_iter).normal})
        self.assertEqual(204, req.get_response(self.controller).status_int)

        resp = do_test({'states': 'bad'})
        self.assertEqual(resp.status_int, 404)

    def test_GET_auto_record_type(self):
        # make a container
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()  # used when mocking Timestamp.now()
        headers = {'X-Timestamp': next(ts_iter).normal}
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers)
        self.assertEqual(201, req.get_response(self.controller).status_int)
        # PUT some objects
        objects = [{'name': 'obj_%d' % i,
                    'x-timestamp': next(ts_iter).normal,
                    'x-content-type': 'text/plain',
                    'x-etag': 'etag_%d' % i,
                    'x-size': 1024 * i
                    } for i in range(2)]
        for obj in objects:
            req = Request.blank('/sda1/p/a/c/%s' % obj['name'], method='PUT',
                                headers=obj)
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(201, resp.status_int)
        # PUT some shard ranges
        shard_bounds = [('', 'm', ShardRange.CLEAVED),
                        ('m', '', ShardRange.CREATED)]
        shard_ranges = [
            ShardRange('.sharded_a/_%s' % upper, next(ts_iter),
                       lower, upper,
                       i * 100, i * 1000, meta_timestamp=next(ts_iter),
                       state=state, state_timestamp=next(ts_iter))
            for i, (lower, upper, state) in enumerate(shard_bounds)]
        for shard_range in shard_ranges:
            self._put_shard_range(shard_range)

        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')

        def assert_GET_objects(req, expected_objects):
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            expected = [
                dict(hash=obj['x-etag'], bytes=obj['x-size'],
                     content_type=obj['x-content-type'],
                     last_modified=Timestamp(obj['x-timestamp']).isoformat,
                     name=obj['name']) for obj in expected_objects]
            self.assertEqual(expected, json.loads(resp.body))
            self.assertIn('X-Backend-Record-Type', resp.headers)
            self.assertEqual(
                'object', resp.headers.pop('X-Backend-Record-Type'))
            resp.headers.pop('Content-Length')
            return resp

        def assert_GET_shard_ranges(req, expected_shard_ranges):
            with mock_timestamp_now(ts_now):
                resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            expected = [
                dict(sr, last_modified=Timestamp(sr.timestamp).isoformat)
                for sr in expected_shard_ranges]
            self.assertEqual(expected, json.loads(resp.body))
            self.assertIn('X-Backend-Record-Type', resp.headers)
            self.assertEqual(
                'shard', resp.headers.pop('X-Backend-Record-Type'))
            resp.headers.pop('Content-Length')
            return resp

        # unsharded
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'auto'})
        resp = assert_GET_objects(req, objects)
        headers = resp.headers
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'shard'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'object'})
        resp = assert_GET_objects(req, objects)
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json', method='GET')
        resp = assert_GET_objects(req, objects)
        self.assertEqual(headers, resp.headers)

        # move to sharding state
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'auto'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        headers = resp.headers
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'shard'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'object'})
        resp = assert_GET_objects(req, objects)
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json', method='GET')
        resp = assert_GET_objects(req, objects)
        self.assertEqual(headers, resp.headers)

        # limit is applied to objects but not shard ranges
        req = Request.blank('/sda1/p/a/c?format=json&limit=1', method='GET',
                            headers={'X-Backend-Record-Type': 'auto'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        headers = resp.headers
        req = Request.blank('/sda1/p/a/c?format=json&limit=1', method='GET',
                            headers={'X-Backend-Record-Type': 'shard'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json&limit=1', method='GET',
                            headers={'X-Backend-Record-Type': 'object'})
        resp = assert_GET_objects(req, objects[:1])
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json&limit=1', method='GET')
        resp = assert_GET_objects(req, objects[:1])
        self.assertEqual(headers, resp.headers)

        # move to sharded state
        self.assertTrue(broker.set_sharded_state())
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'auto'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        headers = resp.headers
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'shard'})
        resp = assert_GET_shard_ranges(req, shard_ranges)
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json', method='GET',
                            headers={'X-Backend-Record-Type': 'object'})
        resp = assert_GET_objects(req, [])
        self.assertEqual(headers, resp.headers)
        req = Request.blank('/sda1/p/a/c?format=json', method='GET')
        resp = assert_GET_objects(req, [])
        self.assertEqual(headers, resp.headers)

    def test_PUT_GET_to_sharding_container(self):
        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        ts_iter = make_timestamp_iter()
        headers = {'X-Timestamp': next(ts_iter).normal}
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers)
        self.assertEqual(201, req.get_response(self.controller).status_int)

        def do_update(name, timestamp=None, headers=None):
            # Make a PUT request to container controller to update an object
            timestamp = timestamp or next(ts_iter)
            headers = headers or {}
            headers.update({'X-Timestamp': timestamp.internal,
                            'X-Size': 17,
                            'X-Content-Type': 'text/plain',
                            'X-Etag': 'fake etag'})
            req = Request.blank(
                '/sda1/p/a/c/%s' % name, method='PUT', headers=headers)
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(201, resp.status_int)

        def get_api_listing():
            req = Request.blank(
                '/sda1/p/a/c', method='GET', params={'format': 'json'})
            resp = req.get_response(self.controller)
            self.assertEqual(200, resp.status_int)
            return [obj['name'] for obj in json.loads(resp.body)]

        def assert_broker_rows(broker, expected_names, expected_max_row):
            self.assertEqual(expected_max_row, broker.get_max_row())
            with broker.get() as conn:
                curs = conn.execute('''
                    SELECT * FROM object WHERE ROWID > -1 ORDER BY ROWID ASC
                ''')
                actual = [r[1] for r in curs]

            self.assertEqual(expected_names, actual)

        do_update('unsharded')
        self.assertEqual(['unsharded'], get_api_listing())
        assert_broker_rows(broker, ['unsharded'], 1)

        # move container to sharding state
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        assert_broker_rows(broker.get_brokers()[0], ['unsharded'], 1)
        assert_broker_rows(broker.get_brokers()[1], [], 1)

        # add another update - should not merge into the older db and therefore
        # not appear in api listing
        do_update('sharding')
        self.assertEqual(['unsharded'], get_api_listing())
        assert_broker_rows(broker.get_brokers()[0], ['unsharded'], 1)
        assert_broker_rows(broker.get_brokers()[1], ['sharding'], 2)

        orig_lister = swift.container.backend.ContainerBroker.list_objects_iter

        def mock_list_objects_iter(*args, **kwargs):
            # cause an update to land in the pending file after it has been
            # flushed by get_info() calls in the container PUT method, but
            # before it is flushed by the call to list_objects_iter
            do_update('racing_update')
            return orig_lister(*args, **kwargs)

        with mock.patch(
                'swift.container.backend.ContainerBroker.list_objects_iter',
                mock_list_objects_iter):
            listing = get_api_listing()

        self.assertEqual(['unsharded'], listing)
        assert_broker_rows(broker.get_brokers()[0], ['unsharded'], 1)
        assert_broker_rows(broker.get_brokers()[1], ['sharding'], 2)

        # next listing will flush pending file
        listing = get_api_listing()
        self.assertEqual(['unsharded'], listing)
        assert_broker_rows(broker.get_brokers()[0], ['unsharded'], 1)
        assert_broker_rows(broker.get_brokers()[1],
                           ['sharding', 'racing_update'], 3)

    def _check_object_update_redirected_to_shard(self, method):
        expected_status = 204 if method == 'DELETE' else 201
        broker = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        ts_iter = make_timestamp_iter()
        headers = {'X-Timestamp': next(ts_iter).normal}
        req = Request.blank('/sda1/p/a/c', method='PUT', headers=headers)
        self.assertEqual(201, req.get_response(self.controller).status_int)

        def do_update(name, timestamp=None, headers=None):
            # Make a PUT request to container controller to update an object
            timestamp = timestamp or next(ts_iter)
            headers = headers or {}
            headers.update({'X-Timestamp': timestamp.internal,
                            'X-Size': 17,
                            'X-Content-Type': 'text/plain',
                            'X-Etag': 'fake etag'})
            req = Request.blank(
                '/sda1/p/a/c/%s' % name, method=method, headers=headers)
            self._update_object_put_headers(req)
            return req.get_response(self.controller)

        def get_listing(broker_index):
            # index -1 is always the freshest db
            sub_broker = broker.get_brokers()[broker_index]
            return sub_broker.get_objects()

        def assert_not_redirected(obj_name, timestamp=None, headers=None):
            resp = do_update(obj_name, timestamp=timestamp, headers=headers)
            self.assertEqual(expected_status, resp.status_int)
            self.assertNotIn('Location', resp.headers)
            self.assertNotIn('X-Backend-Redirect-Timestamp', resp.headers)

        def assert_redirected(obj_name, shard_range, headers=None):
            resp = do_update(obj_name, headers=headers)
            self.assertEqual(301, resp.status_int)
            self.assertEqual('/%s/%s' % (shard_range.name, obj_name),
                             resp.headers['Location'])
            self.assertEqual(shard_range.timestamp.internal,
                             resp.headers['X-Backend-Redirect-Timestamp'])

        # sanity check
        ts_bashful_orig = next(ts_iter)
        mocked_fn = 'swift.container.backend.ContainerBroker.get_shard_ranges'
        with mock.patch(mocked_fn) as mock_get_shard_ranges:
            assert_not_redirected('bashful', ts_bashful_orig)
        mock_get_shard_ranges.assert_not_called()

        shard_ranges = {
            'dopey': ShardRange(
                '.sharded_a/sr_dopey', next(ts_iter), '', 'dopey'),
            'happy': ShardRange(
                '.sharded_a/sr_happy', next(ts_iter), 'dopey', 'happy'),
            '': ShardRange('.sharded_a/sr_', next(ts_iter), 'happy', '')
        }
        # start with only the middle shard range
        self._put_shard_range(shard_ranges['happy'])

        # db not yet sharding but shard ranges exist
        sr_happy = shard_ranges['happy']
        redirect_states = (
            ShardRange.CREATED, ShardRange.CLEAVED, ShardRange.ACTIVE,
            ShardRange.SHARDING)
        headers = {'X-Backend-Accept-Redirect': 'true'}
        for state in ShardRange.STATES:
            self.assertTrue(
                sr_happy.update_state(state,
                                      state_timestamp=next(ts_iter)))
            self._put_shard_range(sr_happy)
            with annotate_failure(state):
                obj_name = 'grumpy%s' % state
                if state in redirect_states:
                    assert_redirected(obj_name, sr_happy, headers=headers)
                    self.assertNotIn(obj_name,
                                     [obj['name'] for obj in get_listing(-1)])
                else:
                    assert_not_redirected(obj_name, headers=headers)
                    self.assertIn(obj_name,
                                  [obj['name'] for obj in get_listing(-1)])
                obj_name = 'grumpy%s_no_header' % state
                with mock.patch(mocked_fn) as mock_get_shard_ranges:
                    assert_not_redirected(obj_name)
                mock_get_shard_ranges.assert_not_called()
                self.assertIn(obj_name,
                              [obj['name'] for obj in get_listing(-1)])

        # set broker to sharding state
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        for state in ShardRange.STATES:
            self.assertTrue(
                sr_happy.update_state(state,
                                      state_timestamp=next(ts_iter)))
            self._put_shard_range(sr_happy)
            with annotate_failure(state):
                obj_name = 'grumpier%s' % state
                if state in redirect_states:
                    assert_redirected(obj_name, sr_happy, headers=headers)
                    self.assertNotIn(obj_name,
                                     [obj['name'] for obj in get_listing(-1)])
                else:
                    assert_not_redirected(obj_name, headers=headers)
                    # update goes to fresh db, misplaced
                    self.assertIn(
                        obj_name, [obj['name'] for obj in get_listing(-1)])
                    self.assertNotIn(
                        obj_name, [obj['name'] for obj in get_listing(0)])
                obj_name = 'grumpier%s_no_header' % state
                with mock.patch(mocked_fn) as mock_get_shard_ranges:
                    assert_not_redirected(obj_name)
                mock_get_shard_ranges.assert_not_called()
                self.assertIn(
                    obj_name, [obj['name'] for obj in get_listing(-1)])
                # update is misplaced, not in retiring db
                self.assertNotIn(
                    obj_name, [obj['name'] for obj in get_listing(0)])

        # no shard for this object yet so it is accepted by root container
        # and stored in misplaced objects...
        assert_not_redirected('dopey', timestamp=next(ts_iter))
        self.assertIn('dopey', [obj['name'] for obj in get_listing(-1)])
        self.assertNotIn('dopey', [obj['name'] for obj in get_listing(0)])

        # now PUT the first shard range
        sr_dopey = shard_ranges['dopey']
        sr_dopey.update_state(ShardRange.CLEAVED,
                              state_timestamp=next(ts_iter))
        self._put_shard_range(sr_dopey)
        for state in ShardRange.STATES:
            self.assertTrue(
                sr_happy.update_state(state,
                                      state_timestamp=next(ts_iter)))
            self._put_shard_range(sr_happy)
            with annotate_failure(state):
                obj_name = 'dopey%s' % state
                if state in redirect_states:
                    assert_redirected(obj_name, sr_happy, headers=headers)
                    self.assertNotIn(obj_name,
                                     [obj['name'] for obj in get_listing(-1)])
                    self.assertNotIn(obj_name,
                                     [obj['name'] for obj in get_listing(0)])
                else:
                    assert_not_redirected(obj_name, headers=headers)
                    self.assertIn(obj_name,
                                  [obj['name'] for obj in get_listing(-1)])
                    self.assertNotIn(obj_name,
                                     [obj['name'] for obj in get_listing(0)])
                obj_name = 'dopey%s_no_header' % state
                with mock.patch(mocked_fn) as mock_get_shard_ranges:
                    assert_not_redirected(obj_name)
                mock_get_shard_ranges.assert_not_called()
                self.assertIn(obj_name,
                              [obj['name'] for obj in get_listing(-1)])
                self.assertNotIn(obj_name,
                                 [obj['name'] for obj in get_listing(0)])

        # further updates to bashful and dopey are now redirected...
        assert_redirected('bashful', sr_dopey, headers=headers)
        assert_redirected('dopey', sr_dopey, headers=headers)
        # ...and existing updates in this container are *not* updated
        self.assertEqual([ts_bashful_orig.internal],
                         [obj['created_at'] for obj in get_listing(0)
                          if obj['name'] == 'bashful'])

        # set broker to sharded state
        self.assertTrue(broker.set_sharded_state())
        for state in ShardRange.STATES:
            self.assertTrue(
                sr_happy.update_state(state,
                                      state_timestamp=next(ts_iter)))
            self._put_shard_range(sr_happy)
            with annotate_failure(state):
                obj_name = 'grumpiest%s' % state
                if state in redirect_states:
                    assert_redirected(obj_name, sr_happy, headers=headers)
                    self.assertNotIn(obj_name,
                                     [obj['name'] for obj in get_listing(-1)])
                else:
                    assert_not_redirected(obj_name, headers=headers)
                    self.assertIn(obj_name,
                                  [obj['name'] for obj in get_listing(-1)])
                obj_name = 'grumpiest%s_no_header' % state
                with mock.patch(mocked_fn) as mock_get_shard_ranges:
                    assert_not_redirected(obj_name)
                mock_get_shard_ranges.assert_not_called()
                self.assertIn(obj_name,
                              [obj['name'] for obj in get_listing(-1)])

    def test_PUT_object_update_redirected_to_shard(self):
        self._check_object_update_redirected_to_shard('PUT')

    def test_DELETE_object_update_redirected_to_shard(self):
        self._check_object_update_redirected_to_shard('DELETE')

    def test_GET_json(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/jsonc', environ={'REQUEST_METHOD': 'PUT',
                                        'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        # test an empty container
        req = Request.blank(
            '/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [])
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/jsonc/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        # test format
        json_body = [{"name": "0",
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.000000"},
                     {"name": "1",
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.000000"},
                     {"name": "2",
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.000000"}]

        req = Request.blank(
            '/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(
            resp.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(0)))
        self.assertEqual(json.loads(resp.body), json_body)
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')

        for accept in ('application/json', 'application/json;q=1.0,*/*;q=0.9',
                       '*/*;q=0.9,application/json;q=1.0', 'application/*'):
            req = Request.blank(
                '/sda1/p/a/jsonc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEqual(
                json.loads(resp.body), json_body,
                'Invalid body for Accept: %s' % accept)
            self.assertEqual(
                resp.content_type, 'application/json',
                'Invalid content_type for Accept: %s' % accept)

            req = Request.blank(
                '/sda1/p/a/jsonc',
                environ={'REQUEST_METHOD': 'HEAD'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEqual(
                resp.content_type, 'application/json',
                'Invalid content_type for Accept: %s' % accept)

    def test_GET_non_ascii(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/jsonc', environ={'REQUEST_METHOD': 'PUT',
                                        'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)

        noodles = [u"Spätzle", u"ラーメン"]
        for n in noodles:
            req = Request.blank(
                '/sda1/p/a/jsonc/%s' % bytes_to_wsgi(n.encode("utf-8")),
                environ={'REQUEST_METHOD': 'PUT',
                         'HTTP_X_TIMESTAMP': '1',
                         'HTTP_X_CONTENT_TYPE': 'text/plain',
                         'HTTP_X_ETAG': 'x',
                         'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)  # sanity check

        json_body = [{"name": noodles[0],
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.000000"},
                     {"name": noodles[1],
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.000000"}]

        # JSON
        req = Request.blank(
            '/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)  # sanity check
        self.assertEqual(json.loads(resp.body), json_body)

        # Plain text
        text_body = u''.join(n + u"\n" for n in noodles).encode('utf-8')
        req = Request.blank(
            '/sda1/p/a/jsonc?format=text',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)  # sanity check
        self.assertEqual(resp.body, text_body)

    def test_GET_plain(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/plainc', environ={'REQUEST_METHOD': 'PUT',
                                         'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        # test an empty container
        req = Request.blank(
            '/sda1/p/a/plainc', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/plainc/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        plain_body = b'0\n1\n2\n'

        req = Request.blank('/sda1/p/a/plainc',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(
            resp.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(0)))
        self.assertEqual(resp.body, plain_body)
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a/plainc',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'text/plain')

        for accept in ('', 'text/plain', 'application/xml;q=0.8,*/*;q=0.9',
                       '*/*;q=0.9,application/xml;q=0.8', '*/*',
                       'text/plain,application/xml'):
            req = Request.blank(
                '/sda1/p/a/plainc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEqual(
                resp.body, plain_body,
                'Invalid body for Accept: %s' % accept)
            self.assertEqual(
                resp.content_type, 'text/plain',
                'Invalid content_type for Accept: %s' % accept)

            req = Request.blank(
                '/sda1/p/a/plainc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEqual(
                resp.content_type, 'text/plain',
                'Invalid content_type for Accept: %s' % accept)

        # test conflicting formats
        req = Request.blank(
            '/sda1/p/a/plainc?format=plain',
            environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(resp.body, plain_body)

        # test unknown format uses default plain
        req = Request.blank(
            '/sda1/p/a/plainc?format=somethingelse',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(resp.body, plain_body)

    def test_GET_json_last_modified(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/jsonc', environ={
                'REQUEST_METHOD': 'PUT',
                'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i, d in [(0, 1.5), (1, 1.0), ]:
            req = Request.blank(
                '/sda1/p/a/jsonc/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': d,
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        # test format
        # last_modified format must be uniform, even when there are not msecs
        json_body = [{"name": "0",
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.500000"},
                     {"name": "1",
                      "hash": "x",
                      "bytes": 0,
                      "content_type": "text/plain",
                      "last_modified": "1970-01-01T00:00:01.000000"}, ]

        req = Request.blank(
            '/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(json.loads(resp.body), json_body)
        self.assertEqual(resp.charset, 'utf-8')

    def test_GET_xml(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/xmlc', environ={'REQUEST_METHOD': 'PUT',
                                       'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/xmlc/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        xml_body = b'<?xml version="1.0" encoding="UTF-8"?>\n' \
            b'<container name="xmlc">' \
            b'<object><name>0</name><hash>x</hash><bytes>0</bytes>' \
            b'<content_type>text/plain</content_type>' \
            b'<last_modified>1970-01-01T00:00:01.000000' \
            b'</last_modified></object>' \
            b'<object><name>1</name><hash>x</hash><bytes>0</bytes>' \
            b'<content_type>text/plain</content_type>' \
            b'<last_modified>1970-01-01T00:00:01.000000' \
            b'</last_modified></object>' \
            b'<object><name>2</name><hash>x</hash><bytes>0</bytes>' \
            b'<content_type>text/plain</content_type>' \
            b'<last_modified>1970-01-01T00:00:01.000000' \
            b'</last_modified></object>' \
            b'</container>'

        # tests
        req = Request.blank(
            '/sda1/p/a/xmlc?format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')
        self.assertEqual(
            resp.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(0)))
        self.assertEqual(resp.body, xml_body)
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/xmlc?format=xml',
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')

        for xml_accept in (
                'application/xml', 'application/xml;q=1.0,*/*;q=0.9',
                '*/*;q=0.9,application/xml;q=1.0', 'application/xml,text/xml'):
            req = Request.blank(
                '/sda1/p/a/xmlc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = xml_accept
            resp = req.get_response(self.controller)
            self.assertEqual(
                resp.body, xml_body,
                'Invalid body for Accept: %s' % xml_accept)
            self.assertEqual(
                resp.content_type, 'application/xml',
                'Invalid content_type for Accept: %s' % xml_accept)

            req = Request.blank(
                '/sda1/p/a/xmlc',
                environ={'REQUEST_METHOD': 'HEAD'})
            req.accept = xml_accept
            resp = req.get_response(self.controller)
            self.assertEqual(
                resp.content_type, 'application/xml',
                'Invalid content_type for Accept: %s' % xml_accept)

        req = Request.blank(
            '/sda1/p/a/xmlc',
            environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'text/xml'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'text/xml')
        self.assertEqual(resp.body, xml_body)

    def test_GET_invalid_accept(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'},
            headers={'Accept': 'application/plain;q'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual(resp.body, b'Invalid Accept header')

    def test_GET_marker(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        # test limit with marker
        req = Request.blank('/sda1/p/a/c?limit=2&marker=1',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split(b'\n')
        self.assertEqual(result, [b'2', b''])
        # test limit with end_marker
        req = Request.blank('/sda1/p/a/c?limit=2&end_marker=1',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split(b'\n')
        self.assertEqual(result, [b'0', b''])
        # test limit, reverse with end_marker
        req = Request.blank('/sda1/p/a/c?limit=2&end_marker=1&reverse=True',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split(b'\n')
        self.assertEqual(result, [b'2', b''])
        # test marker > end_marker
        req = Request.blank('/sda1/p/a/c?marker=2&end_marker=1',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split(b'\n')
        self.assertEqual(result, [b''])

    def test_weird_content_types(self):
        snowman = u'\u2603'
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i, ctype in enumerate((snowman.encode('utf-8'),
                                  b'text/plain; charset="utf-8"')):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': bytes_to_wsgi(ctype),
                    'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        result = [x['content_type'] for x in json.loads(resp.body)]
        self.assertEqual(result, [u'\u2603', 'text/plain;charset="utf-8"'])

    def test_swift_bytes_in_content_type(self):
        # create container
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)

        # regular object update
        ctype = 'text/plain; charset="utf-8"'
        req = Request.blank(
            '/sda1/p/a/c/o1', environ={
                'REQUEST_METHOD': 'PUT',
                'HTTP_X_TIMESTAMP': '1', 'HTTP_X_CONTENT_TYPE': ctype,
                'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 99})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # slo object update
        ctype = 'text/plain; charset="utf-8"; swift_bytes=12345678'
        req = Request.blank(
            '/sda1/p/a/c/o2', environ={
                'REQUEST_METHOD': 'PUT',
                'HTTP_X_TIMESTAMP': '1', 'HTTP_X_CONTENT_TYPE': ctype,
                'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 99})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # verify listing
        req = Request.blank('/sda1/p/a/c?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        listing = json.loads(resp.body)
        self.assertEqual(2, len(listing))
        self.assertEqual('text/plain;charset="utf-8"',
                         listing[0]['content_type'])
        self.assertEqual(99, listing[0]['bytes'])
        self.assertEqual('text/plain;charset="utf-8"',
                         listing[1]['content_type'])
        self.assertEqual(12345678, listing[1]['bytes'])

    def test_GET_accept_not_valid(self):
        req = Request.blank('/sda1/p/a/c', method='PUT', headers={
            'X-Timestamp': Timestamp(0).internal})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', method='GET')
        req.accept = 'application/xml*'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 406)

    def test_GET_limit(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        # test limit
        req = Request.blank(
            '/sda1/p/a/c?limit=2', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split(b'\n')
        self.assertEqual(result, [b'0', b'1', b''])

    def test_GET_prefix(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i in ('a1', 'b1', 'a2', 'b2', 'a3', 'b3'):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?prefix=a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.body.split(b'\n'), [b'a1', b'a2', b'a3', b''])

    def test_GET_delimiter_too_long(self):
        req = Request.blank('/sda1/p/a/c?delimiter=xx',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 412)

    def test_GET_delimiter(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i in ('US-TX-A', 'US-TX-B', 'US-OK-A', 'US-OK-B', 'US-UT-A'):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?prefix=US-&delimiter=-&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(
            json.loads(resp.body),
            [{"subdir": "US-OK-"},
             {"subdir": "US-TX-"},
             {"subdir": "US-UT-"}])

    def test_GET_delimiter_non_ascii(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for obj_name in [u"a/❥/1", u"a/❥/2", u"a/ꙮ/1", u"a/ꙮ/2"]:
            req = Request.blank(
                '/sda1/p/a/c/%s' % bytes_to_wsgi(obj_name.encode('utf-8')),
                environ={
                    'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)

        # JSON
        req = Request.blank(
            '/sda1/p/a/c?prefix=a/&delimiter=/&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(
            json.loads(resp.body),
            [{"subdir": u"a/❥/"},
             {"subdir": u"a/ꙮ/"}])

        # Plain text
        req = Request.blank(
            '/sda1/p/a/c?prefix=a/&delimiter=/&format=text',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.body, u"a/❥/\na/ꙮ/\n".encode("utf-8"))

    def test_GET_leading_delimiter(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i in ('US-TX-A', 'US-TX-B', '-UK', '-CH'):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?delimiter=-&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(
            json.loads(resp.body),
            [{"subdir": "-"},
             {"subdir": "US-"}])

    def test_GET_delimiter_xml(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i in ('US-TX-A', 'US-TX-B', 'US-OK-A', 'US-OK-B', 'US-UT-A'):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?prefix=US-&delimiter=-&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(
            resp.body, b'<?xml version="1.0" encoding="UTF-8"?>'
            b'\n<container name="c"><subdir name="US-OK-">'
            b'<name>US-OK-</name></subdir>'
            b'<subdir name="US-TX-"><name>US-TX-</name></subdir>'
            b'<subdir name="US-UT-"><name>US-UT-</name></subdir></container>')

    def test_GET_delimiter_xml_with_quotes(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        req = Request.blank(
            '/sda1/p/a/c/<\'sub\' "dir">/object',
            environ={
                'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                'HTTP_X_SIZE': 0})
        self._update_object_put_headers(req)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?delimiter=/&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        dom = minidom.parseString(resp.body)
        self.assertTrue(len(dom.getElementsByTagName('container')) == 1)
        container = dom.getElementsByTagName('container')[0]
        self.assertTrue(len(container.getElementsByTagName('subdir')) == 1)
        subdir = container.getElementsByTagName('subdir')[0]
        self.assertEqual(six.text_type(subdir.attributes['name'].value),
                         u'<\'sub\' "dir">/')
        self.assertTrue(len(subdir.getElementsByTagName('name')) == 1)
        name = subdir.getElementsByTagName('name')[0]
        self.assertEqual(six.text_type(name.childNodes[0].data),
                         u'<\'sub\' "dir">/')

    def test_GET_path(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i in ('US/TX', 'US/TX/B', 'US/OK', 'US/OK/B', 'US/UT/A'):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i,
                environ={
                    'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            self._update_object_put_headers(req)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?path=US&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(
            json.loads(resp.body),
            [{"name": "US/OK", "hash": "x", "bytes": 0,
              "content_type": "text/plain",
              "last_modified": "1970-01-01T00:00:01.000000"},
             {"name": "US/TX", "hash": "x", "bytes": 0,
              "content_type": "text/plain",
              "last_modified": "1970-01-01T00:00:01.000000"}])

    def test_through_call(self):
        inbuf = BytesIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(status, headers):
            outbuf.writelines(status)

        self.controller.__call__({'REQUEST_METHOD': 'GET',
                                  'SCRIPT_NAME': '',
                                  'PATH_INFO': '/sda1/p/a/c',
                                  'SERVER_NAME': '127.0.0.1',
                                  'SERVER_PORT': '8080',
                                  'SERVER_PROTOCOL': 'HTTP/1.0',
                                  'CONTENT_LENGTH': '0',
                                  'wsgi.version': (1, 0),
                                  'wsgi.url_scheme': 'http',
                                  'wsgi.input': inbuf,
                                  'wsgi.errors': errbuf,
                                  'wsgi.multithread': False,
                                  'wsgi.multiprocess': False,
                                  'wsgi.run_once': False},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '404 ')

    def test_through_call_invalid_path(self):
        inbuf = BytesIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(status, headers):
            outbuf.writelines(status)

        self.controller.__call__({'REQUEST_METHOD': 'GET',
                                  'SCRIPT_NAME': '',
                                  'PATH_INFO': '/bob',
                                  'SERVER_NAME': '127.0.0.1',
                                  'SERVER_PORT': '8080',
                                  'SERVER_PROTOCOL': 'HTTP/1.0',
                                  'CONTENT_LENGTH': '0',
                                  'wsgi.version': (1, 0),
                                  'wsgi.url_scheme': 'http',
                                  'wsgi.input': inbuf,
                                  'wsgi.errors': errbuf,
                                  'wsgi.multithread': False,
                                  'wsgi.multiprocess': False,
                                  'wsgi.run_once': False},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '400 ')

    def test_through_call_invalid_path_utf8(self):
        inbuf = BytesIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(status, headers):
            outbuf.writelines(status)

        self.controller.__call__({'REQUEST_METHOD': 'GET',
                                  'SCRIPT_NAME': '',
                                  'PATH_INFO': '\x00',
                                  'SERVER_NAME': '127.0.0.1',
                                  'SERVER_PORT': '8080',
                                  'SERVER_PROTOCOL': 'HTTP/1.0',
                                  'CONTENT_LENGTH': '0',
                                  'wsgi.version': (1, 0),
                                  'wsgi.url_scheme': 'http',
                                  'wsgi.input': inbuf,
                                  'wsgi.errors': errbuf,
                                  'wsgi.multithread': False,
                                  'wsgi.multiprocess': False,
                                  'wsgi.run_once': False},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '412 ')

    def test_invalid_method_doesnt_exist(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(status, headers):
            outbuf.writelines(status)

        self.controller.__call__({'REQUEST_METHOD': 'method_doesnt_exist',
                                  'PATH_INFO': '/sda1/p/a/c'},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(status, headers):
            outbuf.writelines(status)

        self.controller.__call__({'REQUEST_METHOD': '__init__',
                                  'PATH_INFO': '/sda1/p/a/c'},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_params_format(self):
        req = Request.blank(
            '/sda1/p/a/c', method='PUT',
            headers={'X-Timestamp': Timestamp(1).internal})
        req.get_response(self.controller)
        for format in ('xml', 'json'):
            req = Request.blank('/sda1/p/a/c?format=%s' % format,
                                method='GET')
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)

    def test_params_utf8(self):
        # Bad UTF8 sequence, all parameters should cause 400 error
        for param in ('delimiter', 'limit', 'marker', 'path', 'prefix',
                      'end_marker', 'format'):
            req = Request.blank('/sda1/p/a/c?%s=\xce' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 400,
                             "%d on param %s" % (resp.status_int, param))
        # Good UTF8 sequence for delimiter, too long (1 byte delimiters only)
        req = Request.blank('/sda1/p/a/c?delimiter=\xce\xa9',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 412,
                         "%d on param delimiter" % (resp.status_int))
        req = Request.blank('/sda1/p/a/c', method='PUT',
                            headers={'X-Timestamp': Timestamp(1).internal})
        req.get_response(self.controller)
        # Good UTF8 sequence, ignored for limit, doesn't affect other queries
        for param in ('limit', 'marker', 'path', 'prefix', 'end_marker',
                      'format'):
            req = Request.blank('/sda1/p/a/c?%s=\xce\xa9' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 204,
                             "%d on param %s" % (resp.status_int, param))

    def test_put_auto_create(self):
        headers = {'x-timestamp': Timestamp(1).internal,
                   'x-size': '0',
                   'x-content-type': 'text/plain',
                   'x-etag': 'd41d8cd98f00b204e9800998ecf8427e'}

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/.a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/.c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/c/.o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_delete_auto_create(self):
        headers = {'x-timestamp': Timestamp(1).internal}

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/.a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/.c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/.c/.o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_content_type_on_HEAD(self):
        Request.blank('/sda1/p/a/o',
                      headers={'X-Timestamp': Timestamp(1).internal},
                      environ={'REQUEST_METHOD': 'PUT'}).get_response(
                          self.controller)

        env = {'REQUEST_METHOD': 'HEAD'}

        req = Request.blank('/sda1/p/a/o?format=xml', environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a/o?format=json', environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a/o', environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/o', headers={'Accept': 'application/json'}, environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/o', headers={'Accept': 'application/xml'}, environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')
        self.assertEqual(resp.charset, 'utf-8')

    def test_updating_multiple_container_servers(self):
        http_connect_args = []

        def fake_http_connect(ipaddr, port, device, partition, method, path,
                              headers=None, query_string=None, ssl=False):

            class SuccessfulFakeConn(object):
                @property
                def status(self):
                    return 200

                def getresponse(self):
                    return self

                def read(self):
                    return ''

            captured_args = {'ipaddr': ipaddr, 'port': port,
                             'device': device, 'partition': partition,
                             'method': method, 'path': path, 'ssl': ssl,
                             'headers': headers, 'query_string': query_string}

            http_connect_args.append(
                dict((k, v) for k, v in captured_args.items()
                     if v is not None))

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'X-Account-Partition': '30',
                     'X-Account-Host': '1.2.3.4:5, 6.7.8.9:10',
                     'X-Account-Device': 'sdb1, sdf1'})

        orig_http_connect = container_server.http_connect
        try:
            container_server.http_connect = fake_http_connect
            req.get_response(self.controller)
        finally:
            container_server.http_connect = orig_http_connect

        http_connect_args.sort(key=operator.itemgetter('ipaddr'))

        self.assertEqual(len(http_connect_args), 2)
        self.assertEqual(
            http_connect_args[0],
            {'ipaddr': '1.2.3.4',
             'port': '5',
             'path': '/a/c',
             'device': 'sdb1',
             'partition': '30',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-bytes-used': 0,
                 'x-delete-timestamp': '0',
                 'x-object-count': 0,
                 'x-put-timestamp': Timestamp(12345).internal,
                 'X-Backend-Storage-Policy-Index': '%s' % POLICIES.default.idx,
                 'referer': 'PUT http://localhost/sda1/p/a/c',
                 'user-agent': 'container-server %d' % os.getpid(),
                 'x-trans-id': '-'})})
        self.assertEqual(
            http_connect_args[1],
            {'ipaddr': '6.7.8.9',
             'port': '10',
             'path': '/a/c',
             'device': 'sdf1',
             'partition': '30',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-bytes-used': 0,
                 'x-delete-timestamp': '0',
                 'x-object-count': 0,
                 'x-put-timestamp': Timestamp(12345).internal,
                 'X-Backend-Storage-Policy-Index': '%s' % POLICIES.default.idx,
                 'referer': 'PUT http://localhost/sda1/p/a/c',
                 'user-agent': 'container-server %d' % os.getpid(),
                 'x-trans-id': '-'})})

    def test_serv_reserv(self):
        # Test replication_server flag was set from configuration file.
        container_controller = container_server.ContainerController
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertIsNone(container_controller(conf).replication_server)
        for val in [True, '1', 'True', 'true']:
            conf['replication_server'] = val
            self.assertTrue(container_controller(conf).replication_server)
        for val in [False, 0, '0', 'False', 'false', 'test_string']:
            conf['replication_server'] = val
            self.assertFalse(container_controller(conf).replication_server)

    def test_list_allowed_methods(self):
        # Test list of allowed_methods
        obj_methods = ['DELETE', 'PUT', 'HEAD', 'GET', 'POST']
        repl_methods = ['REPLICATE']
        for method_name in obj_methods:
            method = getattr(self.controller, method_name)
            self.assertFalse(hasattr(method, 'replication'))
        for method_name in repl_methods:
            method = getattr(self.controller, method_name)
            self.assertEqual(method.replication, True)

    def test_correct_allowed_method(self):
        # Test correct work for allowed method using
        # swift.container.server.ContainerController.__call__
        inbuf = BytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(status, headers):
            """Sends args to outbuf"""
            outbuf.writelines(status)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
               'SCRIPT_NAME': '',
               'PATH_INFO': '/sda1/p/a/c',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '8080',
               'SERVER_PROTOCOL': 'HTTP/1.0',
               'CONTENT_LENGTH': '0',
               'wsgi.version': (1, 0),
               'wsgi.url_scheme': 'http',
               'wsgi.input': inbuf,
               'wsgi.errors': errbuf,
               'wsgi.multithread': False,
               'wsgi.multiprocess': False,
               'wsgi.run_once': False}

        method_res = mock.MagicMock()
        mock_method = public(lambda x: mock.MagicMock(return_value=method_res))
        with mock.patch.object(self.controller, method, new=mock_method):
            response = self.controller(env, start_response)
            self.assertEqual(response, method_res)
            # The controller passed responsibility of calling start_response
            # to the mock, which never did
            self.assertEqual(outbuf.getvalue(), '')

    def test_not_allowed_method(self):
        # Test correct work for NOT allowed method using
        # swift.container.server.ContainerController.__call__
        inbuf = BytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(status, headers):
            """Sends args to outbuf"""
            outbuf.writelines(status)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
               'SCRIPT_NAME': '',
               'PATH_INFO': '/sda1/p/a/c',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '8080',
               'SERVER_PROTOCOL': 'HTTP/1.0',
               'CONTENT_LENGTH': '0',
               'wsgi.version': (1, 0),
               'wsgi.url_scheme': 'http',
               'wsgi.input': inbuf,
               'wsgi.errors': errbuf,
               'wsgi.multithread': False,
               'wsgi.multiprocess': False,
               'wsgi.run_once': False}

        answer = [b'<html><h1>Method Not Allowed</h1><p>The method is not '
                  b'allowed for this resource.</p></html>']
        mock_method = replication(public(lambda x: mock.MagicMock()))
        with mock.patch.object(self.controller, method, new=mock_method):
            response = self.controller.__call__(env, start_response)
            self.assertEqual(response, answer)
            self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_call_incorrect_replication_method(self):
        inbuf = BytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'true'})

        def start_response(status, headers):
            """Sends args to outbuf"""
            outbuf.writelines(status)

        obj_methods = ['DELETE', 'PUT', 'HEAD', 'GET', 'POST', 'OPTIONS']
        for method in obj_methods:
            env = {'REQUEST_METHOD': method,
                   'SCRIPT_NAME': '',
                   'PATH_INFO': '/sda1/p/a/c',
                   'SERVER_NAME': '127.0.0.1',
                   'SERVER_PORT': '8080',
                   'SERVER_PROTOCOL': 'HTTP/1.0',
                   'CONTENT_LENGTH': '0',
                   'wsgi.version': (1, 0),
                   'wsgi.url_scheme': 'http',
                   'wsgi.input': inbuf,
                   'wsgi.errors': errbuf,
                   'wsgi.multithread': False,
                   'wsgi.multiprocess': False,
                   'wsgi.run_once': False}
            self.controller(env, start_response)
            self.assertEqual(errbuf.getvalue(), '')
            self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test__call__raise_timeout(self):
        inbuf = WsgiBytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.logger = debug_logger('test')
        self.container_controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false', 'log_requests': 'false'},
            logger=self.logger)

        def start_response(status, headers):
            # Sends args to outbuf
            outbuf.writelines(status)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
               'SCRIPT_NAME': '',
               'PATH_INFO': '/sda1/p/a/c',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '8080',
               'SERVER_PROTOCOL': 'HTTP/1.0',
               'CONTENT_LENGTH': '0',
               'wsgi.version': (1, 0),
               'wsgi.url_scheme': 'http',
               'wsgi.input': inbuf,
               'wsgi.errors': errbuf,
               'wsgi.multithread': False,
               'wsgi.multiprocess': False,
               'wsgi.run_once': False}

        @public
        def mock_put_method(*args, **kwargs):
            raise Exception()

        with mock.patch.object(self.container_controller, method,
                               new=mock_put_method):
            response = self.container_controller.__call__(env, start_response)
            self.assertTrue(response[0].startswith(
                b'Traceback (most recent call last):'))
            self.assertEqual(self.logger.get_lines_for_level('error'), [
                'ERROR __call__ error with %(method)s %(path)s : ' % {
                    'method': 'PUT', 'path': '/sda1/p/a/c'},
            ])
            self.assertEqual(self.logger.get_lines_for_level('info'), [])
            self.assertEqual(outbuf.getvalue()[:4], '500 ')

    def test_GET_log_requests_true(self):
        self.controller.log_requests = True

        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue(self.controller.logger.log_dict['info'])

    def test_GET_log_requests_false(self):
        self.controller.log_requests = False
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertFalse(self.controller.logger.log_dict['info'])

    def test_log_line_format(self):
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'HEAD', 'REMOTE_ADDR': '1.2.3.4'})
        with mock.patch('time.gmtime',
                        mock.MagicMock(side_effect=[gmtime(10001.0)])), \
                mock.patch('time.time',
                           mock.MagicMock(side_effect=[
                               10000.0, 10001.0, 10002.0])), \
                mock.patch('os.getpid', mock.MagicMock(return_value=1234)):
            req.get_response(self.controller)
        info_lines = self.controller.logger.get_lines_for_level('info')
        self.assertEqual(info_lines, [
            '1.2.3.4 - - [01/Jan/1970:02:46:41 +0000] "HEAD /sda1/p/a/c" '
            '404 - "-" "-" "-" 2.0000 "-" 1234 0',
        ])


@patch_policies([
    StoragePolicy(0, 'legacy'),
    StoragePolicy(1, 'one'),
    StoragePolicy(2, 'two', True),
    StoragePolicy(3, 'three'),
    StoragePolicy(4, 'four'),
])
class TestNonLegacyDefaultStoragePolicy(TestContainerController):
    """
    Test swift.container.server.ContainerController with a non-legacy default
    Storage Policy.
    """

    def _update_object_put_headers(self, req):
        """
        Add policy index headers for containers created with default policy
        - which in this TestCase is 1.
        """
        req.headers['X-Backend-Storage-Policy-Index'] = \
            str(POLICIES.default.idx)


if __name__ == '__main__':
    unittest.main()
