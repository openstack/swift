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

import os
import sys
import unittest
from shutil import rmtree
from StringIO import StringIO
from time import time
from tempfile import mkdtemp

from eventlet import spawn, Timeout, listen
import simplejson
from webob import Request

from swift.container import server as container_server
from swift.common.utils import normalize_timestamp, mkdirs


class TestContainerController(unittest.TestCase):
    """ Test swift.container_server.ContainerController """
    def setUp(self):
        """ Set up for testing swift.object_server.ObjectController """
        self.testdir = os.path.join(mkdtemp(),
                                    'tmp_test_object_server_ObjectController')
        mkdirs(self.testdir)
        rmtree(self.testdir)
        mkdirs(os.path.join(self.testdir, 'sda1'))
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false'})

    def tearDown(self):
        """ Tear down for testing swift.object_server.ObjectController """
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_acl_container(self):
        # Ensure no acl by default
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = self.controller.HEAD(req)
        self.assert_(response.status.startswith('204'))
        self.assert_('x-container-read' not in response.headers)
        self.assert_('x-container-write' not in response.headers)
        # Ensure POSTing acls works
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '1', 'X-Container-Read': '.r:*',
                     'X-Container-Write': 'account:user'})
        self.controller.POST(req)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = self.controller.HEAD(req)
        self.assert_(response.status.startswith('204'))
        self.assertEquals(response.headers.get('x-container-read'), '.r:*')
        self.assertEquals(response.headers.get('x-container-write'),
                          'account:user')
        # Ensure we can clear acls on POST
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '3', 'X-Container-Read': '',
                     'X-Container-Write': ''})
        self.controller.POST(req)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = self.controller.HEAD(req)
        self.assert_(response.status.startswith('204'))
        self.assert_('x-container-read' not in response.headers)
        self.assert_('x-container-write' not in response.headers)
        # Ensure PUTing acls works
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '4', 'X-Container-Read': '.r:*',
                     'X-Container-Write': 'account:user'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'HEAD'})
        response = self.controller.HEAD(req)
        self.assert_(response.status.startswith('204'))
        self.assertEquals(response.headers.get('x-container-read'), '.r:*')
        self.assertEquals(response.headers.get('x-container-write'),
                          'account:user')

    def test_HEAD(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        response = self.controller.HEAD(req)
        self.assert_(response.status.startswith('204'))
        self.assertEquals(int(response.headers['x-container-bytes-used']), 0)
        self.assertEquals(int(response.headers['x-container-object-count']), 0)
        req2 = Request.blank('/sda1/p/a/c/o', environ=
                {'HTTP_X_TIMESTAMP': '1', 'HTTP_X_SIZE': 42,
                 'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x'})
        self.controller.PUT(req2)
        response = self.controller.HEAD(req)
        self.assertEquals(int(response.headers['x-container-bytes-used']), 42)
        self.assertEquals(int(response.headers['x-container-object-count']), 1)

    def test_HEAD_not_found(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)

    def test_PUT(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '2'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)

    def test_PUT_obj_not_found(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '1', 'X-Size': '0',
                     'X-Content-Type': 'text/plain', 'X-ETag': 'e'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 404)

    def test_PUT_GET_metadata(self):
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Container-Meta-Test': 'Value'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Container-Meta-Test2': 'Value2'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'), 'Value')
        self.assertEquals(resp.headers.get('x-container-meta-test2'), 'Value2')
        # Update metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Container-Meta-Test': 'New Value'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Send old update to metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Container-Meta-Test': 'Old Value'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Container-Meta-Test': ''})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assert_('x-container-meta-test' not in resp.headers)

    def test_POST_HEAD_metadata(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        # Set metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Container-Meta-Test': 'Value'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'), 'Value')
        # Update metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Container-Meta-Test': 'New Value'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Send old update to metadata header
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Container-Meta-Test': 'Old Value'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Container-Meta-Test': ''})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assert_('x-container-meta-test' not in resp.headers)

    def test_DELETE_obj_not_found(self):
        req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 404)

    def test_PUT_utf8(self):
        snowman = u'\u2603'
        container_name = snowman.encode('utf-8')
        req = Request.blank('/sda1/p/a/%s'%container_name, environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_account_update(self):
        bindsock = listen(('127.0.0.1', 0))
        def accept(return_code, expected_timestamp):
            try:
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEquals(inc.readline(),
                                      'PUT /sda1/123/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = inc.readline()
                    self.assertEquals(headers['x-put-timestamp'],
                                      expected_timestamp)
            except BaseException, err:
                return err
            return None
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0000000001.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 201, '0000000001.00000')
        try:
            with Timeout(3):
                resp = self.controller.PUT(req)
                self.assertEquals(resp.status_int, 201)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '2'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0000000003.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 404, '0000000003.00000')
        try:
            with Timeout(3):
                resp = self.controller.PUT(req)
                self.assertEquals(resp.status_int, 404)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0000000005.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 503, '0000000005.00000')
        got_exc = False
        try:
            with Timeout(3):
                resp = self.controller.PUT(req)
        except BaseException, err:
            got_exc = True
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        self.assert_(not got_exc)

    def test_PUT_reset_container_sync(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)
        db.set_x_container_sync_points(123, 456)
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], 123)
        self.assertEquals(info['x_container_sync_point2'], 456)
        # Set to same value
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], 123)
        self.assertEquals(info['x_container_sync_point2'], 456)
        # Set to new value
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c2'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_POST_reset_container_sync(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)
        db.set_x_container_sync_points(123, 456)
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], 123)
        self.assertEquals(info['x_container_sync_point2'], 456)
        # Set to same value
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], 123)
        self.assertEquals(info['x_container_sync_point2'], 456)
        # Set to new value
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c2'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_DELETE(self):
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '2'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'}, headers={'X-Timestamp': '3'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_not_found(self):
        # Even if the container wasn't previously heard of, the container
        # server will accept the delete and replicate it to where it belongs
        # later.
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE', 'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_object(self):
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '2'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0',
                     'HTTP_X_SIZE': 1, 'HTTP_X_CONTENT_TYPE': 'text/plain',
                     'HTTP_X_ETAG': 'x'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '3'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 409)
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '4'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '5'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'}, headers={'X-Timestamp': '6'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_account_update(self):
        bindsock = listen(('127.0.0.1', 0))
        def accept(return_code, expected_timestamp):
            try:
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    inc = sock.makefile('rb')
                    out = sock.makefile('wb')
                    out.write('HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n' %
                              return_code)
                    out.flush()
                    self.assertEquals(inc.readline(),
                                      'PUT /sda1/123/a/c HTTP/1.1\r\n')
                    headers = {}
                    line = inc.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = inc.readline()
                    self.assertEquals(headers['x-delete-timestamp'],
                                      expected_timestamp)
            except BaseException, err:
                return err
            return None
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '0000000002.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 204, '0000000002.00000')
        try:
            with Timeout(3):
                resp = self.controller.DELETE(req)
                self.assertEquals(resp.status_int, 204)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '2'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '0000000003.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 404, '0000000003.00000')
        try:
            with Timeout(3):
                resp = self.controller.DELETE(req)
                self.assertEquals(resp.status_int, 404)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '4'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '0000000005.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 503, '0000000005.00000')
        got_exc = False
        try:
            with Timeout(3):
                resp = self.controller.DELETE(req)
        except BaseException, err:
            got_exc = True
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        self.assert_(not got_exc)

    def test_GET_over_limit(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': '0'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?limit=%d' %
            (container_server.CONTAINER_LISTING_LIMIT + 1),
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 412)

    def test_GET_json(self):
        # make a container
        req = Request.blank('/sda1/p/a/jsonc', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        # test an empty container
        req = Request.blank('/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(eval(resp.body), [])
        # fill the container
        for i in range(3):
            req = Request.blank('/sda1/p/a/jsonc/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        # test format
        json_body = [{"name":"0",
                    "hash":"x",
                    "bytes":0,
                    "content_type":"text/plain",
                    "last_modified":"1970-01-01T00:00:01.000000"},
                    {"name":"1",
                    "hash":"x",
                    "bytes":0,
                    "content_type":"text/plain",
                    "last_modified":"1970-01-01T00:00:01.000000"},
                    {"name":"2",
                    "hash":"x",
                    "bytes":0,
                    "content_type":"text/plain",
                    "last_modified":"1970-01-01T00:00:01.000000"}]

        req = Request.blank('/sda1/p/a/jsonc?format=json',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(eval(resp.body), json_body)
        self.assertEquals(resp.charset, 'utf-8')

        for accept in ('application/json', 'application/json;q=1.0,*/*;q=0.9',
                 '*/*;q=0.9,application/json;q=1.0', 'application/*'):
            req = Request.blank('/sda1/p/a/jsonc',
                    environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = self.controller.GET(req)
            self.assertEquals(eval(resp.body), json_body,
                'Invalid body for Accept: %s' % accept)
            self.assertEquals(resp.content_type, 'application/json',
                'Invalid content_type for Accept: %s' % accept)

    def test_GET_plain(self):
        # make a container
        req = Request.blank('/sda1/p/a/plainc', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        # test an empty container
        req = Request.blank('/sda1/p/a/plainc', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        # fill the container
        for i in range(3):
            req = Request.blank('/sda1/p/a/plainc/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        plain_body = '0\n1\n2\n'

        req = Request.blank('/sda1/p/a/plainc',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.body, plain_body)
        self.assertEquals(resp.charset, 'utf-8')

        for accept in ('', 'text/plain', 'application/xml;q=0.8,*/*;q=0.9',
                '*/*;q=0.9,application/xml;q=0.8', '*/*',
                'text/plain,application/xml'):
            req = Request.blank('/sda1/p/a/plainc',
                    environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = self.controller.GET(req)
            self.assertEquals(resp.body, plain_body,
                'Invalid body for Accept: %s' % accept)
            self.assertEquals(resp.content_type, 'text/plain',
                'Invalid content_type for Accept: %s' % accept)

        # test conflicting formats
        req = Request.blank('/sda1/p/a/plainc?format=plain',
                environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.body, plain_body)

    def test_GET_json_last_modified(self):
        # make a container
        req = Request.blank('/sda1/p/a/jsonc', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for i, d in [(0, 1.5),
                     (1, 1.0),]:
            req = Request.blank('/sda1/p/a/jsonc/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': d,
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        # test format
        # last_modified format must be uniform, even when there are not msecs
        json_body = [{"name":"0",
                    "hash":"x",
                    "bytes":0,
                    "content_type":"text/plain",
                    "last_modified":"1970-01-01T00:00:01.500000"},
                    {"name":"1",
                    "hash":"x",
                    "bytes":0,
                    "content_type":"text/plain",
                    "last_modified":"1970-01-01T00:00:01.000000"},]

        req = Request.blank('/sda1/p/a/jsonc?format=json',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(eval(resp.body), json_body)
        self.assertEquals(resp.charset, 'utf-8')

    def test_GET_xml(self):
        # make a container
        req = Request.blank('/sda1/p/a/xmlc', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        # fill the container
        for i in range(3):
            req = Request.blank('/sda1/p/a/xmlc/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        xml_body = '<?xml version="1.0" encoding="UTF-8"?>\n' \
            '<container name="xmlc">' \
                '<object><name>0</name><hash>x</hash><bytes>0</bytes>' \
                    '<content_type>text/plain</content_type>' \
                    '<last_modified>1970-01-01T00:00:01.000000' \
                    '</last_modified></object>' \
                '<object><name>1</name><hash>x</hash><bytes>0</bytes>' \
                    '<content_type>text/plain</content_type>' \
                    '<last_modified>1970-01-01T00:00:01.000000' \
                    '</last_modified></object>' \
                '<object><name>2</name><hash>x</hash><bytes>0</bytes>' \
                    '<content_type>text/plain</content_type>' \
                    '<last_modified>1970-01-01T00:00:01.000000' \
                    '</last_modified></object>' \
            '</container>'
        # tests
        req = Request.blank('/sda1/p/a/xmlc?format=xml',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'application/xml')
        self.assertEquals(resp.body, xml_body)
        self.assertEquals(resp.charset, 'utf-8')

        for xml_accept in ('application/xml', 'application/xml;q=1.0,*/*;q=0.9',
                 '*/*;q=0.9,application/xml;q=1.0', 'application/xml,text/xml'):
            req = Request.blank('/sda1/p/a/xmlc',
                    environ={'REQUEST_METHOD': 'GET'})
            req.accept = xml_accept
            resp = self.controller.GET(req)
            self.assertEquals(resp.body, xml_body,
                'Invalid body for Accept: %s' % xml_accept)
            self.assertEquals(resp.content_type, 'application/xml',
                'Invalid content_type for Accept: %s' % xml_accept)

        req = Request.blank('/sda1/p/a/xmlc',
                environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'text/xml'
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'text/xml')
        self.assertEquals(resp.body, xml_body)

    def test_GET_marker(self):
        # make a container
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        # fill the container
        for i in range(3):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ= {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1', 'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        # test limit with marker
        req = Request.blank('/sda1/p/a/c?limit=2&marker=1', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        result = resp.body.split()
        self.assertEquals(result, ['2',])

    def test_weird_content_types(self):
        snowman = u'\u2603'
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for i, ctype in enumerate((snowman.encode('utf-8'), 'text/plain; "utf-8"')):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ= {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1', 'HTTP_X_CONTENT_TYPE': ctype,
                    'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?format=json', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        result = [x['content_type'] for x in simplejson.loads(resp.body)]
        self.assertEquals(result, [u'\u2603', 'text/plain; "utf-8"'])
        
    def test_GET_accept_not_valid(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/xml*'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 400)
        self.assertEquals(resp.body, 'bad accept header: application/xml*')
        
    def test_GET_limit(self):
        # make a container
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        # fill the container
        for i in range(3):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        # test limit
        req = Request.blank('/sda1/p/a/c?limit=2', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        result = resp.body.split()
        self.assertEquals(result, ['0','1'])

    def test_GET_prefix(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for i in ('a1', 'b1', 'a2', 'b2', 'a3', 'b3'):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?prefix=a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.body.split(), ['a1','a2', 'a3'])

    def test_GET_delimiter(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for i in ('US-TX-A', 'US-TX-B', 'US-OK-A', 'US-OK-B', 'US-UT-A'):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?prefix=US-&delimiter=-&format=json',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(simplejson.loads(resp.body),
            [{"subdir":"US-OK-"},{"subdir":"US-TX-"},{"subdir":"US-UT-"}])

    def test_GET_delimiter_xml(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for i in ('US-TX-A', 'US-TX-B', 'US-OK-A', 'US-OK-B', 'US-UT-A'):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?prefix=US-&delimiter=-&format=xml',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.body, '<?xml version="1.0" encoding="UTF-8"?>'
            '\n<container name="c"><subdir name="US-OK-"><name>US-OK-</name></subdir>'
            '<subdir name="US-TX-"><name>US-TX-</name></subdir>'
            '<subdir name="US-UT-"><name>US-UT-</name></subdir></container>')

    def test_GET_path(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for i in ('US/TX', 'US/TX/B', 'US/OK', 'US/OK/B', 'US/UT/A'):
            req = Request.blank('/sda1/p/a/c/%s'%i, environ=
                    {'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = self.controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?path=US&format=json',
                environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(simplejson.loads(resp.body),
            [{"name":"US/OK","hash":"x","bytes":0,"content_type":"text/plain",
              "last_modified":"1970-01-01T00:00:01.000000"},
             {"name":"US/TX","hash":"x","bytes":0,"content_type":"text/plain",
              "last_modified":"1970-01-01T00:00:01.000000"}])

    def test_through_call(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        def start_response(*args):
            outbuf.writelines(args)
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
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '404 ')

    def test_through_call_invalid_path(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        def start_response(*args):
            outbuf.writelines(args)
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
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '400 ')

    def test_params_utf8(self):
        self.controller.PUT(Request.blank('/sda1/p/a/c',
                            headers={'X-Timestamp': normalize_timestamp(1)},
                            environ={'REQUEST_METHOD': 'PUT'}))
        for param in ('delimiter', 'format', 'limit', 'marker', 'path',
                      'prefix'):
            req = Request.blank('/sda1/p/a/c?%s=\xce' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assertEquals(resp.status_int, 400)
            req = Request.blank('/sda1/p/a/c?%s=\xce\xa9' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assert_(resp.status_int in (204, 412), resp.status_int)

    def test_put_auto_create(self):
        headers = {'x-timestamp': normalize_timestamp(1),
                   'x-size': '0',
                   'x-content-type': 'text/plain',
                   'x-etag': 'd41d8cd98f00b204e9800998ecf8427e'}

        resp = self.controller.PUT(Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

        resp = self.controller.PUT(Request.blank('/sda1/p/.a/c/o',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 201)

        resp = self.controller.PUT(Request.blank('/sda1/p/a/.c/o',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

        resp = self.controller.PUT(Request.blank('/sda1/p/a/.c/.o',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

    def test_delete_auto_create(self):
        headers = {'x-timestamp': normalize_timestamp(1)}

        resp = self.controller.DELETE(Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

        resp = self.controller.DELETE(Request.blank('/sda1/p/.a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 204)

        resp = self.controller.DELETE(Request.blank('/sda1/p/a/.c/o',
            environ={'REQUEST_METHOD': 'DELETE'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

        resp = self.controller.DELETE(Request.blank('/sda1/p/a/.c/.o',
            environ={'REQUEST_METHOD': 'DELETE'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)


if __name__ == '__main__':
    unittest.main()

