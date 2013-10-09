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
import mock
import unittest
from contextlib import contextmanager
from shutil import rmtree
from StringIO import StringIO
from tempfile import mkdtemp
from xml.dom import minidom

from eventlet import spawn, Timeout, listen
import simplejson

from swift.common.swob import Request, HeaderKeyDict
import swift.container
from swift.container import server as container_server
from swift.common.utils import normalize_timestamp, mkdirs, public, replication
from test.unit import fake_http_connect


@contextmanager
def save_globals():
    orig_http_connect = getattr(swift.container.server, 'http_connect',
                                None)
    try:
        yield True
    finally:
        swift.container.server.http_connect = orig_http_connect


class TestContainerController(unittest.TestCase):
    """Test swift.container.server.ContainerController"""
    def setUp(self):
        """Set up for testing swift.object_server.ObjectController"""
        self.testdir = os.path.join(mkdtemp(),
                                    'tmp_test_object_server_ObjectController')
        mkdirs(self.testdir)
        rmtree(self.testdir)
        mkdirs(os.path.join(self.testdir, 'sda1'))
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false'})

    def tearDown(self):
        """Tear down for testing swift.object_server.ObjectController"""
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_acl_container(self):
        # Ensure no acl by default
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0'})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('201'))
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assert_(response.status.startswith('204'))
        self.assert_('x-container-read' not in response.headers)
        self.assert_('x-container-write' not in response.headers)
        # Ensure POSTing acls works
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '1', 'X-Container-Read': '.r:*',
                     'X-Container-Write': 'account:user'})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('204'))
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assert_(response.status.startswith('204'))
        self.assertEquals(response.headers.get('x-container-read'), '.r:*')
        self.assertEquals(response.headers.get('x-container-write'),
                          'account:user')
        # Ensure we can clear acls on POST
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': '3', 'X-Container-Read': '',
                     'X-Container-Write': ''})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('204'))
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assert_(response.status.startswith('204'))
        self.assert_('x-container-read' not in response.headers)
        self.assert_('x-container-write' not in response.headers)
        # Ensure PUTing acls works
        req = Request.blank(
            '/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '4', 'X-Container-Read': '.r:*',
                     'X-Container-Write': 'account:user'})
        resp = req.get_response(self.controller)
        self.assert_(resp.status.startswith('201'))
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'HEAD'})
        response = req.get_response(self.controller)
        self.assert_(response.status.startswith('204'))
        self.assertEquals(response.headers.get('x-container-read'), '.r:*')
        self.assertEquals(response.headers.get('x-container-write'),
                          'account:user')

    def test_HEAD(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD',
            'HTTP_X_TIMESTAMP': '0'})
        response = req.get_response(self.controller)
        self.assert_(response.status.startswith('204'))
        self.assertEquals(int(response.headers['x-container-bytes-used']), 0)
        self.assertEquals(int(response.headers['x-container-object-count']), 0)
        req2 = Request.blank(
            '/sda1/p/a/c/o', environ={
                'REQUEST_METHOD': 'PUT',
                'HTTP_X_TIMESTAMP': '1', 'HTTP_X_SIZE': 42,
                'HTTP_X_CONTENT_TYPE': 'text/plain', 'HTTP_X_ETAG': 'x'})
        req2.get_response(self.controller)
        response = req.get_response(self.controller)
        self.assertEquals(int(response.headers['x-container-bytes-used']), 42)
        self.assertEquals(int(response.headers['x-container-object-count']), 1)

    def test_HEAD_not_found(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_HEAD_invalid_partition(self):
        req = Request.blank('/sda1/./a/c', environ={'REQUEST_METHOD': 'HEAD',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_HEAD_insufficient_storage(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'HEAD',
                                        'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 507)

    def test_HEAD_invalid_content_type(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'Accept': 'application/plain'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 406)

    def test_HEAD_invalid_format(self):
        format = '%D1%BD%8A9'  # invalid UTF-8; should be %E1%BD%8A9 (E -> D)
        req = Request.blank(
            '/sda1/p/a/c?format=' + format,
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_PUT(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 202)

    def test_PUT_obj_not_found(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '1', 'X-Size': '0',
                     'X-Content-Type': 'text/plain', 'X-ETag': 'e'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_PUT_GET_metadata(self):
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Container-Meta-Test': 'Value'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Container-Meta-Test2': 'Value2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'), 'Value')
        self.assertEquals(resp.headers.get('x-container-meta-test2'), 'Value2')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Container-Meta-Test': 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Container-Meta-Test': 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Container-Meta-Test': ''})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assert_('x-container-meta-test' not in resp.headers)

    def test_PUT_invalid_partition(self):
        req = Request.blank('/sda1/./a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_insufficient_storage(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                        'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 507)

    def test_POST_HEAD_metadata(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Container-Meta-Test': 'Value'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'), 'Value')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Container-Meta-Test': 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Container-Meta-Test': 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-container-meta-test'),
                          'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Container-Meta-Test': ''})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        self.assert_('x-container-meta-test' not in resp.headers)

    def test_POST_invalid_partition(self):
        req = Request.blank('/sda1/./a/c', environ={'REQUEST_METHOD': 'POST',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_POST_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_POST_insufficient_storage(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'POST',
                                        'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 507)

    def test_POST_invalid_container_sync_to(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'POST',
                                        'HTTP_X_TIMESTAMP': '1'},
            headers={'x-container-sync-to': '192.168.0.1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

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
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_obj_not_found(self):
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_container_not_found(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'DELETE',
                                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_PUT_utf8(self):
        snowman = u'\u2603'
        container_name = snowman.encode('utf-8')
        req = Request.blank(
            '/sda1/p/a/%s' % container_name, environ={
            'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)

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
        self.assertEquals(resp.status_int, 400)

    def test_account_update_account_override_deleted(self):
        bindsock = listen(('127.0.0.1', 0))
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
            except BaseException as err:
                return err
            return None

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0000000001.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 201, '0000000001.00000')
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEquals(resp.status_int, 201)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0000000003.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 404, '0000000003.00000')
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEquals(resp.status_int, 404)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '0000000005.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 503, '0000000005.00000')
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
        self.assert_(not got_exc)

    def test_PUT_reset_container_sync(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
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
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 202)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], 123)
        self.assertEquals(info['x_container_sync_point2'], 456)
        # Set to new value
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 202)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_POST_reset_container_sync(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
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
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], 123)
        self.assertEquals(info['x_container_sync_point2'], 456)
        # Set to new value
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
            headers={'x-timestamp': '1',
                     'x-container-sync-to': 'http://127.0.0.1:12345/v1/a/c2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        db = self.controller._get_container_broker('sda1', 'p', 'a', 'c')
        info = db.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_DELETE(self):
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'}, headers={'X-Timestamp': '3'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_not_found(self):
        # Even if the container wasn't previously heard of, the container
        # server will accept the delete and replicate it to where it belongs
        # later.
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE', 'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_object(self):
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0',
                     'HTTP_X_SIZE': 1, 'HTTP_X_CONTENT_TYPE': 'text/plain',
                     'HTTP_X_ETAG': 'x'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '3'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 409)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '4'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'}, headers={'X-Timestamp': '5'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'}, headers={'X-Timestamp': '6'})
        resp = req.get_response(self.controller)
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
            except BaseException as err:
                return err
            return None

        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'X-Timestamp': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '0000000002.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 204, '0000000002.00000')
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEquals(resp.status_int, 204)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '2'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '0000000003.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 404, '0000000003.00000')
        try:
            with Timeout(3):
                resp = req.get_response(self.controller)
                self.assertEquals(resp.status_int, 404)
        finally:
            err = event.wait()
            if err:
                raise Exception(err)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '4'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': '0000000005.00000',
                     'X-Account-Host': '%s:%s' % bindsock.getsockname(),
                     'X-Account-Partition': '123',
                     'X-Account-Device': 'sda1'})
        event = spawn(accept, 503, '0000000005.00000')
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
        self.assert_(not got_exc)

    def test_DELETE_invalid_partition(self):
        req = Request.blank(
            '/sda1/./a/c', environ={'REQUEST_METHOD': 'DELETE',
                                    'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_DELETE_timestamp_not_float(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 400)

    def test_DELETE_insufficient_storage(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'DELETE',
                                        'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 507)

    def test_GET_over_limit(self):
        req = Request.blank(
            '/sda1/p/a/c?limit=%d' %
            (container_server.CONTAINER_LISTING_LIMIT + 1),
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 412)

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
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(simplejson.loads(resp.body), [])
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/jsonc/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
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
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(simplejson.loads(resp.body), json_body)
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/jsonc?format=json',
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/json')

        for accept in ('application/json', 'application/json;q=1.0,*/*;q=0.9',
                       '*/*;q=0.9,application/json;q=1.0', 'application/*'):
            req = Request.blank(
                '/sda1/p/a/jsonc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEquals(
                simplejson.loads(resp.body), json_body,
                'Invalid body for Accept: %s' % accept)
            self.assertEquals(
                resp.content_type, 'application/json',
                'Invalid content_type for Accept: %s' % accept)

            req = Request.blank(
                '/sda1/p/a/jsonc',
                environ={'REQUEST_METHOD': 'HEAD'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEquals(
                resp.content_type, 'application/json',
                'Invalid content_type for Accept: %s' % accept)

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
        self.assertEquals(resp.status_int, 204)
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/plainc/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x',
                    'HTTP_X_SIZE': 0})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        plain_body = '0\n1\n2\n'

        req = Request.blank('/sda1/p/a/plainc',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.body, plain_body)
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a/plainc',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'text/plain')

        for accept in ('', 'text/plain', 'application/xml;q=0.8,*/*;q=0.9',
                       '*/*;q=0.9,application/xml;q=0.8', '*/*',
                       'text/plain,application/xml'):
            req = Request.blank(
                '/sda1/p/a/plainc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEquals(
                resp.body, plain_body,
                'Invalid body for Accept: %s' % accept)
            self.assertEquals(
                resp.content_type, 'text/plain',
                'Invalid content_type for Accept: %s' % accept)

            req = Request.blank(
                '/sda1/p/a/plainc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = accept
            resp = req.get_response(self.controller)
            self.assertEquals(
                resp.content_type, 'text/plain',
                'Invalid content_type for Accept: %s' % accept)

        # test conflicting formats
        req = Request.blank(
            '/sda1/p/a/plainc?format=plain',
            environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.body, plain_body)

        # test unknown format uses default plain
        req = Request.blank(
            '/sda1/p/a/plainc?format=somethingelse',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.body, plain_body)

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
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
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
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(simplejson.loads(resp.body), json_body)
        self.assertEquals(resp.charset, 'utf-8')

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
            resp = req.get_response(self.controller)
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
        req = Request.blank(
            '/sda1/p/a/xmlc?format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/xml')
        self.assertEquals(resp.body, xml_body)
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/xmlc?format=xml',
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/xml')

        for xml_accept in (
                'application/xml', 'application/xml;q=1.0,*/*;q=0.9',
                '*/*;q=0.9,application/xml;q=1.0', 'application/xml,text/xml'):
            req = Request.blank(
                '/sda1/p/a/xmlc',
                environ={'REQUEST_METHOD': 'GET'})
            req.accept = xml_accept
            resp = req.get_response(self.controller)
            self.assertEquals(
                resp.body, xml_body,
                'Invalid body for Accept: %s' % xml_accept)
            self.assertEquals(
                resp.content_type, 'application/xml',
                'Invalid content_type for Accept: %s' % xml_accept)

            req = Request.blank(
                '/sda1/p/a/xmlc',
                environ={'REQUEST_METHOD': 'HEAD'})
            req.accept = xml_accept
            resp = req.get_response(self.controller)
            self.assertEquals(
                resp.content_type, 'application/xml',
                'Invalid content_type for Accept: %s' % xml_accept)

        req = Request.blank(
            '/sda1/p/a/xmlc',
            environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'text/xml'
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'text/xml')
        self.assertEquals(resp.body, xml_body)

    def test_GET_marker(self):
        # make a container
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        # fill the container
        for i in range(3):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1',
                    'HTTP_X_CONTENT_TYPE': 'text/plain',
                    'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 0})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        # test limit with marker
        req = Request.blank('/sda1/p/a/c?limit=2&marker=1',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split()
        self.assertEquals(result, ['2', ])

    def test_weird_content_types(self):
        snowman = u'\u2603'
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for i, ctype in enumerate((snowman.encode('utf-8'),
                                  'text/plain; charset="utf-8"')):
            req = Request.blank(
                '/sda1/p/a/c/%s' % i, environ={
                    'REQUEST_METHOD': 'PUT',
                    'HTTP_X_TIMESTAMP': '1', 'HTTP_X_CONTENT_TYPE': ctype,
                    'HTTP_X_ETAG': 'x', 'HTTP_X_SIZE': 0})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = [x['content_type'] for x in simplejson.loads(resp.body)]
        self.assertEquals(result, [u'\u2603', 'text/plain;charset="utf-8"'])

    def test_GET_accept_not_valid(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT',
                                    'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/xml*'
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 406)

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
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        # test limit
        req = Request.blank(
            '/sda1/p/a/c?limit=2', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        result = resp.body.split()
        self.assertEquals(result, ['0', '1'])

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
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?prefix=a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.body.split(), ['a1', 'a2', 'a3'])

    def test_GET_delimiter_too_long(self):
        req = Request.blank('/sda1/p/a/c?delimiter=xx',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 412)

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
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?prefix=US-&delimiter=-&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(
            simplejson.loads(resp.body),
            [{"subdir": "US-OK-"},
             {"subdir": "US-TX-"},
             {"subdir": "US-UT-"}])

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
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?prefix=US-&delimiter=-&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(
            resp.body, '<?xml version="1.0" encoding="UTF-8"?>'
            '\n<container name="c"><subdir name="US-OK-">'
            '<name>US-OK-</name></subdir>'
            '<subdir name="US-TX-"><name>US-TX-</name></subdir>'
            '<subdir name="US-UT-"><name>US-UT-</name></subdir></container>')

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
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?delimiter=/&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        dom = minidom.parseString(resp.body)
        self.assert_(len(dom.getElementsByTagName('container')) == 1)
        container = dom.getElementsByTagName('container')[0]
        self.assert_(len(container.getElementsByTagName('subdir')) == 1)
        subdir = container.getElementsByTagName('subdir')[0]
        self.assertEquals(unicode(subdir.attributes['name'].value),
                          u'<\'sub\' "dir">/')
        self.assert_(len(subdir.getElementsByTagName('name')) == 1)
        name = subdir.getElementsByTagName('name')[0]
        self.assertEquals(unicode(name.childNodes[0].data),
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
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c?path=US&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(
            simplejson.loads(resp.body),
            [{"name": "US/OK", "hash": "x", "bytes": 0,
              "content_type": "text/plain",
              "last_modified": "1970-01-01T00:00:01.000000"},
             {"name": "US/TX", "hash": "x", "bytes": 0,
              "content_type": "text/plain",
              "last_modified": "1970-01-01T00:00:01.000000"}])

    def test_GET_insufficient_storage(self):
        self.controller = container_server.ContainerController(
            {'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a/c', environ={'REQUEST_METHOD': 'GET',
                                        'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 507)

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

    def test_through_call_invalid_path_utf8(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

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
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '412 ')

    def test_invalid_method_doesnt_exist(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': 'method_doesnt_exist',
                                  'PATH_INFO': '/sda1/p/a/c'},
                                 start_response)
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': '__init__',
                                  'PATH_INFO': '/sda1/p/a/c'},
                                 start_response)
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_params_format(self):
        req = Request.blank(
            '/sda1/p/a/c',
            headers={'X-Timestamp': normalize_timestamp(1)},
            environ={'REQUEST_METHOD': 'PUT'})
        req.get_response(self.controller)
        for format in ('xml', 'json'):
            req = Request.blank('/sda1/p/a/c?format=%s' % format,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 200)

    def test_params_utf8(self):
        # Bad UTF8 sequence, all parameters should cause 400 error
        for param in ('delimiter', 'limit', 'marker', 'path', 'prefix',
                      'end_marker', 'format'):
            req = Request.blank('/sda1/p/a/c?%s=\xce' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 400,
                              "%d on param %s" % (resp.status_int, param))
        # Good UTF8 sequence for delimiter, too long (1 byte delimiters only)
        req = Request.blank('/sda1/p/a/c?delimiter=\xce\xa9',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 412,
                          "%d on param delimiter" % (resp.status_int))
        req = Request.blank('/sda1/p/a/c',
                            headers={'X-Timestamp': normalize_timestamp(1)},
                            environ={'REQUEST_METHOD': 'PUT'})
        req.get_response(self.controller)
        # Good UTF8 sequence, ignored for limit, doesn't affect other queries
        for param in ('limit', 'marker', 'path', 'prefix', 'end_marker',
                      'format'):
            req = Request.blank('/sda1/p/a/c?%s=\xce\xa9' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEquals(resp.status_int, 204,
                              "%d on param %s" % (resp.status_int, param))

    def test_put_auto_create(self):
        headers = {'x-timestamp': normalize_timestamp(1),
                   'x-size': '0',
                   'x-content-type': 'text/plain',
                   'x-etag': 'd41d8cd98f00b204e9800998ecf8427e'}

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

        req = Request.blank('/sda1/p/.a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/.c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/c/.o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_delete_auto_create(self):
        headers = {'x-timestamp': normalize_timestamp(1)}

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

        req = Request.blank('/sda1/p/.a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/.c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/.c/.o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 404)

    def test_content_type_on_HEAD(self):
        Request.blank('/sda1/p/a/o',
                      headers={'X-Timestamp': normalize_timestamp(1)},
                      environ={'REQUEST_METHOD': 'PUT'}).get_response(
                          self.controller)

        env = {'REQUEST_METHOD': 'HEAD'}

        req = Request.blank('/sda1/p/a/o?format=xml', environ=env)
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/xml')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a/o?format=json', environ=env)
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a/o', environ=env)
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/o', headers={'Accept': 'application/json'}, environ=env)
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a/o', headers={'Accept': 'application/xml'}, environ=env)
        resp = req.get_response(self.controller)
        self.assertEquals(resp.content_type, 'application/xml')
        self.assertEquals(resp.charset, 'utf-8')

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
                dict((k, v) for k, v in captured_args.iteritems()
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

        self.assertEquals(len(http_connect_args), 2)
        self.assertEquals(
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
                 'x-put-timestamp': '0000012345.00000',
                 'referer': 'PUT http://localhost/sda1/p/a/c',
                 'user-agent': 'container-server %d' % os.getpid(),
                 'x-trans-id': '-'})})
        self.assertEquals(
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
                 'x-put-timestamp': '0000012345.00000',
                 'referer': 'PUT http://localhost/sda1/p/a/c',
                 'user-agent': 'container-server %d' % os.getpid(),
                 'x-trans-id': '-'})})

    def test_serv_reserv(self):
        # Test replication_server flag was set from configuration file.
        container_controller = container_server.ContainerController
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertEquals(container_controller(conf).replication_server, None)
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
            self.assertEquals(method.replication, True)

    def test_correct_allowed_method(self):
        # Test correct work for allowed method using
        # swift.container.server.ContainerController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

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
            response = self.controller.__call__(env, start_response)
            self.assertEqual(response, method_res)

    def test_not_allowed_method(self):
        # Test correct work for NOT allowed method using
        # swift.container.server.ContainerController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = container_server.ContainerController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

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

        answer = ['<html><h1>Method Not Allowed</h1><p>The method is not '
                  'allowed for this resource.</p></html>']
        mock_method = replication(public(lambda x: mock.MagicMock()))
        with mock.patch.object(self.controller, method, new=mock_method):
            response = self.controller.__call__(env, start_response)
            self.assertEqual(response, answer)


if __name__ == '__main__':
    unittest.main()
