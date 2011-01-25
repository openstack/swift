# Copyright (c) 2010-2011 OpenStack, LLC.
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

""" Tests for swift.object_server """

import cPickle as pickle
import os
import sys
import unittest
from nose import SkipTest
from shutil import rmtree
from StringIO import StringIO
from time import gmtime, sleep, strftime, time
from tempfile import mkdtemp

from eventlet import sleep, spawn, wsgi, listen
from webob import Request
from test.unit import _getxattr as getxattr
from test.unit import _setxattr as setxattr

from test.unit import connect_tcp, readuntil2crlfs
from swift.obj import server as object_server
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, \
                               NullLogger, storage_directory


class TestObjectController(unittest.TestCase):
    """ Test swift.object_server.ObjectController """

    def setUp(self):
        """ Set up for testing swift.object_server.ObjectController """
        self.testdir = \
            os.path.join(mkdtemp(), 'tmp_test_object_server_ObjectController')
        mkdirs(self.testdir)
        rmtree(self.testdir)
        mkdirs(os.path.join(self.testdir, 'sda1'))
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.object_controller = object_server.ObjectController(conf)
        self.object_controller.bytes_per_sync = 1

    def tearDown(self):
        """ Tear down for testing swift.object_server.ObjectController """
        rmtree(os.path.dirname(self.testdir))

    def test_POST_update_meta(self):
        """ Test swift.object_server.ObjectController.POST """
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-3': 'Three',
                                     'X-Object-Meta-4': 'Four',
                                     'Content-Type': 'application/x-test'})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assert_("X-Object-Meta-1" not in resp.headers and \
                     "X-Object-Meta-3" in resp.headers)
        self.assertEquals(resp.headers['Content-Type'], 'application/x-test')

    def test_POST_not_exist(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/fail',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 404)

    def test_POST_invalid_path(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 400)

    def test_POST_container_connection(self):

        def mock_http_connect(response, with_exc=False):

            class FakeConn(object):

                def __init__(self, status, with_exc):
                    self.status = status
                    self.reason = 'Fake'
                    self.host = '1.2.3.4'
                    self.port = '1234'
                    self.with_exc = with_exc

                def getresponse(self):
                    if self.with_exc:
                        raise Exception('test')
                    return self

                def read(self, amt=None):
                    return ''

            return lambda *args, **kwargs: FakeConn(response, with_exc)

        old_http_connect = object_server.http_connect
        try:
            timestamp = normalize_timestamp(time())
            req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD':
                'POST'}, headers={'X-Timestamp': timestamp, 'Content-Type':
                'text/plain', 'Content-Length': '0'})
            resp = self.object_controller.PUT(req)
            req = Request.blank('/sda1/p/a/c/o',
                    environ={'REQUEST_METHOD': 'POST'},
                    headers={'X-Timestamp': timestamp,
                             'X-Container-Host': '1.2.3.4:0',
                             'X-Container-Partition': '3',
                             'X-Container-Device': 'sda1',
                             'X-Container-Timestamp': '1',
                             'Content-Type': 'application/new1'})
            object_server.http_connect = mock_http_connect(202)
            resp = self.object_controller.POST(req)
            self.assertEquals(resp.status_int, 202)
            req = Request.blank('/sda1/p/a/c/o',
                    environ={'REQUEST_METHOD': 'POST'},
                    headers={'X-Timestamp': timestamp,
                             'X-Container-Host': '1.2.3.4:0',
                             'X-Container-Partition': '3',
                             'X-Container-Device': 'sda1',
                             'X-Container-Timestamp': '1',
                             'Content-Type': 'application/new1'})
            object_server.http_connect = mock_http_connect(202, with_exc=True)
            resp = self.object_controller.POST(req)
            self.assertEquals(resp.status_int, 202)
            req = Request.blank('/sda1/p/a/c/o',
                    environ={'REQUEST_METHOD': 'POST'},
                    headers={'X-Timestamp': timestamp,
                             'X-Container-Host': '1.2.3.4:0',
                             'X-Container-Partition': '3',
                             'X-Container-Device': 'sda1',
                             'X-Container-Timestamp': '1',
                             'Content-Type': 'application/new2'})
            object_server.http_connect = mock_http_connect(500)
            resp = self.object_controller.POST(req)
            self.assertEquals(resp.status_int, 202)
        finally:
            object_server.http_connect = old_http_connect

    def test_PUT_invalid_path(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'})
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_no_timestamp(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT',
                                                      'CONTENT_LENGTH': '0'})
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_no_content_type(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(time()),
                         'Content-Length': '6'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_invalid_content_type(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(time()),
                         'Content-Length': '6',
                         'Content-Type': '\xff\xff'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 400)
        self.assert_('Content-Type' in resp.body)

    def test_PUT_no_content_length(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(time()),
                         'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        del req.headers['Content-Length']
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 411)

    def test_PUT_common(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Length': '6',
                         'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(open(objfile).read(), 'VERIFY')
        self.assertEquals(pickle.loads(getxattr(objfile,
                            object_server.METADATA_KEY)),
                          {'X-Timestamp': timestamp,
                           'Content-Length': '6',
                           'ETag': '0b4c12d7e0a73840c1c4f148fda3b037',
                           'Content-Type': 'application/octet-stream',
                           'name': '/a/c/o'})

    def test_PUT_overwrite(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(time()),
                         'Content-Length': '6',
                         'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'text/plain',
                                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(open(objfile).read(), 'VERIFY TWO')
        self.assertEquals(pickle.loads(getxattr(objfile,
                            object_server.METADATA_KEY)),
                          {'X-Timestamp': timestamp,
                           'Content-Length': '10',
                           'ETag': 'b381a4c5dab1eaa1eb9711fa647cd039',
                           'Content-Type': 'text/plain',
                           'name': '/a/c/o',
                           'Content-Encoding': 'gzip'})

    def test_PUT_no_etag(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                           headers={'X-Timestamp': normalize_timestamp(time()),
                                    'Content-Type': 'text/plain'})
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_invalid_etag(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                           headers={'X-Timestamp': normalize_timestamp(time()),
                                    'Content-Type': 'text/plain',
                                    'ETag': 'invalid'})
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 422)

    def test_PUT_user_metadata(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Type': 'text/plain',
                         'ETag': 'b114ab7b90d9ccac4bd5d99cc7ebb568',
                         'X-Object-Meta-1': 'One',
                         'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY THREE'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(open(objfile).read(), 'VERIFY THREE')
        self.assertEquals(pickle.loads(getxattr(objfile,
        object_server.METADATA_KEY)),
                          {'X-Timestamp': timestamp,
                           'Content-Length': '12',
                           'ETag': 'b114ab7b90d9ccac4bd5d99cc7ebb568',
                           'Content-Type': 'text/plain',
                           'name': '/a/c/o',
                           'X-Object-Meta-1': 'One',
                           'X-Object-Meta-Two': 'Two'})

    def test_PUT_container_connection(self):

        def mock_http_connect(response, with_exc=False):

            class FakeConn(object):

                def __init__(self, status, with_exc):
                    self.status = status
                    self.reason = 'Fake'
                    self.host = '1.2.3.4'
                    self.port = '1234'
                    self.with_exc = with_exc

                def getresponse(self):
                    if self.with_exc:
                        raise Exception('test')
                    return self

                def read(self, amt=None):
                    return ''

            return lambda *args, **kwargs: FakeConn(response, with_exc)

        old_http_connect = object_server.http_connect
        try:
            timestamp = normalize_timestamp(time())
            req = Request.blank('/sda1/p/a/c/o',
                    environ={'REQUEST_METHOD': 'POST'},
                    headers={'X-Timestamp': timestamp,
                             'X-Container-Host': '1.2.3.4:0',
                             'X-Container-Partition': '3',
                             'X-Container-Device': 'sda1',
                             'X-Container-Timestamp': '1',
                             'Content-Type': 'application/new1',
                             'Content-Length': '0'})
            object_server.http_connect = mock_http_connect(201)
            resp = self.object_controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            timestamp = normalize_timestamp(time())
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'X-Timestamp': timestamp,
                                         'X-Container-Host': '1.2.3.4:0',
                                         'X-Container-Partition': '3',
                                         'X-Container-Device': 'sda1',
                                         'X-Container-Timestamp': '1',
                                         'Content-Type': 'application/new1',
                                         'Content-Length': '0'})
            object_server.http_connect = mock_http_connect(500)
            resp = self.object_controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            timestamp = normalize_timestamp(time())
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'X-Timestamp': timestamp,
                                         'X-Container-Host': '1.2.3.4:0',
                                         'X-Container-Partition': '3',
                                         'X-Container-Device': 'sda1',
                                         'X-Container-Timestamp': '1',
                                         'Content-Type': 'application/new1',
                                         'Content-Length': '0'})
            object_server.http_connect = mock_http_connect(500, with_exc=True)
            resp = self.object_controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
        finally:
            object_server.http_connect = old_http_connect

    def test_HEAD(self):
        """ Test swift.object_server.ObjectController.HEAD """
        req = Request.blank('/sda1/p/a/c')
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.content_length, 6)
        self.assertEquals(resp.content_type, 'application/x-test')
        self.assertEquals(resp.headers['content-type'], 'application/x-test')
        self.assertEquals(resp.headers['last-modified'],
               strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp))))
        self.assertEquals(resp.headers['etag'],
                          '"0b4c12d7e0a73840c1c4f148fda3b037"')
        self.assertEquals(resp.headers['x-object-meta-1'], 'One')
        self.assertEquals(resp.headers['x-object-meta-two'], 'Two')

        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        os.unlink(objfile)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-length': '6'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)

    def test_GET(self):
        """ Test swift.object_server.ObjectController.GET """
        req = Request.blank('/sda1/p/a/c')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, 'VERIFY')
        self.assertEquals(resp.content_length, 6)
        self.assertEquals(resp.content_type, 'application/x-test')
        self.assertEquals(resp.headers['content-length'], '6')
        self.assertEquals(resp.headers['content-type'], 'application/x-test')
        self.assertEquals(resp.headers['last-modified'],
               strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp))))
        self.assertEquals(resp.headers['etag'],
                          '"0b4c12d7e0a73840c1c4f148fda3b037"')
        self.assertEquals(resp.headers['x-object-meta-1'], 'One')
        self.assertEquals(resp.headers['x-object-meta-two'], 'Two')

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=1-3'
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 206)
        self.assertEquals(resp.body, 'ERI')
        self.assertEquals(resp.headers['content-length'], '3')

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=1-'
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 206)
        self.assertEquals(resp.body, 'ERIFY')
        self.assertEquals(resp.headers['content-length'], '5')

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=-2'
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 206)
        self.assertEquals(resp.body, 'FY')
        self.assertEquals(resp.headers['content-length'], '2')

        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        os.unlink(objfile)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application:octet-stream',
                                'Content-Length': '6'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

    def test_GET_if_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '*'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '*'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '"%s"' % etag})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match':
                                        '"11111111111111111111111111111111"'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match':
                            '"11111111111111111111111111111111", "%s"' % etag})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match':
                            '"11111111111111111111111111111111", '
                            '"22222222222222222222222222222222"'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 412)

    def test_GET_if_none_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '*'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 304)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '*'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '"%s"' % etag})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 304)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match':
                                        '"11111111111111111111111111111111"'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match':
                                        '"11111111111111111111111111111111", '
                                        '"%s"' % etag})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 304)
        self.assertEquals(resp.etag, etag)

    def test_GET_if_modified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp)))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 304)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 304)

    def test_GET_if_unmodified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 9))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 412)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 9))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

    def test_DELETE(self):
        """ Test swift.object_server.ObjectController.DELETE """
        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 400)
        # self.assertRaises(KeyError, self.object_controller.DELETE, req)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4',
                                })
        req.body = 'test'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        timestamp = normalize_timestamp(float(timestamp) - 1)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.ts')
        self.assert_(os.path.isfile(objfile))

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.ts')
        self.assert_(os.path.isfile(objfile))

    def test_call(self):
        """ Test swift.object_server.ObjectController.__call__ """
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            """ Sends args to outbuf """
            outbuf.writelines(args)

        self.object_controller.__call__({'REQUEST_METHOD': 'PUT',
                                         'SCRIPT_NAME': '',
                                         'PATH_INFO': '/sda1/p/a/c/o',
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

        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller.__call__({'REQUEST_METHOD': 'GET',
                                         'SCRIPT_NAME': '',
                                         'PATH_INFO': '/sda1/p/a/c/o',
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

        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller.__call__({'REQUEST_METHOD': 'INVALID',
                                         'SCRIPT_NAME': '',
                                         'PATH_INFO': '/sda1/p/a/c/o',
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
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_chunked_put(self):
        listener = listen(('localhost', 0))
        port = listener.getsockname()[1]
        killer = spawn(wsgi.server, listener, self.object_controller,
                       NullLogger())
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('PUT /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Content-Type: text/plain\r\n'
                 'Connection: close\r\nX-Timestamp: 1.0\r\n'
                 'Transfer-Encoding: chunked\r\n\r\n'
                 '2\r\noh\r\n4\r\n hai\r\n0\r\n\r\n')
        fd.flush()
        readuntil2crlfs(fd)
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('GET /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\n\r\n')
        fd.flush()
        readuntil2crlfs(fd)
        response = fd.read()
        self.assertEquals(response, 'oh hai')
        killer.kill()

    def test_max_object_name_length(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/' + ('1' * 1024),
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/' + ('2' * 1025),
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_disk_file_app_iter_corners(self):
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o')
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        f.write('1234567890')
        setxattr(f.fileno(), object_server.METADATA_KEY,
                 pickle.dumps({}, object_server.PICKLE_PROTOCOL))
        f.close()
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    keep_data_fp=True)
        it = df.app_iter_range(0, None)
        sio = StringIO()
        for chunk in it:
            sio.write(chunk)
        self.assertEquals(sio.getvalue(), '1234567890')

        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    keep_data_fp=True)
        it = df.app_iter_range(5, None)
        sio = StringIO()
        for chunk in it:
            sio.write(chunk)
        self.assertEquals(sio.getvalue(), '67890')

    def test_disk_file_mkstemp_creates_dir(self):
        tmpdir = os.path.join(self.testdir, 'sda1', 'tmp')
        os.rmdir(tmpdir)
        with object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                'o').mkstemp():
            self.assert_(os.path.exists(tmpdir))

    def test_max_upload_time(self):

        class SlowBody():

            def __init__(self):
                self.sent = 0

            def read(self, size=-1):
                if self.sent < 4:
                    sleep(0.1)
                    self.sent += 1
                    return ' '
                return ''

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.object_controller.max_upload_time = 0.1
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 408)

    def test_short_body(self):

        class ShortBody():

            def __init__(self):
                self.sent = False

            def read(self, size=-1):
                if not self.sent:
                    self.sent = True
                    return '   '
                return ''

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': ShortBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 499)

    def test_bad_sinces(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'},
            body='    ')
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Unmodified-Since': 'Not a valid date'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Not a valid date'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Unmodified-Since': 'Sat, 29 Oct 1000 19:43:31 GMT'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 412)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Sat, 29 Oct 1000 19:43:31 GMT'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 412)

    def test_content_encoding(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'},
            body='    ')
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-encoding'], 'gzip')
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD':
            'HEAD'})
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-encoding'], 'gzip')

    def test_manifest_header(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Type': 'text/plain',
                         'Content-Length': '0',
                         'X-Object-Manifest': 'c/o/'})
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p', hash_path('a', 'c',
            'o')), timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(pickle.loads(getxattr(objfile,
            object_server.METADATA_KEY)), {'X-Timestamp': timestamp,
            'Content-Length': '0', 'Content-Type': 'text/plain', 'name':
            '/a/c/o', 'X-Object-Manifest': 'c/o/', 'ETag':
            'd41d8cd98f00b204e9800998ecf8427e'})
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers.get('x-object-manifest'), 'c/o/')


if __name__ == '__main__':
    unittest.main()
