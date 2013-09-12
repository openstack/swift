#-*- coding:utf-8 -*-
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

"""Tests for swift.obj.server"""

import cPickle as pickle
import operator
import os
import mock
import unittest
from shutil import rmtree
from StringIO import StringIO
from time import gmtime, strftime, time
from tempfile import mkdtemp
from hashlib import md5

from eventlet import sleep, spawn, wsgi, listen, Timeout
from test.unit import FakeLogger
from test.unit import connect_tcp, readuntil2crlfs
from swift.obj import server as object_server
from swift.obj import diskfile
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, \
    NullLogger, storage_directory, public, replication
from swift.common import constraints
from eventlet import tpool
from swift.common.swob import Request, HeaderKeyDict


def mock_time(*args, **kwargs):
    return 5000.0


class TestObjectController(unittest.TestCase):
    """Test swift.obj.server.ObjectController"""

    def setUp(self):
        """Set up for testing swift.object.server.ObjectController"""
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'
        self.testdir = \
            os.path.join(mkdtemp(), 'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.object_controller = object_server.ObjectController(conf)
        self.object_controller.bytes_per_sync = 1
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.df_mgr = diskfile.DiskFileManager(conf, FakeLogger())

    def tearDown(self):
        """Tear down for testing swift.object.server.ObjectController"""
        rmtree(os.path.dirname(self.testdir))
        tpool.execute = self._orig_tpool_exc

    def test_REQUEST_SPECIAL_CHARS(self):
        obj = 'special昆%20/%'
        path = '/sda1/p/a/c/%s' % obj
        body = 'SPECIAL_STRING'

        # create one
        timestamp = normalize_timestamp(time())
        req = Request.blank(path, environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = body
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        # check it
        timestamp = normalize_timestamp(time())
        req = Request.blank(path, environ={'REQUEST_METHOD': 'GET'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, body)

        # update it
        timestamp = normalize_timestamp(time())
        req = Request.blank(path, environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 202)

        # head it
        timestamp = normalize_timestamp(time())
        req = Request.blank(path, environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        #delete it
        timestamp = normalize_timestamp(time())
        req = Request.blank(path, environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)

    def test_POST_update_meta(self):
        # Test swift.obj.server.ObjectController.POST
        original_headers = self.object_controller.allowed_headers
        test_headers = 'content-encoding foo bar'.split()
        self.object_controller.allowed_headers = set(test_headers)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'Foo': 'fooheader',
                                     'Baz': 'bazheader',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-3': 'Three',
                                     'X-Object-Meta-4': 'Four',
                                     'Content-Encoding': 'gzip',
                                     'Foo': 'fooheader',
                                     'Bar': 'barheader',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assert_("X-Object-Meta-1" not in resp.headers and
                     "X-Object-Meta-Two" not in resp.headers and
                     "X-Object-Meta-3" in resp.headers and
                     "X-Object-Meta-4" in resp.headers and
                     "Foo" in resp.headers and
                     "Bar" in resp.headers and
                     "Baz" not in resp.headers and
                     "Content-Encoding" in resp.headers)
        self.assertEquals(resp.headers['Content-Type'], 'application/x-test')

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assert_("X-Object-Meta-1" not in resp.headers and
                     "X-Object-Meta-Two" not in resp.headers and
                     "X-Object-Meta-3" in resp.headers and
                     "X-Object-Meta-4" in resp.headers and
                     "Foo" in resp.headers and
                     "Bar" in resp.headers and
                     "Baz" not in resp.headers and
                     "Content-Encoding" in resp.headers)
        self.assertEquals(resp.headers['Content-Type'], 'application/x-test')

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assert_("X-Object-Meta-3" not in resp.headers and
                     "X-Object-Meta-4" not in resp.headers and
                     "Foo" not in resp.headers and
                     "Bar" not in resp.headers and
                     "Content-Encoding" not in resp.headers)
        self.assertEquals(resp.headers['Content-Type'], 'application/x-test')

        # test defaults
        self.object_controller.allowed_headers = original_headers
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'Foo': 'fooheader',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Manifest': 'c/bar',
                                     'Content-Encoding': 'gzip',
                                     'Content-Disposition': 'bar',
                                     })
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assert_("X-Object-Meta-1" in resp.headers and
                     "Foo" not in resp.headers and
                     "Content-Encoding" in resp.headers and
                     "X-Object-Manifest" in resp.headers and
                     "Content-Disposition" in resp.headers)
        self.assertEquals(resp.headers['Content-Type'], 'application/x-test')

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-3': 'Three',
                                     'Foo': 'fooheader',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assert_("X-Object-Meta-1" not in resp.headers and
                     "Foo" not in resp.headers and
                     "Content-Encoding" not in resp.headers and
                     "X-Object-Manifest" not in resp.headers and
                     "Content-Disposition" not in resp.headers and
                     "X-Object-Meta-3" in resp.headers)
        self.assertEquals(resp.headers['Content-Type'], 'application/x-test')

    def test_POST_old_timestamp(self):
        ts = time()
        timestamp = normalize_timestamp(ts)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        # Same timestamp should result in 409
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-3': 'Three',
                                     'X-Object-Meta-4': 'Four',
                                     'Content-Encoding': 'gzip',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 409)

        # Earlier timestamp should result in 409
        timestamp = normalize_timestamp(ts - 1)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-5': 'Five',
                                     'X-Object-Meta-6': 'Six',
                                     'Content-Encoding': 'gzip',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 409)

    def test_POST_not_exist(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/fail',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

    def test_POST_invalid_path(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
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
            ts = time()
            timestamp = normalize_timestamp(ts)
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Type': 'text/plain',
                         'Content-Length': '0'})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 201)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'POST'},
                headers={'X-Timestamp': normalize_timestamp(ts + 1),
                         'X-Container-Host': '1.2.3.4:0',
                         'X-Container-Partition': '3',
                         'X-Container-Device': 'sda1',
                         'X-Container-Timestamp': '1',
                         'Content-Type': 'application/new1'})
            object_server.http_connect = mock_http_connect(202)
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 202)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'POST'},
                headers={'X-Timestamp': normalize_timestamp(ts + 2),
                         'X-Container-Host': '1.2.3.4:0',
                         'X-Container-Partition': '3',
                         'X-Container-Device': 'sda1',
                         'X-Container-Timestamp': '1',
                         'Content-Type': 'application/new1'})
            object_server.http_connect = mock_http_connect(202, with_exc=True)
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 202)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'POST'},
                headers={'X-Timestamp': normalize_timestamp(ts + 3),
                         'X-Container-Host': '1.2.3.4:0',
                         'X-Container-Partition': '3',
                         'X-Container-Device': 'sda1',
                         'X-Container-Timestamp': '1',
                         'Content-Type': 'application/new2'})
            object_server.http_connect = mock_http_connect(500)
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 202)
        finally:
            object_server.http_connect = old_http_connect

    def test_POST_quarantine_zbyte(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        objfile = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o')
        objfile.open()

        file_name = os.path.basename(objfile._data_file)
        with open(objfile._data_file) as fp:
            metadata = diskfile.read_metadata(fp)
        os.unlink(objfile._data_file)
        with open(objfile._data_file, 'w') as fp:
            diskfile.write_metadata(fp, metadata)

        self.assertEquals(os.listdir(objfile._datadir)[0], file_name)
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': normalize_timestamp(time())})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(objfile._data_file)))
        self.assertEquals(os.listdir(quar_dir)[0], file_name)

    def test_PUT_invalid_path(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_no_timestamp(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT',
                                                      'CONTENT_LENGTH': '0'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_no_content_type(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '6'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_invalid_content_type(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '6',
                     'Content-Type': '\xff\xff'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)
        self.assert_('Content-Type' in resp.body)

    def test_PUT_no_content_length(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        del req.headers['Content-Length']
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 411)

    def test_PUT_zero_content_length(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/octet-stream'})
        req.body = ''
        self.assertEquals(req.headers['Content-Length'], '0')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_common(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(open(objfile).read(), 'VERIFY')
        self.assertEquals(diskfile.read_metadata(objfile),
                          {'X-Timestamp': timestamp,
                           'Content-Length': '6',
                           'ETag': '0b4c12d7e0a73840c1c4f148fda3b037',
                           'Content-Type': 'application/octet-stream',
                           'name': '/a/c/o'})

    def test_PUT_overwrite(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(open(objfile).read(), 'VERIFY TWO')
        self.assertEquals(diskfile.read_metadata(objfile),
                          {'X-Timestamp': timestamp,
                           'Content-Length': '10',
                           'ETag': 'b381a4c5dab1eaa1eb9711fa647cd039',
                           'Content-Type': 'text/plain',
                           'name': '/a/c/o',
                           'Content-Encoding': 'gzip'})

    def test_PUT_old_timestamp(self):
        ts = time()
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(ts),
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': normalize_timestamp(ts),
                                     'Content-Type': 'text/plain',
                                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 409)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(ts - 1),
                                'Content-Type': 'text/plain',
                                'Content-Encoding': 'gzip'})
        req.body = 'VERIFY THREE'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 409)

    def test_PUT_no_etag(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'text/plain'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_invalid_etag(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'text/plain',
                     'ETag': 'invalid'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 422)

    def test_PUT_user_metadata(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'ETag': 'b114ab7b90d9ccac4bd5d99cc7ebb568',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY THREE'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(open(objfile).read(), 'VERIFY THREE')
        self.assertEquals(diskfile.read_metadata(objfile),
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
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'X-Container-Host': '1.2.3.4:0',
                         'X-Container-Partition': '3',
                         'X-Container-Device': 'sda1',
                         'X-Container-Timestamp': '1',
                         'Content-Type': 'application/new1',
                         'Content-Length': '0'})
            object_server.http_connect = mock_http_connect(201)
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 201)
            timestamp = normalize_timestamp(time())
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'X-Container-Host': '1.2.3.4:0',
                         'X-Container-Partition': '3',
                         'X-Container-Device': 'sda1',
                         'X-Container-Timestamp': '1',
                         'Content-Type': 'application/new1',
                         'Content-Length': '0'})
            object_server.http_connect = mock_http_connect(500)
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 201)
            timestamp = normalize_timestamp(time())
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'X-Container-Host': '1.2.3.4:0',
                         'X-Container-Partition': '3',
                         'X-Container-Device': 'sda1',
                         'X-Container-Timestamp': '1',
                         'Content-Type': 'application/new1',
                         'Content-Length': '0'})
            object_server.http_connect = mock_http_connect(500, with_exc=True)
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 201)
        finally:
            object_server.http_connect = old_http_connect

    def test_HEAD(self):
        # Test swift.obj.server.ObjectController.HEAD
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'application/x-test',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.content_length, 6)
        self.assertEquals(resp.content_type, 'application/x-test')
        self.assertEquals(resp.headers['content-type'], 'application/x-test')
        self.assertEquals(
            resp.headers['last-modified'],
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp))))
        self.assertEquals(resp.headers['etag'],
                          '"0b4c12d7e0a73840c1c4f148fda3b037"')
        self.assertEquals(resp.headers['x-object-meta-1'], 'One')
        self.assertEquals(resp.headers['x-object-meta-two'], 'Two')

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        os.unlink(objfile)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-length': '6'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

    def test_HEAD_quarantine_zbyte(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o')
        disk_file.open()

        file_name = os.path.basename(disk_file._data_file)
        with open(disk_file._data_file) as fp:
            metadata = diskfile.read_metadata(fp)
        os.unlink(disk_file._data_file)
        with open(disk_file._data_file, 'w') as fp:
            diskfile.write_metadata(fp, metadata)

        file_name = os.path.basename(disk_file._data_file)
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEquals(os.listdir(quar_dir)[0], file_name)

    def test_GET(self):
        # Test swift.obj.server.ObjectController.GET
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, 'VERIFY')
        self.assertEquals(resp.content_length, 6)
        self.assertEquals(resp.content_type, 'application/x-test')
        self.assertEquals(resp.headers['content-length'], '6')
        self.assertEquals(resp.headers['content-type'], 'application/x-test')
        self.assertEquals(
            resp.headers['last-modified'],
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp))))
        self.assertEquals(resp.headers['etag'],
                          '"0b4c12d7e0a73840c1c4f148fda3b037"')
        self.assertEquals(resp.headers['x-object-meta-1'], 'One')
        self.assertEquals(resp.headers['x-object-meta-two'], 'Two')

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        req.range = 'bytes=1-3'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 206)
        self.assertEquals(resp.body, 'ERI')
        self.assertEquals(resp.headers['content-length'], '3')

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        req.range = 'bytes=1-'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 206)
        self.assertEquals(resp.body, 'ERIFY')
        self.assertEquals(resp.headers['content-length'], '5')

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        req.range = 'bytes=-2'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 206)
        self.assertEquals(resp.body, 'FY')
        self.assertEquals(resp.headers['content-length'], '2')

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        os.unlink(objfile)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application:octet-stream',
                                'Content-Length': '6'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

    def test_GET_if_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={
                'If-Match': '"11111111111111111111111111111111", "%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={
                'If-Match':
                '"11111111111111111111111111111111", '
                '"22222222222222222222222222222222"'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

    def test_GET_if_none_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 304)
        self.assertEquals(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 304)
        self.assertEquals(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-None-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-None-Match':
                     '"11111111111111111111111111111111", '
                     '"%s"' % etag})
        resp = req.get_response(self.object_controller)
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
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 304)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 304)

    def test_GET_if_unmodified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 9))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 9))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

    def test_GET_quarantine(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o')
        disk_file.open()
        file_name = os.path.basename(disk_file._data_file)
        etag = md5()
        etag.update('VERIF')
        etag = etag.hexdigest()
        metadata = {'X-Timestamp': timestamp, 'name': '/a/c/o',
                    'Content-Length': 6, 'ETag': etag}
        diskfile.write_metadata(disk_file._fp, metadata)
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        body = resp.body  # actually does quarantining
        self.assertEquals(body, 'VERIFY')
        self.assertEquals(os.listdir(quar_dir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

    def test_GET_quarantine_zbyte(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o')
        disk_file.open()
        file_name = os.path.basename(disk_file._data_file)
        with open(disk_file._data_file) as fp:
            metadata = diskfile.read_metadata(fp)
        os.unlink(disk_file._data_file)
        with open(disk_file._data_file, 'w') as fp:
            diskfile.write_metadata(fp, metadata)

        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEquals(os.listdir(quar_dir)[0], file_name)

    def test_GET_quarantine_range(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o')
        disk_file.open()
        file_name = os.path.basename(disk_file._data_file)
        etag = md5()
        etag.update('VERIF')
        etag = etag.hexdigest()
        metadata = {'X-Timestamp': timestamp, 'name': '/a/c/o',
                    'Content-Length': 6, 'ETag': etag}
        diskfile.write_metadata(disk_file._fp, metadata)
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=0-4'  # partial
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        resp.body
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        self.assertFalse(os.path.isdir(quar_dir))
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=1-6'  # partial
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        resp.body
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        self.assertFalse(os.path.isdir(quar_dir))

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=0-14'  # full
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEquals(os.listdir(disk_file._datadir)[0], file_name)
        resp.body
        self.assertTrue(os.path.isdir(quar_dir))
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)

    @mock.patch("time.time", mock_time)
    def test_DELETE(self):
        # Test swift.obj.server.ObjectController.DELETE
        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)
        # self.assertRaises(KeyError, self.object_controller.DELETE, req)

        # The following should have created a tombstone file
        timestamp = normalize_timestamp(1000)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)
        ts_1000_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.ts')
        self.assertTrue(os.path.isfile(ts_1000_file))
        # There should now be a 1000 ts file.
        self.assertEquals(len(os.listdir(os.path.dirname(ts_1000_file))), 1)

        # The following should *not* have created a tombstone file.
        timestamp = normalize_timestamp(999)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 404)
        ts_999_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.ts')
        self.assertFalse(os.path.isfile(ts_999_file))
        self.assertTrue(os.path.isfile(ts_1000_file))
        self.assertEquals(len(os.listdir(os.path.dirname(ts_1000_file))), 1)

        timestamp = normalize_timestamp(1002)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4',
                            })
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        # There should now be 1000 ts and a 1001 data file.
        data_1002_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assertTrue(os.path.isfile(data_1002_file))
        self.assertEquals(len(os.listdir(os.path.dirname(data_1002_file))), 1)

        # The following should *not* have created a tombstone file.
        timestamp = normalize_timestamp(1001)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 409)
        ts_1001_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.ts')
        self.assertFalse(os.path.isfile(ts_1001_file))
        self.assertTrue(os.path.isfile(data_1002_file))
        self.assertEquals(len(os.listdir(os.path.dirname(ts_1001_file))), 1)

        timestamp = normalize_timestamp(1003)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)
        ts_1003_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.ts')
        self.assertTrue(os.path.isfile(ts_1003_file))
        self.assertEquals(len(os.listdir(os.path.dirname(ts_1003_file))), 1)

    def test_DELETE_container_updates(self):
        # Test swift.obj.server.ObjectController.DELETE and container
        # updates, making sure container update is called in the correct
        # state.
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4',
                            })
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        calls_made = [0]

        def our_container_update(*args, **kwargs):
            calls_made[0] += 1

        orig_cu = self.object_controller.container_update
        self.object_controller.container_update = our_container_update
        try:
            # The following request should return 409 (HTTP Conflict). A
            # tombstone file should not have been created with this timestamp.
            timestamp = normalize_timestamp(float(timestamp) - 1)
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 409)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.DATADIR, 'p',
                                  hash_path('a', 'c', 'o')),
                timestamp + '.ts')
            self.assertFalse(os.path.isfile(objfile))
            self.assertEquals(len(os.listdir(os.path.dirname(objfile))), 1)
            self.assertEquals(0, calls_made[0])

            # The following request should return 204, and the object should
            # be truly deleted (container update is performed) because this
            # timestamp is newer. A tombstone file should have been created
            # with this timestamp.
            sleep(.00001)
            timestamp = normalize_timestamp(time())
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 204)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.DATADIR, 'p',
                                  hash_path('a', 'c', 'o')),
                timestamp + '.ts')
            self.assert_(os.path.isfile(objfile))
            self.assertEquals(1, calls_made[0])
            self.assertEquals(len(os.listdir(os.path.dirname(objfile))), 1)

            # The following request should return a 404, as the object should
            # already have been deleted, but it should have also performed a
            # container update because the timestamp is newer, and a tombstone
            # file should also exist with this timestamp.
            sleep(.00001)
            timestamp = normalize_timestamp(time())
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 404)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.DATADIR, 'p',
                                  hash_path('a', 'c', 'o')),
                timestamp + '.ts')
            self.assert_(os.path.isfile(objfile))
            self.assertEquals(2, calls_made[0])
            self.assertEquals(len(os.listdir(os.path.dirname(objfile))), 1)

            # The following request should return a 404, as the object should
            # already have been deleted, and it should not have performed a
            # container update because the timestamp is older, or created a
            # tombstone file with this timestamp.
            timestamp = normalize_timestamp(float(timestamp) - 1)
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 404)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.DATADIR, 'p',
                                  hash_path('a', 'c', 'o')),
                timestamp + '.ts')
            self.assertFalse(os.path.isfile(objfile))
            self.assertEquals(2, calls_made[0])
            self.assertEquals(len(os.listdir(os.path.dirname(objfile))), 1)
        finally:
            self.object_controller.container_update = orig_cu

    def test_call_bad_request(self):
        # Test swift.obj.server.ObjectController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            """Sends args to outbuf"""
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

    def test_call_not_found(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

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

    def test_call_bad_method(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

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

    def test_call_name_collision(self):
        def my_check(*args):
            return False

        def my_hash_path(*args):
            return md5('collide').hexdigest()

        with mock.patch("swift.obj.diskfile.hash_path", my_hash_path):
            with mock.patch("swift.obj.server.check_object_creation",
                            my_check):
                inbuf = StringIO()
                errbuf = StringIO()
                outbuf = StringIO()

                def start_response(*args):
                    """Sends args to outbuf"""
                    outbuf.writelines(args)

                self.object_controller.__call__({
                    'REQUEST_METHOD': 'PUT',
                    'SCRIPT_NAME': '',
                    'PATH_INFO': '/sda1/p/a/c/o',
                    'SERVER_NAME': '127.0.0.1',
                    'SERVER_PORT': '8080',
                    'SERVER_PROTOCOL': 'HTTP/1.0',
                    'CONTENT_LENGTH': '0',
                    'CONTENT_TYPE': 'text/html',
                    'HTTP_X_TIMESTAMP': normalize_timestamp(1.2),
                    'wsgi.version': (1, 0),
                    'wsgi.url_scheme': 'http',
                    'wsgi.input': inbuf,
                    'wsgi.errors': errbuf,
                    'wsgi.multithread': False,
                    'wsgi.multiprocess': False,
                    'wsgi.run_once': False},
                    start_response)
                self.assertEquals(errbuf.getvalue(), '')
                self.assertEquals(outbuf.getvalue()[:4], '201 ')

                inbuf = StringIO()
                errbuf = StringIO()
                outbuf = StringIO()

                def start_response(*args):
                    """Sends args to outbuf"""
                    outbuf.writelines(args)

                self.object_controller.__call__({
                    'REQUEST_METHOD': 'PUT',
                    'SCRIPT_NAME': '',
                    'PATH_INFO': '/sda1/p/b/d/x',
                    'SERVER_NAME': '127.0.0.1',
                    'SERVER_PORT': '8080',
                    'SERVER_PROTOCOL': 'HTTP/1.0',
                    'CONTENT_LENGTH': '0',
                    'CONTENT_TYPE': 'text/html',
                    'HTTP_X_TIMESTAMP': normalize_timestamp(1.3),
                    'wsgi.version': (1, 0),
                    'wsgi.url_scheme': 'http',
                    'wsgi.input': inbuf,
                    'wsgi.errors': errbuf,
                    'wsgi.multithread': False,
                    'wsgi.multiprocess': False,
                    'wsgi.run_once': False},
                    start_response)
                self.assertEquals(errbuf.getvalue(), '')
                self.assertEquals(outbuf.getvalue()[:4], '403 ')

    def test_invalid_method_doesnt_exist(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.object_controller.__call__({
            'REQUEST_METHOD': 'method_doesnt_exist',
            'PATH_INFO': '/sda1/p/a/c/o'},
            start_response)
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.object_controller.__call__({'REQUEST_METHOD': '__init__',
                                         'PATH_INFO': '/sda1/p/a/c/o'},
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
                 'Connection: close\r\nX-Timestamp: %s\r\n'
                 'Transfer-Encoding: chunked\r\n\r\n'
                 '2\r\noh\r\n4\r\n hai\r\n0\r\n\r\n' % normalize_timestamp(
                     1.0))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('GET /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        response = fd.read()
        self.assertEquals(response, 'oh hai')
        killer.kill()

    def test_chunked_content_length_mismatch_zero(self):
        listener = listen(('localhost', 0))
        port = listener.getsockname()[1]
        killer = spawn(wsgi.server, listener, self.object_controller,
                       NullLogger())
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('PUT /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Content-Type: text/plain\r\n'
                 'Connection: close\r\nX-Timestamp: %s\r\n'
                 'Content-Length: 0\r\n'
                 'Transfer-Encoding: chunked\r\n\r\n'
                 '2\r\noh\r\n4\r\n hai\r\n0\r\n\r\n' % normalize_timestamp(
                     1.0))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('GET /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        response = fd.read()
        self.assertEquals(response, 'oh hai')
        killer.kill()

    def test_max_object_name_length(self):
        timestamp = normalize_timestamp(time())
        max_name_len = constraints.MAX_OBJECT_NAME_LENGTH
        req = Request.blank(
            '/sda1/p/a/c/' + ('1' * max_name_len),
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/' + ('2' * (max_name_len + 1)),
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)

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

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        self.object_controller.max_upload_time = 0.1
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
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

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': ShortBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 499)

    def test_bad_sinces(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'},
            body='    ')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Unmodified-Since': 'Not a valid date'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Not a valid date'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Unmodified-Since': 'Sat, 29 Oct 1000 19:43:31 GMT'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Sat, 29 Oct 1000 19:43:31 GMT'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

    def test_content_encoding(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'},
            body='    ')
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-encoding'], 'gzip')
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-encoding'], 'gzip')

    def test_manifest_header(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Length': '0',
                     'X-Object-Manifest': 'c/o/'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        self.assertEquals(
            diskfile.read_metadata(objfile),
            {'X-Timestamp': timestamp,
             'Content-Length': '0',
             'Content-Type': 'text/plain',
             'name': '/a/c/o',
             'X-Object-Manifest': 'c/o/',
             'ETag': 'd41d8cd98f00b204e9800998ecf8427e'})
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers.get('x-object-manifest'), 'c/o/')

    def test_manifest_head_request(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Length': '0',
                     'X-Object-Manifest': 'c/o/'})
        req.body = 'hi'
        resp = req.get_response(self.object_controller)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.DATADIR, 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.body, '')

    def test_async_update_http_connect(self):
        given_args = []

        def fake_http_connect(*args):
            given_args.extend(args)
            raise Exception('test')

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.async_update(
                'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                {'x-timestamp': '1', 'x-out': 'set'}, 'sda1')
        finally:
            object_server.http_connect = orig_http_connect
        self.assertEquals(
            given_args,
            ['127.0.0.1', '1234', 'sdc1', 1, 'PUT', '/a/c/o', {
                'x-timestamp': '1', 'x-out': 'set',
                'user-agent': 'obj-server %s' % os.getpid()}])

    def test_updating_multiple_delete_at_container_servers(self):
        self.object_controller.expiring_objects_account = 'exp'
        self.object_controller.expiring_objects_container_divisor = 60

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
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'Content-Type': 'application/burrito',
                     'Content-Length': '0',
                     'X-Container-Partition': '20',
                     'X-Container-Host': '1.2.3.4:5',
                     'X-Container-Device': 'sdb1',
                     'X-Delete-At': 9999999999,
                     'X-Delete-At-Container': '9999999960',
                     'X-Delete-At-Host': "10.1.1.1:6001,10.2.2.2:6002",
                     'X-Delete-At-Partition': '6237',
                     'X-Delete-At-Device': 'sdp,sdq'})

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            resp = req.get_response(self.object_controller)
        finally:
            object_server.http_connect = orig_http_connect

        self.assertEqual(resp.status_int, 201)

        http_connect_args.sort(key=operator.itemgetter('ipaddr'))

        self.assertEquals(len(http_connect_args), 3)
        self.assertEquals(
            http_connect_args[0],
            {'ipaddr': '1.2.3.4',
             'port': '5',
             'path': '/a/c/o',
             'device': 'sdb1',
             'partition': '20',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'application/burrito',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': '12345',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'obj-server %d' % os.getpid(),
                 'x-trans-id': '-'})})
        self.assertEquals(
            http_connect_args[1],
            {'ipaddr': '10.1.1.1',
             'port': '6001',
             'path': '/exp/9999999960/9999999999-a/c/o',
             'device': 'sdp',
             'partition': '6237',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'text/plain',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': '12345',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'obj-server %d' % os.getpid(),
                 'x-trans-id': '-'})})
        self.assertEquals(
            http_connect_args[2],
            {'ipaddr': '10.2.2.2',
             'port': '6002',
             'path': '/exp/9999999960/9999999999-a/c/o',
             'device': 'sdq',
             'partition': '6237',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'text/plain',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': '12345',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'obj-server %d' % os.getpid(),
                 'x-trans-id': '-'})})

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
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'Content-Type': 'application/burrito',
                     'Content-Length': '0',
                     'X-Container-Partition': '20',
                     'X-Container-Host': '1.2.3.4:5, 6.7.8.9:10',
                     'X-Container-Device': 'sdb1, sdf1'})

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.PUT(req)
        finally:
            object_server.http_connect = orig_http_connect

        http_connect_args.sort(key=operator.itemgetter('ipaddr'))

        self.assertEquals(len(http_connect_args), 2)
        self.assertEquals(
            http_connect_args[0],
            {'ipaddr': '1.2.3.4',
             'port': '5',
             'path': '/a/c/o',
             'device': 'sdb1',
             'partition': '20',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'application/burrito',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': '12345',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'obj-server %d' % os.getpid(),
                 'x-trans-id': '-'})})
        self.assertEquals(
            http_connect_args[1],
            {'ipaddr': '6.7.8.9',
             'port': '10',
             'path': '/a/c/o',
             'device': 'sdf1',
             'partition': '20',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'application/burrito',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': '12345',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'obj-server %d' % os.getpid(),
                 'x-trans-id': '-'})})

    def test_async_update_saves_on_exception(self):
        _prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_PREFIX = ''

        def fake_http_connect(*args):
            raise Exception('test')

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.async_update(
                'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                {'x-timestamp': '1', 'x-out': 'set'}, 'sda1')
        finally:
            object_server.http_connect = orig_http_connect
            utils.HASH_PATH_PREFIX = _prefix
        self.assertEquals(
            pickle.load(open(os.path.join(
                self.testdir, 'sda1', 'async_pending', 'a83',
                '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000'))),
            {'headers': {'x-timestamp': '1', 'x-out': 'set',
                         'user-agent': 'obj-server %s' % os.getpid()},
             'account': 'a', 'container': 'c', 'obj': 'o', 'op': 'PUT'})

    def test_async_update_saves_on_non_2xx(self):
        _prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_PREFIX = ''

        def fake_http_connect(status):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status

                def getresponse(self):
                    return self

                def read(self):
                    return ''

            return lambda *args: FakeConn(status)

        orig_http_connect = object_server.http_connect
        try:
            for status in (199, 300, 503):
                object_server.http_connect = fake_http_connect(status)
                self.object_controller.async_update(
                    'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                    {'x-timestamp': '1', 'x-out': str(status)}, 'sda1')
                self.assertEquals(
                    pickle.load(open(os.path.join(
                        self.testdir, 'sda1', 'async_pending', 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000'))),
                    {'headers': {'x-timestamp': '1', 'x-out': str(status),
                                 'user-agent': 'obj-server %s' % os.getpid()},
                     'account': 'a', 'container': 'c', 'obj': 'o',
                     'op': 'PUT'})
        finally:
            object_server.http_connect = orig_http_connect
            utils.HASH_PATH_PREFIX = _prefix

    def test_async_update_does_not_save_on_2xx(self):

        def fake_http_connect(status):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status

                def getresponse(self):
                    return self

                def read(self):
                    return ''

            return lambda *args: FakeConn(status)

        orig_http_connect = object_server.http_connect
        try:
            for status in (200, 299):
                object_server.http_connect = fake_http_connect(status)
                self.object_controller.async_update(
                    'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                    {'x-timestamp': '1', 'x-out': str(status)}, 'sda1')
                self.assertFalse(
                    os.path.exists(os.path.join(
                        self.testdir, 'sda1', 'async_pending', 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000')))
        finally:
            object_server.http_connect = orig_http_connect

    def test_container_update_no_async_update(self):
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234'})
        self.object_controller.container_update(
            'PUT', 'a', 'c', 'o', req, {
                'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                'x-content-type': 'text/plain', 'x-timestamp': '1'},
            'sda1')
        self.assertEquals(given_args, [])

    def test_container_update(self):
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '123',
                     'X-Container-Host': 'chost',
                     'X-Container-Partition': 'cpartition',
                     'X-Container-Device': 'cdevice'})
        self.object_controller.container_update(
            'PUT', 'a', 'c', 'o', req, {
                'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                'x-content-type': 'text/plain', 'x-timestamp': '1'},
            'sda1')
        self.assertEquals(
            given_args, [
                'PUT', 'a', 'c', 'o', 'chost', 'cpartition', 'cdevice', {
                    'x-size': '0',
                    'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-content-type': 'text/plain',
                    'x-timestamp': '1',
                    'x-trans-id': '123',
                    'referer': 'PUT http://localhost/v1/a/c/o'},
                'sda1'])

    def test_delete_at_update_on_put(self):
        # Test how delete_at_update works when issued a delete for old
        # expiration info after a new put with no new expiration info.
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '123'})
        self.object_controller.delete_at_update(
            'DELETE', 2, 'a', 'c', 'o', req, 'sda1')
        self.assertEquals(
            given_args, [
                'DELETE', '.expiring_objects', '0',
                '2-a/c/o', None, None, None,
                HeaderKeyDict({
                    'x-timestamp': '1',
                    'x-trans-id': '123',
                    'referer': 'PUT http://localhost/v1/a/c/o'}),
                'sda1'])

    def test_delete_at_negative(self):
        # Test how delete_at_update works when issued a delete for old
        # expiration info after a new put with no new expiration info.
        # Test negative is reset to 0
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234'})
        self.object_controller.delete_at_update(
            'DELETE', -2, 'a', 'c', 'o', req, 'sda1')
        self.assertEquals(given_args, [
            'DELETE', '.expiring_objects', '0', '0-a/c/o', None, None, None,
            HeaderKeyDict({
                'x-timestamp': '1',
                'x-trans-id': '1234',
                'referer': 'PUT http://localhost/v1/a/c/o'}),
            'sda1'])

    def test_delete_at_cap(self):
        # Test how delete_at_update works when issued a delete for old
        # expiration info after a new put with no new expiration info.
        # Test past cap is reset to cap
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234'})
        self.object_controller.delete_at_update(
            'DELETE', 12345678901, 'a', 'c', 'o', req, 'sda1')
        self.assertEquals(given_args, [
            'DELETE', '.expiring_objects', '9999936000', '9999999999-a/c/o',
            None, None, None,
            HeaderKeyDict({
                'x-timestamp': '1',
                'x-trans-id': '1234',
                'referer': 'PUT http://localhost/v1/a/c/o'}),
            'sda1'])

    def test_delete_at_update_put_with_info(self):
        # Keep next test,
        # test_delete_at_update_put_with_info_but_missing_container, in sync
        # with this one but just missing the X-Delete-At-Container header.
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Delete-At-Container': '0',
                     'X-Delete-At-Host': '127.0.0.1:1234',
                     'X-Delete-At-Partition': '3',
                     'X-Delete-At-Device': 'sdc1'})
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
                                                req, 'sda1')
        self.assertEquals(
            given_args, [
                'PUT', '.expiring_objects', '0', '2-a/c/o', '127.0.0.1:1234',
                '3', 'sdc1', HeaderKeyDict({
                    'x-size': '0',
                    'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-content-type': 'text/plain',
                    'x-timestamp': '1',
                    'x-trans-id': '1234',
                    'referer': 'PUT http://localhost/v1/a/c/o'}),
                'sda1'])

    def test_delete_at_update_put_with_info_but_missing_container(self):
        # Same as previous test, test_delete_at_update_put_with_info, but just
        # missing the X-Delete-At-Container header.
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.logger = FakeLogger()
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Delete-At-Host': '127.0.0.1:1234',
                     'X-Delete-At-Partition': '3',
                     'X-Delete-At-Device': 'sdc1'})
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
                                                req, 'sda1')
        self.assertEquals(
            self.object_controller.logger.log_dict['warning'],
            [(('X-Delete-At-Container header must be specified for expiring '
               'objects background PUT to work properly. Making best guess as '
               'to the container name for now.',), {})])

    def test_delete_at_update_delete(self):
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234'})
        self.object_controller.delete_at_update('DELETE', 2, 'a', 'c', 'o',
                                                req, 'sda1')
        self.assertEquals(
            given_args, [
                'DELETE', '.expiring_objects', '0', '2-a/c/o', None, None,
                None, HeaderKeyDict({
                    'x-timestamp': '1', 'x-trans-id': '1234',
                    'referer': 'DELETE http://localhost/v1/a/c/o'}),
                'sda1'])

    def test_POST_calls_delete_at(self):
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [])

        sleep(.00001)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/x-test'})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(given_args, [])

        sleep(.00001)
        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = str(int(time() + 1000))
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'application/x-test',
                     'X-Delete-At': delete_at_timestamp1})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(
            given_args, [
                'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
                req, 'sda1'])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        delete_at_timestamp2 = str(int(time() + 2000))
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Type': 'application/x-test',
                     'X-Delete-At': delete_at_timestamp2})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(
            given_args, [
                'PUT', int(delete_at_timestamp2), 'a', 'c', 'o',
                req, 'sda1',
                'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
                req, 'sda1'])

    def test_PUT_calls_delete_at(self):
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [])

        sleep(.00001)
        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = str(int(time() + 1000))
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': delete_at_timestamp1})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(
            given_args, [
                'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
                req, 'sda1'])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        delete_at_timestamp2 = str(int(time() + 2000))
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': delete_at_timestamp2})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(
            given_args, [
                'PUT', int(delete_at_timestamp2), 'a', 'c', 'o',
                req, 'sda1',
                'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
                req, 'sda1'])

    def test_GET_but_expired(self):
        test_time = time() + 10000
        delete_at_timestamp = int(test_time + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': normalize_timestamp(test_time)})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        orig_time = object_server.time.time
        try:
            t = time()
            object_server.time.time = lambda: t
            delete_at_timestamp = int(t + 1)
            delete_at_container = str(
                delete_at_timestamp /
                self.object_controller.expiring_objects_container_divisor *
                self.object_controller.expiring_objects_container_divisor)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(test_time - 1000),
                         'X-Delete-At': str(delete_at_timestamp),
                         'X-Delete-At-Container': delete_at_container,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
            req.body = 'TEST'
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 201)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Timestamp': normalize_timestamp(test_time)})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 200)
        finally:
            object_server.time.time = orig_time

        orig_time = object_server.time.time
        try:
            t = time() + 2
            object_server.time.time = lambda: t
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Timestamp': normalize_timestamp(t)})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_HEAD_but_expired(self):
        test_time = time() + 10000
        delete_at_timestamp = int(test_time + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'X-Timestamp': normalize_timestamp(test_time)})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 200)

        orig_time = object_server.time.time
        try:
            t = time()
            delete_at_timestamp = int(t + 1)
            delete_at_container = str(
                delete_at_timestamp /
                self.object_controller.expiring_objects_container_divisor *
                self.object_controller.expiring_objects_container_divisor)
            object_server.time.time = lambda: t
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(test_time - 1000),
                         'X-Delete-At': str(delete_at_timestamp),
                         'X-Delete-At-Container': delete_at_container,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
            req.body = 'TEST'
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 201)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'HEAD'},
                headers={'X-Timestamp': normalize_timestamp(test_time)})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 200)
        finally:
            object_server.time.time = orig_time

        orig_time = object_server.time.time
        try:
            t = time() + 2
            object_server.time.time = lambda: t
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'HEAD'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_POST_but_expired(self):
        test_time = time() + 10000
        delete_at_timestamp = int(test_time + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 1500)})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 202)

        delete_at_timestamp = int(time() + 1)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 1000),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        orig_time = object_server.time.time
        try:
            t = time() + 2
            object_server.time.time = lambda: t
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'POST'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_DELETE_but_expired(self):
        test_time = time() + 10000
        delete_at_timestamp = int(test_time + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        orig_time = object_server.time.time
        try:
            t = test_time + 100
            object_server.time.time = lambda: float(t)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_DELETE_if_delete_at(self):
        test_time = time() + 10000
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 99),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 98)})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)

        delete_at_timestamp = int(test_time - 1)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 97),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 95),
                     'X-If-Delete-At': str(int(test_time))})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 95)})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)

        delete_at_timestamp = int(test_time - 1)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 94),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': str(int(test_time))})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': delete_at_timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_calls_delete_at(self):
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = int(time() + 1000)
        delete_at_container1 = str(
            delete_at_timestamp1 /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': str(delete_at_timestamp1),
                     'X-Delete-At-Container': delete_at_container1})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [
            'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
            req, 'sda1'])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Type': 'application/octet-stream'})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(given_args, [
            'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
            req, 'sda1'])

    def test_PUT_delete_at_in_past(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'X-Delete-At': str(int(time() - 1)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)
        self.assertTrue('X-Delete-At in past' in resp.body)

    def test_POST_delete_at_in_past(self):
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time() + 1),
                     'X-Delete-At': str(int(time() - 1))})
        resp = req.get_response(self.object_controller)
        self.assertEquals(resp.status_int, 400)
        self.assertTrue('X-Delete-At in past' in resp.body)

    def test_REPLICATE_works(self):

        def fake_get_hashes(*args, **kwargs):
            return 0, {1: 2}

        def my_tpool_execute(func, *args, **kwargs):
            return func(*args, **kwargs)

        was_get_hashes = diskfile.get_hashes
        was_tpool_exe = tpool.execute
        try:
            diskfile.get_hashes = fake_get_hashes
            tpool.execute = my_tpool_execute
            req = Request.blank('/sda1/p/suff',
                                environ={'REQUEST_METHOD': 'REPLICATE'},
                                headers={})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 200)
            p_data = pickle.loads(resp.body)
            self.assertEquals(p_data, {1: 2})
        finally:
            tpool.execute = was_tpool_exe
            diskfile.get_hashes = was_get_hashes

    def test_REPLICATE_timeout(self):

        def fake_get_hashes(*args, **kwargs):
            raise Timeout()

        def my_tpool_execute(func, *args, **kwargs):
            return func(*args, **kwargs)

        was_get_hashes = diskfile.get_hashes
        was_tpool_exe = tpool.execute
        try:
            diskfile.get_hashes = fake_get_hashes
            tpool.execute = my_tpool_execute
            req = Request.blank('/sda1/p/suff',
                                environ={'REQUEST_METHOD': 'REPLICATE'},
                                headers={})
            self.assertRaises(Timeout, self.object_controller.REPLICATE, req)
        finally:
            tpool.execute = was_tpool_exe
            diskfile.get_hashes = was_get_hashes

    def test_PUT_with_full_drive(self):

        class IgnoredBody():

            def __init__(self):
                self.read_called = False

            def read(self, size=-1):
                if not self.read_called:
                    self.read_called = True
                    return 'VERIFY'
                return ''

        def fake_fallocate(fd, size):
            raise OSError(42, 'Unable to fallocate(%d)' % size)

        orig_fallocate = diskfile.fallocate
        try:
            diskfile.fallocate = fake_fallocate
            timestamp = normalize_timestamp(time())
            body_reader = IgnoredBody()
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT',
                         'wsgi.input': body_reader},
                headers={'X-Timestamp': timestamp,
                         'Content-Length': '6',
                         'Content-Type': 'application/octet-stream',
                         'Expect': '100-continue'})
            resp = req.get_response(self.object_controller)
            self.assertEquals(resp.status_int, 507)
            self.assertFalse(body_reader.read_called)
        finally:
            diskfile.fallocate = orig_fallocate

    def test_serv_reserv(self):
        # Test replication_server flag was set from configuration file.
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertEquals(
            object_server.ObjectController(conf).replication_server, None)
        for val in [True, '1', 'True', 'true']:
            conf['replication_server'] = val
            self.assertTrue(
                object_server.ObjectController(conf).replication_server)
        for val in [False, 0, '0', 'False', 'false', 'test_string']:
            conf['replication_server'] = val
            self.assertFalse(
                object_server.ObjectController(conf).replication_server)

    def test_list_allowed_methods(self):
        # Test list of allowed_methods
        obj_methods = ['DELETE', 'PUT', 'HEAD', 'GET', 'POST']
        repl_methods = ['REPLICATE']
        for method_name in obj_methods:
            method = getattr(self.object_controller, method_name)
            self.assertFalse(hasattr(method, 'replication'))
        for method_name in repl_methods:
            method = getattr(self.object_controller, method_name)
            self.assertEquals(method.replication, True)

    def test_correct_allowed_method(self):
        # Test correct work for allowed method using
        # swift.obj.server.ObjectController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            # Sends args to outbuf
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
        with mock.patch.object(self.object_controller, method,
                               new=mock_method):
            response = self.object_controller.__call__(env, start_response)
            self.assertEqual(response, method_res)

    def test_not_allowed_method(self):
        # Test correct work for NOT allowed method using
        # swift.obj.server.ObjectController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            # Sends args to outbuf
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
        with mock.patch.object(self.object_controller, method,
                               new=mock_method):
            mock_method.replication = True
            response = self.object_controller.__call__(env, start_response)
            self.assertEqual(response, answer)


if __name__ == '__main__':
    unittest.main()
