# coding: utf-8
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

import six.moves.cPickle as pickle
import datetime
import json
import errno
import operator
import os
import mock
import six
from six import StringIO
import unittest
import math
import random
from shutil import rmtree
from time import gmtime, strftime, time, struct_time
from tempfile import mkdtemp
from hashlib import md5
from collections import defaultdict
from contextlib import contextmanager
from textwrap import dedent

from eventlet import sleep, spawn, wsgi, Timeout, tpool, greenthread
from eventlet.green import httplib

from swift import __version__ as swift_version
from swift.common.http import is_success
from test import listen_zero
from test.unit import FakeLogger, debug_logger, mocked_http_conn, \
    make_timestamp_iter, DEFAULT_TEST_EC_TYPE, skip_if_no_xattrs, \
    connect_tcp, readuntil2crlfs, patch_policies, encode_frag_archive_bodies, \
    mock_check_drive
from swift.obj import server as object_server
from swift.obj import updater
from swift.obj import diskfile
from swift.common import utils, bufferedhttp
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, \
    NullLogger, storage_directory, public, replication, encode_timestamps, \
    Timestamp
from swift.common import constraints
from swift.common.swob import Request, WsgiBytesIO
from swift.common.splice import splice
from swift.common.storage_policy import (StoragePolicy, ECStoragePolicy,
                                         POLICIES, EC_POLICY)
from swift.common.exceptions import DiskFileDeviceUnavailable, \
    DiskFileNoSpace, DiskFileQuarantined
from swift.common.wsgi import init_request_processor


def mock_time(*args, **kwargs):
    return 5000.0


test_policies = [
    StoragePolicy(0, name='zero', is_default=True),
    ECStoragePolicy(1, name='one', ec_type=DEFAULT_TEST_EC_TYPE,
                    ec_ndata=10, ec_nparity=4),
]


@contextmanager
def fake_spawn():
    """
    Spawn and capture the result so we can later wait on it. This means we can
    test code executing in a greenthread but still wait() on the result to
    ensure that the method has completed.
    """

    greenlets = []

    def _inner_fake_spawn(func, *a, **kw):
        gt = greenthread.spawn(func, *a, **kw)
        greenlets.append(gt)
        return gt

    object_server.spawn = _inner_fake_spawn
    with mock.patch('swift.obj.server.spawn', _inner_fake_spawn):
        try:
            yield
        finally:
            for gt in greenlets:
                gt.wait()


class TestTpoolSize(unittest.TestCase):
    def test_default_config(self):
        with mock.patch('eventlet.tpool.set_num_threads') as mock_snt:
            object_server.ObjectController({})
        self.assertEqual([], mock_snt.mock_calls)

    def test_explicit_setting(self):
        conf = {'eventlet_tpool_num_threads': '17'}
        with mock.patch('eventlet.tpool.set_num_threads') as mock_snt:
            object_server.ObjectController(conf)
        self.assertEqual([mock.call(17)], mock_snt.mock_calls)

    def test_servers_per_port_no_explicit_setting(self):
        conf = {'servers_per_port': '3'}
        with mock.patch('eventlet.tpool.set_num_threads') as mock_snt:
            object_server.ObjectController(conf)
        self.assertEqual([mock.call(1)], mock_snt.mock_calls)

    def test_servers_per_port_with_explicit_setting(self):
        conf = {'eventlet_tpool_num_threads': '17',
                'servers_per_port': '3'}
        with mock.patch('eventlet.tpool.set_num_threads') as mock_snt:
            object_server.ObjectController(conf)
        self.assertEqual([mock.call(17)], mock_snt.mock_calls)

    def test_servers_per_port_empty(self):
        # run_wsgi is robust to this, so we should be too
        conf = {'servers_per_port': ''}
        with mock.patch('eventlet.tpool.set_num_threads') as mock_snt:
            object_server.ObjectController(conf)
        self.assertEqual([], mock_snt.mock_calls)


@patch_policies(test_policies)
class TestObjectController(unittest.TestCase):
    """Test swift.obj.server.ObjectController"""

    def setUp(self):
        """Set up for testing swift.object.server.ObjectController"""
        skip_if_no_xattrs()
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(self.tmpdir,
                                    'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sda1'))
        self.conf = {'devices': self.testdir, 'mount_check': 'false',
                     'container_update_timeout': 0.0}
        self.object_controller = object_server.ObjectController(
            self.conf, logger=debug_logger())
        self.object_controller.bytes_per_sync = 1
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.df_mgr = diskfile.DiskFileManager(self.conf,
                                               self.object_controller.logger)

        self.logger = debug_logger('test-object-controller')
        self.ts = make_timestamp_iter()
        self.ec_policies = [p for p in POLICIES if p.policy_type == EC_POLICY]

    def tearDown(self):
        """Tear down for testing swift.object.server.ObjectController"""
        rmtree(self.tmpdir)
        tpool.execute = self._orig_tpool_exc

    def _stage_tmp_dir(self, policy):
        mkdirs(os.path.join(self.testdir, 'sda1',
                            diskfile.get_tmp_dir(policy)))

    def iter_policies(self):
        for policy in POLICIES:
            self.policy = policy
            yield policy

    def check_all_api_methods(self, obj_name='o', alt_res=None):
        path = '/sda1/p/a/c/%s' % obj_name
        body = 'SPECIAL_STRING'

        op_table = {
            "PUT": (body, alt_res or 201, ''),  # create one
            "GET": ('', alt_res or 200, body),  # check it
            "POST": ('', alt_res or 202, ''),   # update it
            "HEAD": ('', alt_res or 200, ''),   # head it
            "DELETE": ('', alt_res or 204, '')  # delete it
        }

        for method in ["PUT", "GET", "POST", "HEAD", "DELETE"]:
            in_body, res, out_body = op_table[method]
            timestamp = normalize_timestamp(time())
            req = Request.blank(
                path, environ={'REQUEST_METHOD': method},
                headers={'X-Timestamp': timestamp,
                         'Content-Type': 'application/x-test'})
            req.body = in_body
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, res)
            if out_body and (200 <= res < 300):
                self.assertEqual(resp.body, out_body)

    def test_REQUEST_SPECIAL_CHARS(self):
        obj = 'special昆%20/%'
        self.check_all_api_methods(obj)

    def test_device_unavailable(self):
        def raise_disk_unavail(*args, **kwargs):
            raise DiskFileDeviceUnavailable()

        self.object_controller.get_diskfile = raise_disk_unavail
        self.check_all_api_methods(alt_res=507)

    def test_allowed_headers(self):
        dah = ['content-disposition', 'content-encoding', 'x-delete-at',
               'x-object-manifest', 'x-static-large-object']
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'allowed_headers': ','.join(['content-length'] + dah)}
        self.object_controller = object_server.ObjectController(
            conf, logger=debug_logger())
        self.assertEqual(self.object_controller.allowed_headers, set(dah))

    def test_POST_update_meta(self):
        # Test swift.obj.server.ObjectController.POST
        original_headers = self.object_controller.allowed_headers
        test_headers = 'content-encoding foo bar'.split()
        self.object_controller.allowed_headers = set(test_headers)
        put_timestamp = normalize_timestamp(time())
        headers = {'X-Timestamp': put_timestamp,
                   'Content-Type': 'application/x-test',
                   'Foo': 'fooheader',
                   'Baz': 'bazheader',
                   'X-Object-Sysmeta-Color': 'blue',
                   'X-Object-Transient-Sysmeta-Shape': 'circle',
                   'X-Object-Meta-1': 'One',
                   'X-Object-Meta-Two': 'Two'}
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers)
        req.body = 'VERIFY'
        etag = '"%s"' % md5('VERIFY').hexdigest()
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(resp.body)),
            'Etag': etag,
        })

        post_timestamp = normalize_timestamp(time())
        headers = {'X-Timestamp': post_timestamp,
                   'X-Object-Meta-3': 'Three',
                   'X-Object-Meta-4': 'Four',
                   'Content-Encoding': 'gzip',
                   'Foo': 'fooheader',
                   'Bar': 'barheader',
                   'Content-Type': 'application/x-test'}
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers=headers)
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(resp.body)),
            'X-Object-Sysmeta-Color': 'blue',
        })

        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        expected_headers = {
            'Content-Type': 'application/x-test',
            'Content-Length': '6',
            'Etag': etag,
            'X-Object-Sysmeta-Color': 'blue',
            'X-Object-Meta-3': 'Three',
            'X-Object-Meta-4': 'Four',
            'Foo': 'fooheader',
            'Bar': 'barheader',
            'Content-Encoding': 'gzip',
            'X-Backend-Timestamp': post_timestamp,
            'X-Timestamp': post_timestamp,
            'X-Backend-Data-Timestamp': put_timestamp,
            'X-Backend-Durable-Timestamp': put_timestamp,
            'Last-Modified': strftime(
                '%a, %d %b %Y %H:%M:%S GMT',
                gmtime(math.ceil(float(post_timestamp)))),
        }
        self.assertEqual(dict(resp.headers), expected_headers)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(dict(resp.headers), expected_headers)

        post_timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': post_timestamp,
                                     'X-Object-Sysmeta-Color': 'red',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(resp.body)),
            'X-Object-Sysmeta-Color': 'blue',
        })

        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'application/x-test',
            'Content-Length': '6',
            'Etag': etag,
            'X-Object-Sysmeta-Color': 'blue',
            'X-Backend-Timestamp': post_timestamp,
            'X-Timestamp': post_timestamp,
            'X-Backend-Data-Timestamp': put_timestamp,
            'X-Backend-Durable-Timestamp': put_timestamp,
            'Last-Modified': strftime(
                '%a, %d %b %Y %H:%M:%S GMT',
                gmtime(math.ceil(float(post_timestamp)))),
        })

        # test defaults
        self.object_controller.allowed_headers = original_headers
        put_timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': put_timestamp,
                                     'Content-Type': 'application/x-test',
                                     'Foo': 'fooheader',
                                     'X-Object-Sysmeta-Color': 'red',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Manifest': 'c/bar',
                                     'Content-Encoding': 'gzip',
                                     'Content-Disposition': 'bar',
                                     'X-Static-Large-Object': 'True',
                                     })
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(resp.body)),
            'Etag': etag,
        })

        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'application/x-test',
            'Content-Length': '6',
            'Etag': etag,
            'X-Object-Sysmeta-Color': 'red',
            'X-Object-Meta-1': 'One',
            'Content-Encoding': 'gzip',
            'X-Object-Manifest': 'c/bar',
            'Content-Disposition': 'bar',
            'X-Static-Large-Object': 'True',
            'X-Backend-Timestamp': put_timestamp,
            'X-Timestamp': put_timestamp,
            'X-Backend-Data-Timestamp': put_timestamp,
            'X-Backend-Durable-Timestamp': put_timestamp,
            'Last-Modified': strftime(
                '%a, %d %b %Y %H:%M:%S GMT',
                gmtime(math.ceil(float(put_timestamp)))),
        })

        post_timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': post_timestamp,
                                     'X-Object-Meta-3': 'Three',
                                     'Foo': 'fooheader',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(resp.body)),
            'X-Object-Sysmeta-Color': 'red',
        })

        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'application/x-test',
            'Content-Length': '6',
            'Etag': etag,
            'X-Object-Sysmeta-Color': 'red',
            'X-Object-Meta-3': 'Three',
            'X-Static-Large-Object': 'True',
            'X-Backend-Timestamp': post_timestamp,
            'X-Timestamp': post_timestamp,
            'X-Backend-Data-Timestamp': put_timestamp,
            'X-Backend-Durable-Timestamp': put_timestamp,
            'Last-Modified': strftime(
                '%a, %d %b %Y %H:%M:%S GMT',
                gmtime(math.ceil(float(post_timestamp)))),
        })

        # Test for empty metadata
        post_timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': post_timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-3': ''})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(resp.body)),
            'X-Object-Sysmeta-Color': 'red',
        })

        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(dict(resp.headers), {
            'Content-Type': 'application/x-test',
            'Content-Length': '6',
            'Etag': etag,
            'X-Object-Sysmeta-Color': 'red',
            'X-Object-Meta-3': '',
            'X-Static-Large-Object': 'True',
            'X-Backend-Timestamp': post_timestamp,
            'X-Timestamp': post_timestamp,
            'X-Backend-Data-Timestamp': put_timestamp,
            'X-Backend-Durable-Timestamp': put_timestamp,
            'Last-Modified': strftime(
                '%a, %d %b %Y %H:%M:%S GMT',
                gmtime(math.ceil(float(post_timestamp)))),
        })

    def test_POST_old_timestamp(self):
        ts = time()
        orig_timestamp = utils.Timestamp(ts).internal
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': orig_timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # Same timestamp should result in 409
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': orig_timestamp,
                                     'X-Object-Meta-3': 'Three',
                                     'X-Object-Meta-4': 'Four',
                                     'Content-Encoding': 'gzip',
                                     'Content-Type': 'application/x-test'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['X-Backend-Timestamp'], orig_timestamp)

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
        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['X-Backend-Timestamp'], orig_timestamp)

    def test_POST_conflicts_with_later_POST(self):
        t_put = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': t_put,
                                     'Content-Length': 0,
                                     'Content-Type': 'plain/text'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        t_post1 = next(self.ts).internal
        t_post2 = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': t_post2})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': t_post1})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 409)

        obj_dir = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')))

        ts_file = os.path.join(obj_dir, t_post2 + '.meta')
        self.assertTrue(os.path.isfile(ts_file))
        meta_file = os.path.join(obj_dir, t_post1 + '.meta')
        self.assertFalse(os.path.isfile(meta_file))

    def test_POST_not_exist(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/fail',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

    def test_POST_invalid_path(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': timestamp,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_no_timestamp(self):
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_bad_timestamp(self):
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': 'bad',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-2': 'Two',
                                     'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_container_connection(self):
        # Test that POST does call container_update and returns success
        # whether update to container server succeeds or fails
        def mock_http_connect(calls, response, with_exc=False):

            class FakeConn(object):

                def __init__(self, calls, status, with_exc):
                    self.calls = calls
                    self.status = status
                    self.reason = 'Fake'
                    self.host = '1.2.3.4'
                    self.port = '1234'
                    self.with_exc = with_exc

                def getresponse(self):
                    calls[0] += 1
                    if self.with_exc:
                        raise Exception('test')
                    return self

                def read(self, amt=None):
                    return ''

            return lambda *args, **kwargs: FakeConn(calls, response, with_exc)

        ts = time()
        timestamp = normalize_timestamp(ts)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Length': '0'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(ts + 1),
                     'X-Container-Host': '1.2.3.4:0',
                     'X-Container-Partition': '3',
                     'X-Container-Device': 'sda1',
                     'X-Container-Timestamp': '1',
                     'Content-Type': 'application/new1'})
        calls = [0]
        with mock.patch.object(object_server, 'http_connect',
                               mock_http_connect(calls, 202)):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(ts + 2),
                     'X-Container-Host': '1.2.3.4:0',
                     'X-Container-Partition': '3',
                     'X-Container-Device': 'sda1',
                     'X-Container-Timestamp': '1',
                     'Content-Type': 'application/new1'})
        calls = [0]
        with mock.patch.object(object_server, 'http_connect',
                               mock_http_connect(calls, 202, with_exc=True)):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(ts + 3),
                     'X-Container-Host': '1.2.3.4:0',
                     'X-Container-Partition': '3',
                     'X-Container-Device': 'sda1',
                     'X-Container-Timestamp': '1',
                     'Content-Type': 'application/new2'})
        calls = [0]
        with mock.patch.object(object_server, 'http_connect',
                               mock_http_connect(calls, 500)):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

    def _test_POST_container_updates(self, policy, update_etag=None):
        # Test that POST requests result in correct calls to container_update
        t = [next(self.ts) for _ in range(0, 5)]
        calls_made = []
        update_etag = update_etag or '098f6bcd4621d373cade4e832627b4f6'

        def mock_container_update(ctlr, op, account, container, obj, request,
                                  headers_out, objdevice, policy):
            calls_made.append((headers_out, policy))

        body = 'test'
        headers = {
            'X-Timestamp': t[1].internal,
            'Content-Type': 'application/octet-stream;swift_bytes=123456789',
            'X-Backend-Storage-Policy-Index': int(policy)}
        if policy.policy_type == EC_POLICY:
            # EC fragments will typically have a different size to the body and
            # for small bodies the fragments may be longer. For this test all
            # that matters is that the fragment and body lengths differ.
            body = body + 'ec_overhead'
            headers['X-Backend-Container-Update-Override-Etag'] = update_etag
            headers['X-Backend-Container-Update-Override-Size'] = '4'
            headers['X-Object-Sysmeta-Ec-Etag'] = update_etag
            headers['X-Object-Sysmeta-Ec-Content-Length'] = '4'
            headers['X-Object-Sysmeta-Ec-Frag-Index'] = 2
        headers['Content-Length'] = str(len(body))

        req = Request.blank('/sda1/p/a/c/o', body=body,
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers)
        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 201)
        self.assertEqual(1, len(calls_made))
        expected_headers = HeaderKeyDict({
            'x-size': '4',
            'x-content-type': 'application/octet-stream;swift_bytes=123456789',
            'x-timestamp': t[1].internal,
            'x-etag': update_etag})
        self.assertDictEqual(expected_headers, calls_made[0][0])
        self.assertEqual(policy, calls_made[0][1])

        # POST with no metadata newer than the data should return 409,
        # container update not expected
        calls_made = []
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': t[0].internal,
                     'X-Backend-Storage-Policy-Index': int(policy)})

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['x-backend-timestamp'],
                         t[1].internal)
        self.assertEqual(0, len(calls_made))

        # POST with newer metadata returns success and container update
        # is expected
        calls_made = []
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': t[3].internal,
                     'X-Backend-Storage-Policy-Index': int(policy)})

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 202)
        self.assertEqual(1, len(calls_made))
        expected_headers = HeaderKeyDict({
            'x-size': '4',
            'x-content-type': 'application/octet-stream;swift_bytes=123456789',
            'x-timestamp': t[1].internal,
            'x-content-type-timestamp': t[1].internal,
            'x-meta-timestamp': t[3].internal,
            'x-etag': update_etag})
        self.assertDictEqual(expected_headers, calls_made[0][0])
        self.assertEqual(policy, calls_made[0][1])

        # POST with no metadata newer than existing metadata should return
        # 409, container update not expected
        calls_made = []
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': t[2].internal,
                     'X-Backend-Storage-Policy-Index': int(policy)})

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['x-backend-timestamp'],
                         t[3].internal)
        self.assertEqual(0, len(calls_made))

        # POST with newer content-type but older metadata returns success
        # and container update is expected newer content-type should have
        # existing swift_bytes appended
        calls_made = []
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'X-Timestamp': t[2].internal,
                                'Content-Type': 'text/plain',
                                'Content-Type-Timestamp': t[2].internal,
                                'X-Backend-Storage-Policy-Index': int(policy)
                            })

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 202)
        self.assertEqual(1, len(calls_made))
        expected_headers = HeaderKeyDict({
            'x-size': '4',
            'x-content-type': 'text/plain;swift_bytes=123456789',
            'x-timestamp': t[1].internal,
            'x-content-type-timestamp': t[2].internal,
            'x-meta-timestamp': t[3].internal,
            'x-etag': update_etag})
        self.assertDictEqual(expected_headers, calls_made[0][0])
        self.assertEqual(policy, calls_made[0][1])

        # POST with older content-type but newer metadata returns success
        # and container update is expected
        calls_made = []
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'X-Timestamp': t[4].internal,
                                'Content-Type': 'older',
                                'Content-Type-Timestamp': t[1].internal,
                                'X-Backend-Storage-Policy-Index': int(policy)
                            })

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 202)
        self.assertEqual(1, len(calls_made))
        expected_headers = HeaderKeyDict({
            'x-size': '4',
            'x-content-type': 'text/plain;swift_bytes=123456789',
            'x-timestamp': t[1].internal,
            'x-content-type-timestamp': t[2].internal,
            'x-meta-timestamp': t[4].internal,
            'x-etag': update_etag})
        self.assertDictEqual(expected_headers, calls_made[0][0])
        self.assertEqual(policy, calls_made[0][1])

        # POST with same-time content-type and metadata returns 409
        # and no container update is expected
        calls_made = []
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'X-Timestamp': t[4].internal,
                                'Content-Type': 'ignored',
                                'Content-Type-Timestamp': t[2].internal,
                                'X-Backend-Storage-Policy-Index': int(policy)
                            })

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 409)
        self.assertEqual(0, len(calls_made))

        # POST with implicit newer content-type but older metadata
        # returns success and container update is expected,
        # update reports existing metadata timestamp
        calls_made = []
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'X-Timestamp': t[3].internal,
                                'Content-Type': 'text/newer',
                                'X-Backend-Storage-Policy-Index': int(policy)
                            })

        with mock.patch('swift.obj.server.ObjectController.container_update',
                        mock_container_update):
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 202)
        self.assertEqual(1, len(calls_made))
        expected_headers = HeaderKeyDict({
            'x-size': '4',
            'x-content-type': 'text/newer;swift_bytes=123456789',
            'x-timestamp': t[1].internal,
            'x-content-type-timestamp': t[3].internal,
            'x-meta-timestamp': t[4].internal,
            'x-etag': update_etag})
        self.assertDictEqual(expected_headers, calls_made[0][0])
        self.assertEqual(policy, calls_made[0][1])

    def test_POST_container_updates_with_replication_policy(self):
        self._test_POST_container_updates(POLICIES[0])

    def test_POST_container_updates_with_EC_policy(self):
        self._test_POST_container_updates(
            POLICIES[1], update_etag='override_etag')

    def test_POST_container_updates_precedence(self):
        # Verify correct etag and size being sent with container updates for a
        # PUT and for a subsequent POST.

        def do_test(body, headers, policy):
            def mock_container_update(ctlr, op, account, container, obj, req,
                                      headers_out, objdevice, policy):
                calls_made.append((headers_out, policy))
            calls_made = []
            ts_put = next(self.ts)

            # make PUT with given headers and verify correct etag is sent in
            # container update
            headers.update({
                'Content-Type':
                    'application/octet-stream;swift_bytes=123456789',
                'X-Backend-Storage-Policy-Index': int(policy),
                'X-Object-Sysmeta-Ec-Frag-Index': 2,
                'X-Timestamp': ts_put.internal,
                'Content-Length': len(body)})

            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers, body=body)

            with mock.patch(
                    'swift.obj.server.ObjectController.container_update',
                    mock_container_update):
                resp = req.get_response(self.object_controller)

            self.assertEqual(resp.status_int, 201)
            self.assertEqual(1, len(calls_made))
            expected_headers = HeaderKeyDict({
                'x-size': '4',
                'x-content-type':
                    'application/octet-stream;swift_bytes=123456789',
                'x-timestamp': ts_put.internal,
                'x-etag': 'expected'})
            self.assertDictEqual(expected_headers, calls_made[0][0])
            self.assertEqual(policy, calls_made[0][1])

            # make a POST and verify container update has the same etag
            calls_made = []
            ts_post = next(self.ts)
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                headers={'X-Timestamp': ts_post.internal,
                         'X-Backend-Storage-Policy-Index': int(policy)})

            with mock.patch(
                    'swift.obj.server.ObjectController.container_update',
                    mock_container_update):
                resp = req.get_response(self.object_controller)

            self.assertEqual(resp.status_int, 202)
            self.assertEqual(1, len(calls_made))
            expected_headers.update({
                'x-content-type-timestamp': ts_put.internal,
                'x-meta-timestamp': ts_post.internal})
            self.assertDictEqual(expected_headers, calls_made[0][0])
            self.assertEqual(policy, calls_made[0][1])

        # sanity check - EC headers are ok
        headers = {
            'X-Backend-Container-Update-Override-Etag': 'expected',
            'X-Backend-Container-Update-Override-Size': '4',
            'X-Object-Sysmeta-Ec-Etag': 'expected',
            'X-Object-Sysmeta-Ec-Content-Length': '4'}
        do_test('test ec frag longer than 4', headers, POLICIES[1])

        # middleware overrides take precedence over EC/older overrides
        headers = {
            'X-Backend-Container-Update-Override-Etag': 'unexpected',
            'X-Backend-Container-Update-Override-Size': '3',
            'X-Object-Sysmeta-Ec-Etag': 'unexpected',
            'X-Object-Sysmeta-Ec-Content-Length': '3',
            'X-Object-Sysmeta-Container-Update-Override-Etag': 'expected',
            'X-Object-Sysmeta-Container-Update-Override-Size': '4'}
        do_test('test ec frag longer than 4', headers, POLICIES[1])

        # overrides with replication policy
        headers = {
            'X-Object-Sysmeta-Container-Update-Override-Etag': 'expected',
            'X-Object-Sysmeta-Container-Update-Override-Size': '4'}
        do_test('longer than 4', headers, POLICIES[0])

        # middleware overrides take precedence over EC/older overrides with
        # replication policy
        headers = {
            'X-Backend-Container-Update-Override-Etag': 'unexpected',
            'X-Backend-Container-Update-Override-Size': '3',
            'X-Object-Sysmeta-Container-Update-Override-Etag': 'expected',
            'X-Object-Sysmeta-Container-Update-Override-Size': '4'}
        do_test('longer than 4', headers, POLICIES[0])

    def _test_PUT_then_POST_async_pendings(self, policy, update_etag=None):
        # Test that PUT and POST requests result in distinct async pending
        # files when sync container update fails.
        def fake_http_connect(*args):
            raise Exception('test')

        device_dir = os.path.join(self.testdir, 'sda1')
        t_put = next(self.ts)
        update_etag = update_etag or '098f6bcd4621d373cade4e832627b4f6'

        put_headers = {
            'X-Trans-Id': 'put_trans_id',
            'X-Timestamp': t_put.internal,
            'Content-Type': 'application/octet-stream;swift_bytes=123456789',
            'Content-Length': '4',
            'X-Backend-Storage-Policy-Index': int(policy),
            'X-Container-Host': 'chost:cport',
            'X-Container-Partition': 'cpartition',
            'X-Container-Device': 'cdevice'}
        if policy.policy_type == EC_POLICY:
            put_headers.update({
                'X-Object-Sysmeta-Ec-Frag-Index': '2',
                'X-Backend-Container-Update-Override-Etag': update_etag,
                'X-Object-Sysmeta-Ec-Etag': update_etag})

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=put_headers, body='test')

        with mock.patch('swift.obj.server.http_connect', fake_http_connect), \
                mock.patch('swift.common.utils.HASH_PATH_PREFIX', ''), \
                fake_spawn():
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 201)

        async_pending_file_put = os.path.join(
            device_dir, diskfile.get_async_dir(policy), 'a83',
            '06fbf0b514e5199dfc4e00f42eb5ea83-%s' % t_put.internal)
        self.assertTrue(os.path.isfile(async_pending_file_put),
                        'Expected %s to be a file but it is not.'
                        % async_pending_file_put)
        expected_put_headers = {
            'Referer': 'PUT http://localhost/sda1/p/a/c/o',
            'X-Trans-Id': 'put_trans_id',
            'X-Timestamp': t_put.internal,
            'X-Content-Type': 'application/octet-stream;swift_bytes=123456789',
            'X-Size': '4',
            'X-Etag': '098f6bcd4621d373cade4e832627b4f6',
            'User-Agent': 'object-server %s' % os.getpid(),
            'X-Backend-Storage-Policy-Index': '%d' % int(policy)}
        if policy.policy_type == EC_POLICY:
            expected_put_headers['X-Etag'] = update_etag
        self.assertDictEqual(
            pickle.load(open(async_pending_file_put)),
            {'headers': expected_put_headers,
             'account': 'a', 'container': 'c', 'obj': 'o', 'op': 'PUT'})

        # POST with newer metadata returns success and container update
        # is expected
        t_post = next(self.ts)
        post_headers = {
            'X-Trans-Id': 'post_trans_id',
            'X-Timestamp': t_post.internal,
            'Content-Type': 'application/other',
            'X-Backend-Storage-Policy-Index': int(policy),
            'X-Container-Host': 'chost:cport',
            'X-Container-Partition': 'cpartition',
            'X-Container-Device': 'cdevice'}
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers=post_headers)

        with mock.patch('swift.obj.server.http_connect', fake_http_connect), \
                mock.patch('swift.common.utils.HASH_PATH_PREFIX', ''), \
                fake_spawn():
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 202)

        self.maxDiff = None
        # check async pending file for PUT is still intact
        self.assertDictEqual(
            pickle.load(open(async_pending_file_put)),
            {'headers': expected_put_headers,
             'account': 'a', 'container': 'c', 'obj': 'o', 'op': 'PUT'})

        # check distinct async pending file for POST
        async_pending_file_post = os.path.join(
            device_dir, diskfile.get_async_dir(policy), 'a83',
            '06fbf0b514e5199dfc4e00f42eb5ea83-%s' % t_post.internal)
        self.assertTrue(os.path.isfile(async_pending_file_post),
                        'Expected %s to be a file but it is not.'
                        % async_pending_file_post)
        expected_post_headers = {
            'Referer': 'POST http://localhost/sda1/p/a/c/o',
            'X-Trans-Id': 'post_trans_id',
            'X-Timestamp': t_put.internal,
            'X-Content-Type': 'application/other;swift_bytes=123456789',
            'X-Size': '4',
            'X-Etag': '098f6bcd4621d373cade4e832627b4f6',
            'User-Agent': 'object-server %s' % os.getpid(),
            'X-Backend-Storage-Policy-Index': '%d' % int(policy),
            'X-Meta-Timestamp': t_post.internal,
            'X-Content-Type-Timestamp': t_post.internal,
        }
        if policy.policy_type == EC_POLICY:
            expected_post_headers['X-Etag'] = update_etag
        self.assertDictEqual(
            pickle.load(open(async_pending_file_post)),
            {'headers': expected_post_headers,
             'account': 'a', 'container': 'c', 'obj': 'o', 'op': 'PUT'})

        # verify that only the POST (most recent) async update gets sent by the
        # object updater, and that both update files are deleted
        with mock.patch(
            'swift.obj.updater.ObjectUpdater.object_update') as mock_update, \
                mock.patch('swift.obj.updater.dump_recon_cache'):
            object_updater = updater.ObjectUpdater(
                {'devices': self.testdir,
                 'mount_check': 'false'}, logger=debug_logger())
            node = {'id': 1}
            mock_ring = mock.MagicMock()
            mock_ring.get_nodes.return_value = (99, [node])
            object_updater.container_ring = mock_ring
            mock_update.return_value = ((True, 1))
            object_updater.run_once()
        self.assertEqual(1, mock_update.call_count)
        self.assertEqual((node, 99, 'PUT', '/a/c/o'),
                         mock_update.call_args_list[0][0][0:4])
        actual_headers = mock_update.call_args_list[0][0][4]
        # User-Agent is updated.
        expected_post_headers['User-Agent'] = 'object-updater %s' % os.getpid()
        self.assertDictEqual(expected_post_headers, actual_headers)
        self.assertFalse(
            os.listdir(os.path.join(
                device_dir, diskfile.get_async_dir(policy))))

    def test_PUT_then_POST_async_pendings_with_repl_policy(self):
        self._test_PUT_then_POST_async_pendings(POLICIES[0])

    def test_PUT_then_POST_async_pendings_with_EC_policy(self):
        self._test_PUT_then_POST_async_pendings(
            POLICIES[1], update_etag='override_etag')

    def test_POST_quarantine_zbyte(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        objfile = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o',
                                           policy=POLICIES.legacy)
        objfile.open()
        file_name = os.path.basename(objfile._data_file)
        with open(objfile._data_file) as fp:
            metadata = diskfile.read_metadata(fp)
        os.unlink(objfile._data_file)
        with open(objfile._data_file, 'w') as fp:
            diskfile.write_metadata(fp, metadata)
        self.assertEqual(os.listdir(objfile._datadir)[0], file_name)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time())})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(objfile._data_file)))
        self.assertEqual(os.listdir(quar_dir)[0], file_name)

    def test_PUT_invalid_path(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_no_timestamp(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT',
                                                      'CONTENT_LENGTH': '0'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_bad_timestamp(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 'bad'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_no_content_type(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '6'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_invalid_content_type(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '6',
                     'Content-Type': '\xff\xff'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue('Content-Type' in resp.body)

    def test_PUT_no_content_length(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        del req.headers['Content-Length']
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 411)

    def test_PUT_zero_content_length(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/octet-stream'})
        req.body = ''
        self.assertEqual(req.headers['Content-Length'], '0')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

    def test_PUT_bad_transfer_encoding(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        req.headers['Transfer-Encoding'] = 'bad'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_if_none_match_star(self):
        # First PUT should succeed
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': next(self.ts).normal,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': '*'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        # File should already exist so it should fail
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': next(self.ts).normal,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': '*'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': next(self.ts).normal})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': next(self.ts).normal,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': '*'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

    def test_PUT_if_none_match(self):
        # PUT with if-none-match set and nothing there should succeed
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': 'notthere'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        # PUT with if-none-match of the object etag should fail
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': '0b4c12d7e0a73840c1c4f148fda3b037'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

    def test_PUT_if_none_match_but_expired(self):
        inital_put = next(self.ts)
        put_before_expire = next(self.ts)
        delete_at_timestamp = int(next(self.ts))
        time_after_expire = next(self.ts)
        put_after_expire = next(self.ts)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': inital_put.normal,
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # PUT again before object has expired should fail
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': put_before_expire.normal,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': '*'})
        req.body = 'TEST'
        with mock.patch("swift.obj.server.time.time",
                        lambda: float(put_before_expire.normal)):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        # PUT again after object has expired should succeed
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': put_after_expire.normal,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'If-None-Match': '*'})
        req.body = 'TEST'
        with mock.patch("swift.obj.server.time.time",
                        lambda: float(time_after_expire.normal)):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

    def test_PUT_common(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream',
                     'x-object-meta-test': 'one',
                     'Custom-Header': '*',
                     'X-Backend-Replication-Headers':
                     'Content-Type Content-Length'})
        req.body = 'VERIFY'
        with mock.patch.object(self.object_controller, 'allowed_headers',
                               ['Custom-Header']):
            self.object_controller.allowed_headers = ['Custom-Header']
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]),
                              'p', hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': utils.Timestamp(timestamp).internal,
                          'Content-Length': '6',
                          'ETag': '0b4c12d7e0a73840c1c4f148fda3b037',
                          'Content-Type': 'application/octet-stream',
                          'name': '/a/c/o',
                          'X-Object-Meta-Test': 'one',
                          'Custom-Header': '*'})

    def test_PUT_overwrite(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY TWO')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': utils.Timestamp(timestamp).internal,
                          'Content-Length': '10',
                          'ETag': 'b381a4c5dab1eaa1eb9711fa647cd039',
                          'Content-Type': 'text/plain',
                          'name': '/a/c/o',
                          'Content-Encoding': 'gzip'})

    def test_PUT_overwrite_to_older_ts_success(self):
        old_timestamp = next(self.ts)
        new_timestamp = next(self.ts)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': old_timestamp.normal,
                     'Content-Length': '0',
                     'Content-Type': 'application/octet-stream'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': new_timestamp.normal,
                     'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            new_timestamp.internal + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY TWO')
        self.assertEqual(
            diskfile.read_metadata(objfile),
            {'X-Timestamp': new_timestamp.internal,
             'Content-Length': '10',
             'ETag': 'b381a4c5dab1eaa1eb9711fa647cd039',
             'Content-Type': 'text/plain',
             'name': '/a/c/o',
             'Content-Encoding': 'gzip'})

    def test_PUT_overwrite_to_newer_ts_failed(self):
        old_timestamp = next(self.ts)
        new_timestamp = next(self.ts)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': new_timestamp.normal,
                     'Content-Length': '0',
                     'Content-Type': 'application/octet-stream'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': old_timestamp.normal,
                     'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'

        with mock.patch(
                'swift.obj.diskfile.BaseDiskFile.create') as mock_create:
            resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 409)
        self.assertEqual(mock_create.call_count, 0)

        # data file doesn't exist there (This is sanity because
        # if .data written unexpectedly, it will be removed
        # by cleanup_ondisk_files)
        datafile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            old_timestamp.internal + '.data')
        self.assertFalse(os.path.exists(datafile))

        # ts file sitll exists
        tsfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            new_timestamp.internal + '.ts')
        self.assertTrue(os.path.isfile(tsfile))

    def test_PUT_overwrite_w_delete_at(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'X-Delete-At': 9999999999,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY TWO')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': utils.Timestamp(timestamp).internal,
                          'Content-Length': '10',
                          'ETag': 'b381a4c5dab1eaa1eb9711fa647cd039',
                          'Content-Type': 'text/plain',
                          'name': '/a/c/o',
                          'Content-Encoding': 'gzip'})

    def test_PUT_old_timestamp(self):
        ts = time()
        orig_timestamp = utils.Timestamp(ts).internal
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': orig_timestamp,
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': normalize_timestamp(ts),
                                     'Content-Type': 'text/plain',
                                     'Content-Encoding': 'gzip'})
        req.body = 'VERIFY TWO'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['X-Backend-Timestamp'], orig_timestamp)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(ts - 1),
                                'Content-Type': 'text/plain',
                                'Content-Encoding': 'gzip'})
        req.body = 'VERIFY THREE'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['X-Backend-Timestamp'], orig_timestamp)

    def test_PUT_new_object_really_old_timestamp(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '-1',  # 1969-12-31 23:59:59
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '1',  # 1970-01-01 00:00:01
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

    def test_PUT_object_really_new_timestamp(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '9999999999',  # 2286-11-20 17:46:40
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # roll over to 11 digits before the decimal
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '10000000000',
                     'Content-Length': '6',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_no_etag(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'text/plain'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

    def test_PUT_invalid_etag(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'text/plain',
                     'ETag': 'invalid'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 422)

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
        self.assertEqual(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY THREE')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': utils.Timestamp(timestamp).internal,
                          'Content-Length': '12',
                          'ETag': 'b114ab7b90d9ccac4bd5d99cc7ebb568',
                          'Content-Type': 'text/plain',
                          'name': '/a/c/o',
                          'X-Object-Meta-1': 'One',
                          'X-Object-Meta-Two': 'Two'})

    def test_PUT_etag_in_footer(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'Etag': 'other-etag',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        obj_etag = md5("obj data").hexdigest()
        footer_meta = json.dumps({"Etag": obj_etag})
        footer_meta_cksum = md5(footer_meta).hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "obj data",
            "--boundary",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.etag, obj_etag)
        self.assertEqual(resp.status_int, 201)

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        with open(objfile) as fh:
            self.assertEqual(fh.read(), "obj data")

    def _check_container_override_etag_preference(self, override_headers,
                                                  override_footers):
        def mock_container_update(ctlr, op, account, container, obj, req,
                                  headers_out, objdevice, policy):
            calls_made.append((headers_out, policy))
        calls_made = []
        ts_put = next(self.ts)

        headers = {
            'X-Timestamp': ts_put.internal,
            'Content-Type': 'text/plain',
            'Transfer-Encoding': 'chunked',
            'Etag': 'other-etag',
            'X-Backend-Obj-Metadata-Footer': 'yes',
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'}
        headers.update(override_headers)
        req = Request.blank(
            '/sda1/p/a/c/o', headers=headers,
            environ={'REQUEST_METHOD': 'PUT'})

        obj_etag = md5("obj data").hexdigest()
        footers = {'Etag': obj_etag}
        footers.update(override_footers)
        footer_meta = json.dumps(footers)
        footer_meta_cksum = md5(footer_meta).hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "obj data",
            "--boundary",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        with mock.patch(
                'swift.obj.server.ObjectController.container_update',
                mock_container_update):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.etag, obj_etag)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(1, len(calls_made))
        self.assertEqual({
            'X-Size': str(len('obj data')),
            'X-Etag': 'update-etag',
            'X-Content-Type': 'text/plain',
            'X-Timestamp': ts_put.internal,
        }, calls_made[0][0])
        self.assertEqual(POLICIES[0], calls_made[0][1])

    def test_override_etag_lone_header_footer(self):
        self._check_container_override_etag_preference(
            {'X-Backend-Container-Update-Override-Etag': 'update-etag'}, {})
        self._check_container_override_etag_preference(
            {}, {'X-Backend-Container-Update-Override-Etag': 'update-etag'})
        self._check_container_override_etag_preference(
            {'X-Object-Sysmeta-Container-Update-Override-Etag':
             'update-etag'}, {})
        self._check_container_override_etag_preference(
            {}, {'X-Object-Sysmeta-Container-Update-Override-Etag':
                 'update-etag'}),

    def test_override_etag_footer_trumps_header(self):
        self._check_container_override_etag_preference(
            {'X-Backend-Container-Update-Override-Etag': 'ignored-etag'},
            {'X-Backend-Container-Update-Override-Etag': 'update-etag'})
        self._check_container_override_etag_preference(
            {'X-Object-Sysmeta-Container-Update-Override-Etag':
             'ignored-etag'},
            {'X-Object-Sysmeta-Container-Update-Override-Etag':
             'update-etag'})

    def test_override_etag_sysmeta_trumps_backend(self):
        self._check_container_override_etag_preference(
            {'X-Backend-Container-Update-Override-Etag': 'ignored-etag',
             'X-Object-Sysmeta-Container-Update-Override-Etag':
             'update-etag'}, {})
        self._check_container_override_etag_preference(
            {}, {'X-Backend-Container-Update-Override-Etag': 'ignored-etag',
                 'X-Object-Sysmeta-Container-Update-Override-Etag':
                 'update-etag'})

    def test_override_etag_sysmeta_header_trumps_backend_footer(self):
        headers = {'X-Object-Sysmeta-Container-Update-Override-Etag':
                   'update-etag'}
        footers = {'X-Backend-Container-Update-Override-Etag':
                   'ignored-etag'}
        self._check_container_override_etag_preference(headers, footers)

    def test_override_etag_sysmeta_footer_trumps_backend_header(self):
        headers = {'X-Backend-Container-Update-Override-Etag':
                   'ignored-etag'}
        footers = {'X-Object-Sysmeta-Container-Update-Override-Etag':
                   'update-etag'}
        self._check_container_override_etag_preference(headers, footers)

    def test_PUT_etag_in_footer_mismatch(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        footer_meta = json.dumps({"Etag": md5("green").hexdigest()})
        footer_meta_cksum = md5(footer_meta).hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "blue",
            "--boundary",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 422)

    def test_PUT_meta_in_footer(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'X-Object-Meta-X': 'Z',
                     'X-Object-Sysmeta-X': 'Z',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        footer_meta = json.dumps({
            'X-Object-Meta-X': 'Y',
            'X-Object-Sysmeta-X': 'Y',
        })
        footer_meta_cksum = md5(footer_meta).hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "stuff stuff stuff",
            "--boundary",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp},
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.headers.get('X-Object-Meta-X'), 'Y')
        self.assertEqual(resp.headers.get('X-Object-Sysmeta-X'), 'Y')

    def test_PUT_missing_footer_checksum(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        footer_meta = json.dumps({"Etag": md5("obj data").hexdigest()})

        req.body = "\r\n".join((
            "--boundary",
            "",
            "obj data",
            "--boundary",
            # no Content-MD5
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_bad_footer_checksum(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        footer_meta = json.dumps({"Etag": md5("obj data").hexdigest()})
        bad_footer_meta_cksum = md5(footer_meta + "bad").hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "obj data",
            "--boundary",
            "Content-MD5: " + bad_footer_meta_cksum,
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 422)

    def test_PUT_bad_footer_json(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        footer_meta = "{{{[[{{[{[[{[{[[{{{[{{{{[[{{[{["
        footer_meta_cksum = md5(footer_meta).hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "obj data",
            "--boundary",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_meta,
            "--boundary--",
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_extra_mime_docs_ignored(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Transfer-Encoding': 'chunked',
                     'X-Backend-Obj-Metadata-Footer': 'yes',
                     'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary'},
            environ={'REQUEST_METHOD': 'PUT'})

        footer_meta = json.dumps({'X-Object-Meta-Mint': 'pepper'})
        footer_meta_cksum = md5(footer_meta).hexdigest()

        req.body = "\r\n".join((
            "--boundary",
            "",
            "obj data",
            "--boundary",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_meta,
            "--boundary",
            "This-Document-Is-Useless: yes",
            "",
            "blah blah I take up space",
            "--boundary--"
        ))
        req.headers.pop("Content-Length", None)

        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # swob made this into a StringIO for us
        wsgi_input = req.environ['wsgi.input']
        self.assertEqual(wsgi_input.tell(), len(wsgi_input.getvalue()))

    def test_PUT_user_metadata_no_xattr(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'ETag': 'b114ab7b90d9ccac4bd5d99cc7ebb568',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY THREE'

        def mock_get_and_setxattr(*args, **kargs):
            error_num = errno.ENOTSUP if hasattr(errno, 'ENOTSUP') else \
                errno.EOPNOTSUPP
            raise IOError(error_num, 'Operation not supported')

        with mock.patch('xattr.getxattr', mock_get_and_setxattr):
            with mock.patch('xattr.setxattr', mock_get_and_setxattr):
                resp = req.get_response(self.object_controller)
                self.assertEqual(resp.status_int, 507)

    def test_PUT_client_timeout(self):
        class FakeTimeout(BaseException):
            def __enter__(self):
                raise self

            def __exit__(self, typ, value, tb):
                pass
        # This is just so the test fails when run on older object server code
        # instead of exploding.
        if not hasattr(object_server, 'ChunkReadTimeout'):
            object_server.ChunkReadTimeout = None
        with mock.patch.object(object_server, 'ChunkReadTimeout', FakeTimeout):
            timestamp = normalize_timestamp(time())
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Type': 'text/plain',
                         'Content-Length': '6'})
            req.environ['wsgi.input'] = WsgiBytesIO(b'VERIFY')
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 408)

    def test_PUT_client_closed_connection(self):
        class fake_input(object):
            def read(self, *a, **kw):
                # On client disconnect during a chunked transfer, eventlet
                # may raise a ValueError (or ChunkReadError, following
                # https://github.com/eventlet/eventlet/commit/c3ce3ee -- but
                # that inherits from ValueError)
                raise ValueError

        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'Content-Length': '6'})
        req.environ['wsgi.input'] = fake_input()
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 499)

    def test_PUT_system_metadata(self):
        # check that sysmeta is stored in diskfile
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Sysmeta-1': 'One',
                     'X-Object-Sysmeta-Two': 'Two',
                     'X-Object-Transient-Sysmeta-Foo': 'Bar'})
        req.body = 'VERIFY SYSMETA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY SYSMETA')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': timestamp,
                          'Content-Length': '14',
                          'Content-Type': 'text/plain',
                          'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                          'name': '/a/c/o',
                          'X-Object-Meta-1': 'One',
                          'X-Object-Sysmeta-1': 'One',
                          'X-Object-Sysmeta-Two': 'Two',
                          'X-Object-Transient-Sysmeta-Foo': 'Bar'})

    def test_PUT_succeeds_with_later_POST(self):
        t_put = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': t_put,
                                     'Content-Length': 0,
                                     'Content-Type': 'plain/text'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        t_put2 = next(self.ts).internal
        t_post = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': t_post})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': t_put2,
                                     'Content-Length': 0,
                                     'Content-Type': 'plain/text'},
                            )
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        obj_dir = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')))

        ts_file = os.path.join(obj_dir, t_put2 + '.data')
        self.assertTrue(os.path.isfile(ts_file))
        meta_file = os.path.join(obj_dir, t_post + '.meta')
        self.assertTrue(os.path.isfile(meta_file))

    def test_POST_system_metadata(self):
        # check that diskfile sysmeta is not changed by a POST
        timestamp1 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'text/plain',
                     'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Sysmeta-1': 'One',
                     'X-Object-Sysmeta-Two': 'Two'})
        req.body = 'VERIFY SYSMETA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp2,
                     'X-Object-Meta-1': 'Not One',
                     'X-Object-Sysmeta-1': 'Not One',
                     'X-Object-Sysmeta-Two': 'Not Two'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        # original .data file metadata should be unchanged
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp1 + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY SYSMETA')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': timestamp1,
                          'Content-Length': '14',
                          'Content-Type': 'text/plain',
                          'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                          'name': '/a/c/o',
                          'X-Object-Meta-1': 'One',
                          'X-Object-Sysmeta-1': 'One',
                          'X-Object-Sysmeta-Two': 'Two'})

        # .meta file metadata should have only user meta items
        metafile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp2 + '.meta')
        self.assertTrue(os.path.isfile(metafile))
        self.assertEqual(diskfile.read_metadata(metafile),
                         {'X-Timestamp': timestamp2,
                          'name': '/a/c/o',
                          'X-Object-Meta-1': 'Not One'})

    def test_POST_then_fetch_content_type(self):
        # check that content_type is updated by a POST
        timestamp1 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'text/plain',
                     'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'X-Object-Meta-1': 'One'})
        req.body = 'VERIFY SYSMETA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp2,
                     'X-Object-Meta-1': 'Not One',
                     'Content-Type': 'text/html'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        # original .data file metadata should be unchanged
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp1 + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY SYSMETA')
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': timestamp1,
                          'Content-Length': '14',
                          'Content-Type': 'text/plain',
                          'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                          'name': '/a/c/o',
                          'X-Object-Meta-1': 'One'})

        # .meta file metadata should have updated content-type
        metafile_name = encode_timestamps(Timestamp(timestamp2),
                                          Timestamp(timestamp2),
                                          explicit=True)
        metafile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')),
            metafile_name + '.meta')
        self.assertTrue(os.path.isfile(metafile))
        self.assertEqual(diskfile.read_metadata(metafile),
                         {'X-Timestamp': timestamp2,
                          'name': '/a/c/o',
                          'Content-Type': 'text/html',
                          'Content-Type-Timestamp': timestamp2,
                          'X-Object-Meta-1': 'Not One'})

        def check_response(resp):
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 14)
            self.assertEqual(resp.content_type, 'text/html')
            self.assertEqual(resp.headers['content-type'], 'text/html')
            self.assertEqual(
                resp.headers['last-modified'],
                strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(math.ceil(float(timestamp2)))))
            self.assertEqual(resp.headers['etag'],
                             '"1000d172764c9dbc3a5798a67ec5bb76"')
            self.assertEqual(resp.headers['x-object-meta-1'], 'Not One')

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        check_response(resp)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        check_response(resp)

    def test_POST_transient_sysmeta(self):
        # check that diskfile transient system meta is changed by a POST
        timestamp1 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'text/plain',
                     'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Sysmeta-1': 'One',
                     'X-Object-Transient-Sysmeta-Foo': 'Bar'})
        req.body = 'VERIFY SYSMETA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp2,
                     'X-Object-Meta-1': 'Not One',
                     'X-Object-Sysmeta-1': 'Not One',
                     'X-Object-Transient-Sysmeta-Foo': 'Not Bar'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        # original .data file metadata should be unchanged
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp1 + '.data')
        self.assertTrue(os.path.isfile(objfile))
        self.assertEqual(open(objfile).read(), 'VERIFY SYSMETA')
        self.assertDictEqual(diskfile.read_metadata(objfile),
                             {'X-Timestamp': timestamp1,
                              'Content-Length': '14',
                              'Content-Type': 'text/plain',
                              'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                              'name': '/a/c/o',
                              'X-Object-Meta-1': 'One',
                              'X-Object-Sysmeta-1': 'One',
                              'X-Object-Transient-Sysmeta-Foo': 'Bar'})

        # .meta file metadata should have only user meta items
        metafile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp2 + '.meta')
        self.assertTrue(os.path.isfile(metafile))
        self.assertDictEqual(diskfile.read_metadata(metafile),
                             {'X-Timestamp': timestamp2,
                              'name': '/a/c/o',
                              'X-Object-Meta-1': 'Not One',
                              'X-Object-Transient-Sysmeta-Foo': 'Not Bar'})

    def test_PUT_then_fetch_system_metadata(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Sysmeta-1': 'One',
                     'X-Object-Sysmeta-Two': 'Two',
                     'X-Object-Transient-Sysmeta-Foo': 'Bar'})
        req.body = 'VERIFY SYSMETA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        def check_response(resp):
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 14)
            self.assertEqual(resp.content_type, 'text/plain')
            self.assertEqual(resp.headers['content-type'], 'text/plain')
            self.assertEqual(
                resp.headers['last-modified'],
                strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(math.ceil(float(timestamp)))))
            self.assertEqual(resp.headers['etag'],
                             '"1000d172764c9dbc3a5798a67ec5bb76"')
            self.assertEqual(resp.headers['x-object-meta-1'], 'One')
            self.assertEqual(resp.headers['x-object-sysmeta-1'], 'One')
            self.assertEqual(resp.headers['x-object-sysmeta-two'], 'Two')
            self.assertEqual(resp.headers['x-object-transient-sysmeta-foo'],
                             'Bar')

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        check_response(resp)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        check_response(resp)

    def test_PUT_then_POST_then_fetch_system_metadata(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'text/plain',
                     'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'X-Object-Meta-0': 'deleted by post',
                     'X-Object-Sysmeta-0': 'Zero',
                     'X-Object-Transient-Sysmeta-0': 'deleted by post',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Sysmeta-1': 'One',
                     'X-Object-Sysmeta-Two': 'Two',
                     'X-Object-Transient-Sysmeta-Foo': 'Bar'})
        req.body = 'VERIFY SYSMETA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp2,
                     'X-Object-Meta-1': 'Not One',
                     'X-Object-Sysmeta-1': 'Not One',
                     'X-Object-Sysmeta-Two': 'Not Two',
                     'X-Object-Transient-Sysmeta-Foo': 'Not Bar'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        def check_response(resp):
            # user meta should be updated but not sysmeta
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 14)
            self.assertEqual(resp.content_type, 'text/plain')
            self.assertEqual(resp.headers['content-type'], 'text/plain')
            self.assertEqual(
                resp.headers['last-modified'],
                strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(math.ceil(float(timestamp2)))))
            self.assertEqual(resp.headers['etag'],
                             '"1000d172764c9dbc3a5798a67ec5bb76"')
            self.assertEqual(resp.headers['x-object-meta-1'], 'Not One')
            self.assertEqual(resp.headers['x-object-sysmeta-0'], 'Zero')
            self.assertEqual(resp.headers['x-object-sysmeta-1'], 'One')
            self.assertEqual(resp.headers['x-object-sysmeta-two'], 'Two')
            self.assertEqual(resp.headers['x-object-transient-sysmeta-foo'],
                             'Not Bar')
            self.assertNotIn('x-object-meta-0', resp.headers)
            self.assertNotIn('x-object-transient-sysmeta-0', resp.headers)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        check_response(resp)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        check_response(resp)

    def test_PUT_with_replication_headers(self):
        # check that otherwise disallowed headers are accepted when specified
        # by X-Backend-Replication-Headers

        # first PUT object
        timestamp1 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'text/plain',
                     'Content-Length': '14',
                     'Etag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'Custom-Header': 'custom1',
                     'X-Object-Meta-1': 'meta1',
                     'X-Static-Large-Object': 'False'})
        req.body = 'VERIFY SYSMETA'

        # restrict set of allowed headers on this server
        with mock.patch.object(self.object_controller, 'allowed_headers',
                               ['Custom-Header']):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp1 + '.data')
        # X-Static-Large-Object is disallowed.
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': timestamp1,
                          'Content-Type': 'text/plain',
                          'Content-Length': '14',
                          'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                          'name': '/a/c/o',
                          'Custom-Header': 'custom1',
                          'X-Object-Meta-1': 'meta1'})

        # PUT object again with X-Backend-Replication-Headers
        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Type': 'text/plain',
                     'Content-Length': '14',
                     'Etag': '1000d172764c9dbc3a5798a67ec5bb76',
                     'Custom-Header': 'custom1',
                     'X-Object-Meta-1': 'meta1',
                     'X-Static-Large-Object': 'False',
                     'X-Backend-Replication-Headers':
                     'X-Static-Large-Object'})
        req.body = 'VERIFY SYSMETA'

        with mock.patch.object(self.object_controller, 'allowed_headers',
                               ['Custom-Header']):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')),
            timestamp2 + '.data')
        # X-Static-Large-Object should be copied since it is now allowed by
        # replication headers.
        self.assertEqual(diskfile.read_metadata(objfile),
                         {'X-Timestamp': timestamp2,
                          'Content-Type': 'text/plain',
                          'Content-Length': '14',
                          'ETag': '1000d172764c9dbc3a5798a67ec5bb76',
                          'name': '/a/c/o',
                          'Custom-Header': 'custom1',
                          'X-Object-Meta-1': 'meta1',
                          'X-Static-Large-Object': 'False'})

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
        with mock.patch.object(
                object_server, 'http_connect', mock_http_connect(201)):
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
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
        with mock.patch.object(
                object_server, 'http_connect', mock_http_connect(500)):
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
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
        with mock.patch.object(
                object_server, 'http_connect',
                mock_http_connect(500, with_exc=True)):
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

    def test_EC_GET_PUT_data(self):
        for policy in self.ec_policies:
            raw_data = ('VERIFY' * policy.ec_segment_size)[:-432]
            frag_archives = encode_frag_archive_bodies(policy, raw_data)
            frag_index = random.randint(0, len(frag_archives) - 1)
            # put EC frag archive
            req = Request.blank('/sda1/p/a/c/o', method='PUT', headers={
                'X-Timestamp': next(self.ts).internal,
                'Content-Type': 'application/verify',
                'Content-Length': len(frag_archives[frag_index]),
                'X-Object-Sysmeta-Ec-Frag-Index': frag_index,
                'X-Backend-Storage-Policy-Index': int(policy),
            })
            req.body = frag_archives[frag_index]
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 201)

            # get EC frag archive
            req = Request.blank('/sda1/p/a/c/o', headers={
                'X-Backend-Storage-Policy-Index': int(policy),
            })
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.body, frag_archives[frag_index])

    def test_EC_GET_quarantine_invalid_frag_archive(self):
        policy = random.choice(self.ec_policies)
        raw_data = ('VERIFY' * policy.ec_segment_size)[:-432]
        frag_archives = encode_frag_archive_bodies(policy, raw_data)
        frag_index = random.randint(0, len(frag_archives) - 1)
        content_length = len(frag_archives[frag_index])
        # put EC frag archive
        req = Request.blank('/sda1/p/a/c/o', method='PUT', headers={
            'X-Timestamp': next(self.ts).internal,
            'Content-Type': 'application/verify',
            'Content-Length': content_length,
            'X-Object-Sysmeta-Ec-Frag-Index': frag_index,
            'X-Backend-Storage-Policy-Index': int(policy),
        })
        corrupt = 'garbage' + frag_archives[frag_index]
        req.body = corrupt[:content_length]
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # get EC frag archive
        req = Request.blank('/sda1/p/a/c/o', headers={
            'X-Backend-Storage-Policy-Index': int(policy),
        })
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        with self.assertRaises(DiskFileQuarantined) as ctx:
            resp.body
        self.assertIn("Invalid EC metadata", str(ctx.exception))

        # nothing is logged on *our* loggers
        errors = self.object_controller.logger.get_lines_for_level('error')
        self.assertEqual(errors, [])

        # get EC frag archive - it's gone
        req = Request.blank('/sda1/p/a/c/o', headers={
            'X-Backend-Storage-Policy-Index': int(policy),
        })
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

    def test_PUT_ssync_multi_frag(self):
        timestamp = utils.Timestamp.now().internal

        def put_with_index(expected_rsp, frag_index, node_index=None):
            data_file_tail = '#%d#d.data' % frag_index
            headers = {'X-Timestamp': timestamp,
                       'Content-Length': '6',
                       'Content-Type': 'application/octet-stream',
                       'X-Backend-Ssync-Frag-Index': node_index,
                       'X-Object-Sysmeta-Ec-Frag-Index': frag_index,
                       'X-Backend-Storage-Policy-Index': int(policy)}
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers=headers)
            req.body = 'VERIFY'
            resp = req.get_response(self.object_controller)

            self.assertEqual(
                resp.status_int, expected_rsp,
                'got %s != %s for frag_index=%s node_index=%s' % (
                    resp.status_int, expected_rsp,
                    frag_index, node_index))
            if expected_rsp == 409:
                return
            obj_dir = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.get_data_dir(int(policy)),
                                  'p', hash_path('a', 'c', 'o')))
            data_file = os.path.join(obj_dir, timestamp) + data_file_tail
            self.assertTrue(os.path.isfile(data_file),
                            'Expected file %r not found in %r for policy %r'
                            % (data_file, os.listdir(obj_dir), int(policy)))

        for policy in POLICIES:
            if policy.policy_type == EC_POLICY:
                # upload with a ec-frag-index
                put_with_index(201, 3)
                # same timestamp will conflict a different ec-frag-index
                put_with_index(409, 2)
                # but with the ssync-frag-index (primary node) it will just
                # save both!
                put_with_index(201, 2, 2)
                # but even with the ssync-frag-index we can still get a
                # timestamp collisison if the file already exists
                put_with_index(409, 3, 3)

                # FWIW, ssync will never send in-consistent indexes - but if
                # something else did, from the object server perspective ...

                # ... the ssync-frag-index is canonical on the
                # read/pre-existance check
                put_with_index(409, 7, 2)
                # ... but the ec-frag-index is canonical when it comes to on
                # disk file
                put_with_index(201, 7, 6)

    def test_PUT_commits_data(self):
        for policy in POLICIES:
            timestamp = utils.Timestamp(int(time())).internal
            data_file_tail = '.data'
            headers = {'X-Timestamp': timestamp,
                       'Content-Length': '6',
                       'Content-Type': 'application/octet-stream',
                       'X-Backend-Storage-Policy-Index': int(policy)}
            if policy.policy_type == EC_POLICY:
                # commit renames data file
                headers['X-Object-Sysmeta-Ec-Frag-Index'] = '2'
                data_file_tail = '#2#d.data'
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers=headers)
            req.body = 'VERIFY'
            resp = req.get_response(self.object_controller)

            self.assertEqual(resp.status_int, 201)
            obj_dir = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.get_data_dir(int(policy)),
                                  'p', hash_path('a', 'c', 'o')))
            data_file = os.path.join(obj_dir, timestamp) + data_file_tail
            self.assertTrue(os.path.isfile(data_file),
                            'Expected file %r not found in %r for policy %r'
                            % (data_file, os.listdir(obj_dir), int(policy)))
            rmtree(obj_dir)

    def test_HEAD(self):
        # Test swift.obj.server.ObjectController.HEAD
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertFalse('X-Backend-Timestamp' in resp.headers)

        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'application/x-test',
                     'X-Object-Meta-1': 'One',
                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_length, 6)
        self.assertEqual(resp.content_type, 'application/x-test')
        self.assertEqual(resp.headers['content-type'], 'application/x-test')
        self.assertEqual(
            resp.headers['last-modified'],
            strftime('%a, %d %b %Y %H:%M:%S GMT',
                     gmtime(math.ceil(float(timestamp)))))
        self.assertEqual(resp.headers['etag'],
                         '"0b4c12d7e0a73840c1c4f148fda3b037"')
        self.assertEqual(resp.headers['x-object-meta-1'], 'One')
        self.assertEqual(resp.headers['x-object-meta-two'], 'Two')

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        os.unlink(objfile)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-length': '6'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         utils.Timestamp(timestamp).internal)

    def test_HEAD_quarantine_zbyte(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o',
                                             policy=POLICIES.legacy)
        disk_file.open()

        file_name = os.path.basename(disk_file._data_file)
        with open(disk_file._data_file) as fp:
            metadata = diskfile.read_metadata(fp)
        os.unlink(disk_file._data_file)
        with open(disk_file._data_file, 'w') as fp:
            diskfile.write_metadata(fp, metadata)

        file_name = os.path.basename(disk_file._data_file)
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEqual(os.listdir(quar_dir)[0], file_name)

    def test_OPTIONS(self):
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        server_handler = object_server.ObjectController(
            conf, logger=debug_logger())
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0
        resp = server_handler.OPTIONS(req)
        self.assertEqual(200, resp.status_int)
        for verb in 'OPTIONS GET POST PUT DELETE HEAD REPLICATE \
                SSYNC'.split():
            self.assertTrue(
                verb in resp.headers['Allow'].split(', '))
        self.assertEqual(len(resp.headers['Allow'].split(', ')), 8)
        self.assertEqual(resp.headers['Server'],
                         (server_handler.server_type + '/' + swift_version))

    def test_insufficient_storage_mount_check_true(self):
        conf = {'devices': self.testdir, 'mount_check': 'true'}
        object_controller = object_server.ObjectController(conf)
        for policy in POLICIES:
            mgr = object_controller._diskfile_router[policy]
            self.assertTrue(mgr.mount_check)
        for method in object_controller.allowed_methods:
            if method in ('OPTIONS', 'SSYNC'):
                continue
            path = '/sda1/p/'
            if method == 'REPLICATE':
                path += 'suff'
            else:
                path += 'a/c/o'
            req = Request.blank(path, method=method,
                                headers={'x-timestamp': '1',
                                         'content-type': 'app/test',
                                         'content-length': 0})
            with mock_check_drive() as mocks:
                try:
                    resp = req.get_response(object_controller)
                    self.assertEqual(resp.status_int, 507)
                    mocks['ismount'].return_value = True
                    resp = req.get_response(object_controller)
                    self.assertNotEqual(resp.status_int, 507)
                    # feel free to rip out this last assertion...
                    expected = 2 if method in ('PUT', 'REPLICATE') else 4
                    self.assertEqual(resp.status_int // 100, expected)
                except AssertionError as e:
                    self.fail('%s for %s' % (e, method))

    def test_insufficient_storage_mount_check_false(self):
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        object_controller = object_server.ObjectController(conf)
        for policy in POLICIES:
            mgr = object_controller._diskfile_router[policy]
            self.assertFalse(mgr.mount_check)
        for method in object_controller.allowed_methods:
            if method in ('OPTIONS', 'SSYNC'):
                continue
            path = '/sda1/p/'
            if method == 'REPLICATE':
                path += 'suff'
            else:
                path += 'a/c/o'
            req = Request.blank(path, method=method,
                                headers={'x-timestamp': '1',
                                         'content-type': 'app/test',
                                         'content-length': 0})
            with mock_check_drive() as mocks:
                try:
                    resp = req.get_response(object_controller)
                    self.assertEqual(resp.status_int, 507)
                    mocks['isdir'].return_value = True
                    resp = req.get_response(object_controller)
                    self.assertNotEqual(resp.status_int, 507)
                    # feel free to rip out this last assertion...
                    expected = 2 if method in ('PUT', 'REPLICATE') else 4
                    self.assertEqual(resp.status_int // 100, expected)
                except AssertionError as e:
                    self.fail('%s for %s' % (e, method))

    def test_GET(self):
        # Test swift.obj.server.ObjectController.GET
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertFalse('X-Backend-Timestamp' in resp.headers)

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, 'VERIFY')
        self.assertEqual(resp.content_length, 6)
        self.assertEqual(resp.content_type, 'application/x-test')
        self.assertEqual(resp.headers['content-length'], '6')
        self.assertEqual(resp.headers['content-type'], 'application/x-test')
        self.assertEqual(
            resp.headers['last-modified'],
            strftime('%a, %d %b %Y %H:%M:%S GMT',
                     gmtime(math.ceil(float(timestamp)))))
        self.assertEqual(resp.headers['etag'],
                         '"0b4c12d7e0a73840c1c4f148fda3b037"')
        self.assertEqual(resp.headers['x-object-meta-1'], 'One')
        self.assertEqual(resp.headers['x-object-meta-two'], 'Two')

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        req.range = 'bytes=1-3'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 206)
        self.assertEqual(resp.body, 'ERI')
        self.assertEqual(resp.headers['content-length'], '3')

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        req.range = 'bytes=1-'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 206)
        self.assertEqual(resp.body, 'ERIFY')
        self.assertEqual(resp.headers['content-length'], '5')

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        req.range = 'bytes=-2'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 206)
        self.assertEqual(resp.body, 'FY')
        self.assertEqual(resp.headers['content-length'], '2')

        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.data')
        os.unlink(objfile)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application:octet-stream',
                                'Content-Length': '6'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        sleep(.00001)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         utils.Timestamp(timestamp).internal)

    def test_GET_range_zero_byte_object(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/zero-byte',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = ''
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/zero-byte',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Range': 'bytes=-10'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

    def test_GET_if_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Match': '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)
        self.assertIn(
            '"HEAD /sda1/p/a/c/o" 412 - ',
            self.object_controller.logger.get_lines_for_level('info')[-1])

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)
        self.assertIn(
            '"GET /sda1/p/a/c/o" 412 - ',
            self.object_controller.logger.get_lines_for_level('info')[-1])

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={
                'If-Match': '"11111111111111111111111111111111", "%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={
                'If-Match':
                '"11111111111111111111111111111111", '
                '"22222222222222222222222222222222"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

    def test_GET_if_match_etag_is_at(self):
        headers = {
            'X-Timestamp': utils.Timestamp.now().internal,
            'Content-Type': 'application/octet-stream',
            'X-Object-Meta-Xtag': 'madeup',
            'X-Object-Sysmeta-Xtag': 'alternate madeup',
        }
        req = Request.blank('/sda1/p/a/c/o', method='PUT',
                            headers=headers)
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        real_etag = resp.etag

        # match x-backend-etag-is-at
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': 'madeup',
            'X-Backend-Etag-Is-At': 'X-Object-Meta-Xtag'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # match x-backend-etag-is-at, using first in list of alternates
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': 'madeup',
            'X-Backend-Etag-Is-At':
                'X-Object-Meta-Xtag,X-Object-Sysmeta-Z'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # match x-backend-etag-is-at, using second in list of alternates
        alts = 'X-Object-Sysmeta-Y,X-Object-Meta-Xtag,X-Object-Sysmeta-Z'
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': 'madeup',
            'X-Backend-Etag-Is-At': alts})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # match x-backend-etag-is-at, choosing first of multiple alternates
        alts = 'X-Object-Sysmeta-Y,X-Object-Meta-Xtag,X-Object-Sysmeta-Xtag'
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': 'madeup',
            'X-Backend-Etag-Is-At': alts})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # match x-backend-etag-is-at, choosing first of multiple alternates
        # (switches order of second two alternates from previous assertion)
        alts = 'X-Object-Sysmeta-Y,X-Object-Sysmeta-Xtag,X-Object-Meta-Xtag'
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': 'alternate madeup',
            'X-Backend-Etag-Is-At': alts})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # no match x-backend-etag-is-at
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': real_etag,
            'X-Backend-Etag-Is-At': 'X-Object-Meta-Xtag'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        # etag-is-at metadata doesn't exist, default to real etag
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': real_etag,
            'X-Backend-Etag-Is-At': 'X-Object-Meta-Missing'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # sanity no-match with no etag-is-at
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': 'madeup'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        # sanity match with no etag-is-at
        req = Request.blank('/sda1/p/a/c/o', headers={
            'If-Match': real_etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # sanity with no if-match
        req = Request.blank('/sda1/p/a/c/o', headers={
            'X-Backend-Etag-Is-At': 'X-Object-Meta-Xtag'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

    def test_HEAD_if_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Match': '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'},
            headers={
                'If-Match': '"11111111111111111111111111111111", "%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'},
            headers={
                'If-Match':
                '"11111111111111111111111111111111", '
                '"22222222222222222222222222222222"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

    def test_GET_if_none_match(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'X-Object-Meta-Soup': 'gazpacho',
                                'Content-Type': 'application/fizzbuzz',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(resp.etag, etag)
        self.assertEqual(resp.headers['Content-Type'], 'application/fizzbuzz')
        self.assertEqual(resp.headers['X-Object-Meta-Soup'], 'gazpacho')

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-None-Match': '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-None-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-None-Match':
                     '"11111111111111111111111111111111", '
                     '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(resp.etag, etag)

    def test_HEAD_if_none_match(self):
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': normalize_timestamp(time()),
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        etag = resp.etag

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-None-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(resp.etag, etag)

        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-None-Match': '*'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-None-Match': '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-None-Match': '"11111111111111111111111111111111"'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.etag, etag)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-None-Match':
                     '"11111111111111111111111111111111", '
                     '"%s"' % etag})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)
        self.assertEqual(resp.etag, etag)

    def test_GET_if_modified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        since = resp.headers['Last-Modified']
        self.assertEqual(since, strftime('%a, %d %b %Y %H:%M:%S GMT',
                                         gmtime(math.ceil(float(timestamp)))))

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

        timestamp = normalize_timestamp(int(time()))
        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp)))
        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

    def test_HEAD_if_modified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 1))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        since = resp.headers['Last-Modified']
        self.assertEqual(since, strftime('%a, %d %b %Y %H:%M:%S GMT',
                                         gmtime(math.ceil(float(timestamp)))))

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Modified-Since': since})
        resp = self.object_controller.GET(req)
        self.assertEqual(resp.status_int, 304)

        timestamp = normalize_timestamp(int(time()))
        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'Content-Type': 'application/octet-stream',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp)))
        req = Request.blank('/sda1/p/a/c/o2',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Modified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 304)

    def test_GET_if_unmodified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={
                                'X-Timestamp': timestamp,
                                'X-Object-Meta-Burr': 'ito',
                                'Content-Type': 'application/cat-picture',
                                'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(float(timestamp) + 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) - 9))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)
        self.assertEqual(resp.headers['Content-Type'],
                         'application/cat-picture')
        self.assertEqual(resp.headers['X-Object-Meta-Burr'], 'ito')

        since = \
            strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 9))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        since = resp.headers['Last-Modified']
        self.assertEqual(since, strftime('%a, %d %b %Y %H:%M:%S GMT',
                                         gmtime(math.ceil(float(timestamp)))))

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

    def test_HEAD_if_unmodified_since(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Type': 'application/octet-stream',
                     'Content-Length': '4'})
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(math.ceil(float(timestamp)) + 1))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(math.ceil(float(timestamp))))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        since = strftime('%a, %d %b %Y %H:%M:%S GMT',
                         gmtime(math.ceil(float(timestamp)) - 1))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'If-Unmodified-Since': since})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

    def assertECBodyEqual(self, resp, expected):
        # we pull the policy index from the request environ since it seems to
        # be missing from the response headers
        policy_index = int(
            resp.request.headers['X-Backend-Storage-Policy-Index'])
        policy = POLICIES[policy_index]
        frags = encode_frag_archive_bodies(policy, expected)
        frag_index = int(resp.headers['X-Object-Sysmeta-Ec-Frag-Index'])
        self.assertEqual(resp.body, frags[frag_index])

    def _create_ondisk_fragments(self, policy):
        # Create some on disk files...
        ts_iter = make_timestamp_iter()

        # PUT at ts_0
        ts_0 = next(ts_iter)
        body = 'OLDER'
        headers = {'X-Timestamp': ts_0.internal,
                   'Content-Length': '5',
                   'Content-Type': 'application/octet-stream',
                   'X-Backend-Storage-Policy-Index': int(policy)}
        if policy.policy_type == EC_POLICY:
            body = encode_frag_archive_bodies(policy, body)[0]
            headers.update({
                'X-Object-Sysmeta-Ec-Frag-Index': '0',
                'Content-Length': len(body),
            })
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers)
        req.body = body
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # POST at ts_1
        ts_1 = next(ts_iter)
        headers = {'X-Timestamp': ts_1.internal,
                   'X-Backend-Storage-Policy-Index': int(policy)}
        headers['X-Object-Meta-Test'] = 'abc'
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers=headers)
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        # PUT again at ts_2 but without making the data file durable
        ts_2 = next(ts_iter)
        body = 'NEWER'
        headers = {'X-Timestamp': ts_2.internal,
                   'Content-Length': '5',
                   'Content-Type': 'application/octet-stream',
                   'X-Backend-Storage-Policy-Index': int(policy)}
        if policy.policy_type == EC_POLICY:
            body = encode_frag_archive_bodies(policy, body)[2]
            headers.update({
                'X-Object-Sysmeta-Ec-Frag-Index': '2',
                'Content-Length': len(body),
            })
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers)
        req.body = body
        # patch the commit method to do nothing so EC object is non-durable
        with mock.patch('swift.obj.diskfile.ECDiskFileWriter.commit'):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        return ts_0, ts_1, ts_2

    def test_GET_HEAD_with_fragment_preferences(self):
        for policy in POLICIES:
            ts_0, ts_1, ts_2 = self._create_ondisk_fragments(policy)

            backend_frags = {ts_0.internal: [0], ts_2.internal: [2]}

            def _assert_frag_0_at_ts_0(resp):
                expect = {
                    'X-Timestamp': ts_1.normal,
                    'X-Backend-Timestamp': ts_1.internal,
                    'X-Backend-Data-Timestamp': ts_0.internal,
                    'X-Backend-Durable-Timestamp': ts_0.internal,
                    'X-Object-Sysmeta-Ec-Frag-Index': '0',
                    'X-Object-Meta-Test': 'abc'}
                self.assertDictContainsSubset(expect, resp.headers)
                self.assertEqual(backend_frags, json.loads(
                    resp.headers['X-Backend-Fragments']))

            def _assert_repl_data_at_ts_2():
                self.assertIn(resp.status_int, (200, 202))
                expect = {
                    'X-Timestamp': ts_2.normal,
                    'X-Backend-Timestamp': ts_2.internal,
                    'X-Backend-Data-Timestamp': ts_2.internal,
                    'X-Backend-Durable-Timestamp': ts_2.internal}
                self.assertDictContainsSubset(expect, resp.headers)
                self.assertNotIn('X-Object-Meta-Test', resp.headers)

            # Sanity check: Request with no preferences should default to the
            # durable frag
            headers = {'X-Backend-Storage-Policy-Index': int(policy)}
            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_0_at_ts_0(resp)
                self.assertECBodyEqual(resp, 'OLDER')
            else:
                _assert_repl_data_at_ts_2()
                self.assertEqual(resp.body, 'NEWER')

            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'HEAD'})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 200)
            if policy.policy_type == EC_POLICY:
                _assert_frag_0_at_ts_0(resp)
            else:
                _assert_repl_data_at_ts_2()

            # Request with preferences can select the older frag
            prefs = json.dumps(
                [{'timestamp': ts_0.internal, 'exclude': [1, 3]}])
            headers = {'X-Backend-Storage-Policy-Index': int(policy),
                       'X-Backend-Fragment-Preferences': prefs}
            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_0_at_ts_0(resp)
                self.assertECBodyEqual(resp, 'OLDER')
            else:
                _assert_repl_data_at_ts_2()
                self.assertEqual(resp.body, 'NEWER')

            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'HEAD'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_0_at_ts_0(resp)
            else:
                _assert_repl_data_at_ts_2()

            def _assert_frag_2_at_ts_2(resp):
                self.assertIn(resp.status_int, (200, 202))
                # do not expect meta file to be included since it is older
                expect = {
                    'X-Timestamp': ts_2.normal,
                    'X-Backend-Timestamp': ts_2.internal,
                    'X-Backend-Data-Timestamp': ts_2.internal,
                    'X-Backend-Durable-Timestamp': ts_0.internal,
                    'X-Object-Sysmeta-Ec-Frag-Index': '2'}
                self.assertDictContainsSubset(expect, resp.headers)
                self.assertEqual(backend_frags, json.loads(
                    resp.headers['X-Backend-Fragments']))
                self.assertNotIn('X-Object-Meta-Test', resp.headers)

            # Request with preferences can select the newer non-durable frag
            prefs = json.dumps(
                [{'timestamp': ts_2.internal, 'exclude': [1, 3]}])
            headers = {'X-Backend-Storage-Policy-Index': int(policy),
                       'X-Backend-Fragment-Preferences': prefs}
            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_2_at_ts_2(resp)
                self.assertECBodyEqual(resp, 'NEWER')
            else:
                _assert_repl_data_at_ts_2()
                self.assertEqual(resp.body, 'NEWER')

            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'HEAD'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_2_at_ts_2(resp)
            else:
                _assert_repl_data_at_ts_2()

            # Request with preference for ts_0 but excludes index 0 will
            # default to newest frag
            prefs = json.dumps(
                [{'timestamp': ts_0.internal, 'exclude': [0]}])
            headers = {'X-Backend-Storage-Policy-Index': int(policy),
                       'X-Backend-Fragment-Preferences': prefs}
            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.object_controller)
            if policy.policy_type == EC_POLICY:
                _assert_frag_2_at_ts_2(resp)
                self.assertECBodyEqual(resp, 'NEWER')
            else:
                _assert_repl_data_at_ts_2()
                self.assertEqual(resp.body, 'NEWER')

            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'HEAD'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_2_at_ts_2(resp)
            else:
                _assert_repl_data_at_ts_2()

            # Request with preferences that exclude all frags get nothing
            prefs = json.dumps(
                [{'timestamp': ts_0.internal, 'exclude': [0]},
                 {'timestamp': ts_2.internal, 'exclude': [2]}])
            headers = {'X-Backend-Storage-Policy-Index': int(policy),
                       'X-Backend-Fragment-Preferences': prefs}
            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.object_controller)
            if policy.policy_type == EC_POLICY:
                self.assertEqual(resp.status_int, 404)
            else:
                _assert_repl_data_at_ts_2()
                self.assertEqual(resp.body, 'NEWER')

            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'HEAD'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                self.assertEqual(resp.status_int, 404)
            else:
                _assert_repl_data_at_ts_2()

            # Request with empty preferences will get non-durable
            prefs = json.dumps([])
            headers = {'X-Backend-Storage-Policy-Index': int(policy),
                       'X-Backend-Fragment-Preferences': prefs}
            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.object_controller)
            if policy.policy_type == EC_POLICY:
                _assert_frag_2_at_ts_2(resp)
                self.assertECBodyEqual(resp, 'NEWER')
            else:
                _assert_repl_data_at_ts_2()
                self.assertEqual(resp.body, 'NEWER')

            req = Request.blank('/sda1/p/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'HEAD'})
            resp = req.get_response(self.object_controller)

            if policy.policy_type == EC_POLICY:
                _assert_frag_2_at_ts_2(resp)
            else:
                _assert_repl_data_at_ts_2()

    def test_GET_quarantine(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o',
                                             policy=POLICIES.legacy)
        disk_file.open()
        file_name = os.path.basename(disk_file._data_file)
        etag = md5()
        etag.update('VERIF')
        etag = etag.hexdigest()
        metadata = {'X-Timestamp': timestamp, 'name': '/a/c/o',
                    'Content-Length': 6, 'ETag': etag}
        diskfile.write_metadata(disk_file._fp, metadata)
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        body = resp.body  # actually does quarantining
        self.assertEqual(body, 'VERIFY')
        self.assertEqual(os.listdir(quar_dir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

    def test_GET_quarantine_zbyte(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o',
                                             policy=POLICIES.legacy)
        disk_file.open()
        file_name = os.path.basename(disk_file._data_file)
        with open(disk_file._data_file) as fp:
            metadata = diskfile.read_metadata(fp)
        os.unlink(disk_file._data_file)
        with open(disk_file._data_file, 'w') as fp:
            diskfile.write_metadata(fp, metadata)

        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEqual(os.listdir(quar_dir)[0], file_name)

    def test_GET_quarantine_range(self):
        # Test swift.obj.server.ObjectController.GET
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        disk_file = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o',
                                             policy=POLICIES.legacy)
        disk_file.open()
        file_name = os.path.basename(disk_file._data_file)
        etag = md5()
        etag.update('VERIF')
        etag = etag.hexdigest()
        metadata = {'X-Timestamp': timestamp, 'name': '/a/c/o',
                    'Content-Length': 6, 'ETag': etag}
        diskfile.write_metadata(disk_file._fp, metadata)
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=0-4'  # partial
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        resp.body
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        self.assertFalse(os.path.isdir(quar_dir))
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=1-6'  # partial
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        resp.body
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        self.assertFalse(os.path.isdir(quar_dir))

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=0-14'  # full
        resp = req.get_response(self.object_controller)
        quar_dir = os.path.join(
            self.testdir, 'sda1', 'quarantined', 'objects',
            os.path.basename(os.path.dirname(disk_file._data_file)))
        self.assertEqual(os.listdir(disk_file._datadir)[0], file_name)
        resp.body
        self.assertTrue(os.path.isdir(quar_dir))
        req = Request.blank('/sda1/p/a/c/o')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

    @mock.patch("time.time", mock_time)
    def test_DELETE(self):
        # Test swift.obj.server.ObjectController.DELETE
        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

        # The following should have created a tombstone file
        timestamp = normalize_timestamp(1000)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        ts_1000_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.ts')
        self.assertTrue(os.path.isfile(ts_1000_file))
        # There should now be a 1000 ts file.
        self.assertEqual(len(os.listdir(os.path.dirname(ts_1000_file))), 1)

        # The following should *not* have created a tombstone file.
        timestamp = normalize_timestamp(999)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        ts_999_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.ts')
        self.assertFalse(os.path.isfile(ts_999_file))
        self.assertTrue(os.path.isfile(ts_1000_file))
        self.assertEqual(len(os.listdir(os.path.dirname(ts_1000_file))), 1)

        orig_timestamp = utils.Timestamp(1002).internal
        headers = {'X-Timestamp': orig_timestamp,
                   'Content-Type': 'application/octet-stream',
                   'Content-Length': '4'}
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers)
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        # There should now be 1000 ts and a 1001 data file.
        data_1002_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            orig_timestamp + '.data')
        self.assertTrue(os.path.isfile(data_1002_file))
        self.assertEqual(len(os.listdir(os.path.dirname(data_1002_file))), 1)

        # The following should *not* have created a tombstone file.
        timestamp = normalize_timestamp(1001)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 409)
        self.assertEqual(resp.headers['X-Backend-Timestamp'], orig_timestamp)
        ts_1001_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.ts')
        self.assertFalse(os.path.isfile(ts_1001_file))
        self.assertTrue(os.path.isfile(data_1002_file))
        self.assertEqual(len(os.listdir(os.path.dirname(ts_1001_file))), 1)

        timestamp = normalize_timestamp(1003)
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)
        ts_1003_file = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(timestamp).internal + '.ts')
        self.assertTrue(os.path.isfile(ts_1003_file))
        self.assertEqual(len(os.listdir(os.path.dirname(ts_1003_file))), 1)

    def test_DELETE_bad_timestamp(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': 'bad'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_DELETE_succeeds_with_later_POST(self):
        t_put = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': t_put,
                                     'Content-Length': 0,
                                     'Content-Type': 'plain/text'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        t_delete = next(self.ts).internal
        t_post = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'X-Timestamp': t_post})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': t_delete},
                            )
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

        obj_dir = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(0), 'p',
                              hash_path('a', 'c', 'o')))

        ts_file = os.path.join(obj_dir, t_delete + '.ts')
        self.assertTrue(os.path.isfile(ts_file))
        meta_file = os.path.join(obj_dir, t_post + '.meta')
        self.assertTrue(os.path.isfile(meta_file))

    def test_DELETE_container_updates(self):
        # Test swift.obj.server.ObjectController.DELETE and container
        # updates, making sure container update is called in the correct
        # state.
        start = time()
        orig_timestamp = utils.Timestamp(start)
        headers = {'X-Timestamp': orig_timestamp.internal,
                   'Content-Type': 'application/octet-stream',
                   'Content-Length': '4'}
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers=headers)
        req.body = 'test'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        calls_made = [0]

        def our_container_update(*args, **kwargs):
            calls_made[0] += 1

        orig_cu = self.object_controller.container_update
        self.object_controller.container_update = our_container_update
        try:
            # The following request should return 409 (HTTP Conflict). A
            # tombstone file should not have been created with this timestamp.
            timestamp = utils.Timestamp(start - 0.00001)
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp.internal})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 409)
            self.assertEqual(resp.headers['x-backend-timestamp'],
                             orig_timestamp.internal)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                                  hash_path('a', 'c', 'o')),
                utils.Timestamp(timestamp).internal + '.ts')
            self.assertFalse(os.path.isfile(objfile))
            self.assertEqual(len(os.listdir(os.path.dirname(objfile))), 1)
            self.assertEqual(0, calls_made[0])

            # The following request should return 204, and the object should
            # be truly deleted (container update is performed) because this
            # timestamp is newer. A tombstone file should have been created
            # with this timestamp.
            timestamp = utils.Timestamp(start + 0.00001)
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp.internal})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 204)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                                  hash_path('a', 'c', 'o')),
                utils.Timestamp(timestamp).internal + '.ts')
            self.assertTrue(os.path.isfile(objfile))
            self.assertEqual(1, calls_made[0])
            self.assertEqual(len(os.listdir(os.path.dirname(objfile))), 1)

            # The following request should return a 404, as the object should
            # already have been deleted, but it should have also performed a
            # container update because the timestamp is newer, and a tombstone
            # file should also exist with this timestamp.
            timestamp = utils.Timestamp(start + 0.00002)
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp.internal})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 404)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                                  hash_path('a', 'c', 'o')),
                utils.Timestamp(timestamp).internal + '.ts')
            self.assertTrue(os.path.isfile(objfile))
            self.assertEqual(2, calls_made[0])
            self.assertEqual(len(os.listdir(os.path.dirname(objfile))), 1)

            # The following request should return a 404, as the object should
            # already have been deleted, and it should not have performed a
            # container update because the timestamp is older, or created a
            # tombstone file with this timestamp.
            timestamp = utils.Timestamp(start + 0.00001)
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': timestamp.internal})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 404)
            objfile = os.path.join(
                self.testdir, 'sda1',
                storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                                  hash_path('a', 'c', 'o')),
                utils.Timestamp(timestamp).internal + '.ts')
            self.assertFalse(os.path.isfile(objfile))
            self.assertEqual(2, calls_made[0])
            self.assertEqual(len(os.listdir(os.path.dirname(objfile))), 1)
        finally:
            self.object_controller.container_update = orig_cu

    def test_DELETE_full_drive(self):

        def mock_diskfile_delete(self, timestamp):
            raise DiskFileNoSpace()

        t_put = utils.Timestamp.now()
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': t_put.internal,
                                     'Content-Length': 0,
                                     'Content-Type': 'plain/text'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        with mock.patch('swift.obj.diskfile.BaseDiskFile.delete',
                        mock_diskfile_delete):
            t_delete = utils.Timestamp.now()
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'},
                                headers={'X-Timestamp': t_delete.internal})
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 507)

    def test_object_update_with_offset(self):
        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))
        # create a new object
        create_timestamp = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o', method='PUT', body='test1',
                            headers={'X-Timestamp': create_timestamp,
                                     'X-Container-Host': '10.0.0.1:8080',
                                     'X-Container-Device': 'sda1',
                                     'X-Container-Partition': 'p',
                                     'Content-Type': 'text/plain'})
        with mocked_http_conn(200, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(1, len(container_updates))
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '8080')
            self.assertEqual(method, 'PUT')
            self.assertEqual(path, '/sda1/p/a/c/o')
            expected = {
                'X-Size': len('test1'),
                'X-Etag': md5('test1').hexdigest(),
                'X-Content-Type': 'text/plain',
                'X-Timestamp': create_timestamp,
            }
            for key, value in expected.items():
                self.assertEqual(headers[key], str(value))
        container_updates = []  # reset
        # read back object
        req = Request.blank('/sda1/p/a/c/o', method='GET')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['X-Timestamp'],
                         utils.Timestamp(create_timestamp).normal)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         create_timestamp)
        self.assertEqual(resp.body, 'test1')
        # send an update with an offset
        offset_timestamp = utils.Timestamp(
            create_timestamp, offset=1).internal
        req = Request.blank('/sda1/p/a/c/o', method='PUT', body='test2',
                            headers={'X-Timestamp': offset_timestamp,
                                     'X-Container-Host': '10.0.0.1:8080',
                                     'X-Container-Device': 'sda1',
                                     'X-Container-Partition': 'p',
                                     'Content-Type': 'text/html'})
        with mocked_http_conn(200, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(1, len(container_updates))
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '8080')
            self.assertEqual(method, 'PUT')
            self.assertEqual(path, '/sda1/p/a/c/o')
            expected = {
                'X-Size': len('test2'),
                'X-Etag': md5('test2').hexdigest(),
                'X-Content-Type': 'text/html',
                'X-Timestamp': offset_timestamp,
            }
            for key, value in expected.items():
                self.assertEqual(headers[key], str(value))
        container_updates = []  # reset
        # read back new offset
        req = Request.blank('/sda1/p/a/c/o', method='GET')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['X-Timestamp'],
                         utils.Timestamp(offset_timestamp).normal)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         offset_timestamp)
        self.assertEqual(resp.body, 'test2')
        # now overwrite with a newer time
        overwrite_timestamp = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o', method='PUT', body='test3',
                            headers={'X-Timestamp': overwrite_timestamp,
                                     'X-Container-Host': '10.0.0.1:8080',
                                     'X-Container-Device': 'sda1',
                                     'X-Container-Partition': 'p',
                                     'Content-Type': 'text/enriched'})
        with mocked_http_conn(200, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(1, len(container_updates))
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '8080')
            self.assertEqual(method, 'PUT')
            self.assertEqual(path, '/sda1/p/a/c/o')
            expected = {
                'X-Size': len('test3'),
                'X-Etag': md5('test3').hexdigest(),
                'X-Content-Type': 'text/enriched',
                'X-Timestamp': overwrite_timestamp,
            }
            for key, value in expected.items():
                self.assertEqual(headers[key], str(value))
        container_updates = []  # reset
        # read back overwrite
        req = Request.blank('/sda1/p/a/c/o', method='GET')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['X-Timestamp'],
                         utils.Timestamp(overwrite_timestamp).normal)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         overwrite_timestamp)
        self.assertEqual(resp.body, 'test3')
        # delete with an offset
        offset_delete = utils.Timestamp(overwrite_timestamp,
                                        offset=1).internal
        req = Request.blank('/sda1/p/a/c/o', method='DELETE',
                            headers={'X-Timestamp': offset_delete,
                                     'X-Container-Host': '10.0.0.1:8080',
                                     'X-Container-Device': 'sda1',
                                     'X-Container-Partition': 'p'})
        with mocked_http_conn(200, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(1, len(container_updates))
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '8080')
            self.assertEqual(method, 'DELETE')
            self.assertEqual(path, '/sda1/p/a/c/o')
            expected = {
                'X-Timestamp': offset_delete,
            }
            for key, value in expected.items():
                self.assertEqual(headers[key], str(value))
        container_updates = []  # reset
        # read back offset delete
        req = Request.blank('/sda1/p/a/c/o', method='GET')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertIsNone(resp.headers['X-Timestamp'])
        self.assertEqual(resp.headers['X-Backend-Timestamp'], offset_delete)
        # and one more delete with a newer timestamp
        delete_timestamp = next(self.ts).internal
        req = Request.blank('/sda1/p/a/c/o', method='DELETE',
                            headers={'X-Timestamp': delete_timestamp,
                                     'X-Container-Host': '10.0.0.1:8080',
                                     'X-Container-Device': 'sda1',
                                     'X-Container-Partition': 'p'})
        with mocked_http_conn(200, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(1, len(container_updates))
        for update in container_updates:
            ip, port, method, path, headers = update
            self.assertEqual(ip, '10.0.0.1')
            self.assertEqual(port, '8080')
            self.assertEqual(method, 'DELETE')
            self.assertEqual(path, '/sda1/p/a/c/o')
            expected = {
                'X-Timestamp': delete_timestamp,
            }
            for key, value in expected.items():
                self.assertEqual(headers[key], str(value))
        container_updates = []  # reset
        # read back delete
        req = Request.blank('/sda1/p/a/c/o', method='GET')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertIsNone(resp.headers['X-Timestamp'])
        self.assertEqual(resp.headers['X-Backend-Timestamp'], delete_timestamp)

    def test_call_bad_request(self):
        # Test swift.obj.server.ObjectController.__call__
        inbuf = WsgiBytesIO()
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
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '400 ')

    def test_call_not_found(self):
        inbuf = WsgiBytesIO()
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
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '404 ')

    def test_call_bad_method(self):
        inbuf = WsgiBytesIO()
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
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_call_name_collision(self):
        def my_check(*args):
            return False

        def my_hash_path(*args):
            return md5('collide').hexdigest()

        with mock.patch("swift.obj.diskfile.hash_path", my_hash_path):
            with mock.patch("swift.obj.server.check_object_creation",
                            my_check):
                inbuf = WsgiBytesIO()
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
                self.assertEqual(errbuf.getvalue(), '')
                self.assertEqual(outbuf.getvalue()[:4], '201 ')

                inbuf = WsgiBytesIO()
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
                self.assertEqual(errbuf.getvalue(), '')
                self.assertEqual(outbuf.getvalue()[:4], '403 ')

    def test_invalid_method_doesnt_exist(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.object_controller.__call__({
            'REQUEST_METHOD': 'method_doesnt_exist',
            'PATH_INFO': '/sda1/p/a/c/o'},
            start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.object_controller.__call__({'REQUEST_METHOD': '__init__',
                                         'PATH_INFO': '/sda1/p/a/c/o'},
                                        start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_chunked_put(self):
        listener = listen_zero()
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
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('GET /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        response = fd.read()
        self.assertEqual(response, 'oh hai')
        killer.kill()

    def test_chunked_content_length_mismatch_zero(self):
        listener = listen_zero()
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
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', port))
        fd = sock.makefile()
        fd.write('GET /sda1/p/a/c/o HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        response = fd.read()
        self.assertEqual(response, 'oh hai')
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
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/' + ('2' * (max_name_len + 1)),
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

    def test_max_upload_time(self):

        class SlowBody(object):

            def __init__(self):
                self.sent = 0

            def read(self, size=-1):
                if self.sent < 4:
                    sleep(0.1)
                    self.sent += 1
                    return ' '
                return ''

            def set_hundred_continue_response_headers(*a, **kw):
                pass

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.object_controller.max_upload_time = 0.1
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 408)

    def test_short_body(self):

        class ShortBody(object):

            def __init__(self):
                self.sent = False

            def read(self, size=-1):
                if not self.sent:
                    self.sent = True
                    return '   '
                return ''

            def set_hundred_continue_response_headers(*a, **kw):
                pass

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': ShortBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 499)

    def test_bad_sinces(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain'},
            body='    ')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Unmodified-Since': 'Not a valid date'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Not a valid date'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        too_big_date_list = list(datetime.datetime.max.timetuple())
        too_big_date_list[0] += 1  # bump up the year
        too_big_date = strftime(
            "%a, %d %b %Y %H:%M:%S UTC", struct_time(too_big_date_list))
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Unmodified-Since': too_big_date})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

    def test_content_encoding(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4', 'Content-Type': 'text/plain',
                     'Content-Encoding': 'gzip'},
            body='    ')
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-encoding'], 'gzip')
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-encoding'], 'gzip')

    def test_async_update_http_connect(self):
        policy = random.choice(list(POLICIES))
        self._stage_tmp_dir(policy)
        given_args = []

        def fake_http_connect(*args):
            given_args.extend(args)
            raise Exception('test')

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.async_update(
                'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                {'x-timestamp': '1', 'x-out': 'set',
                 'X-Backend-Storage-Policy-Index': int(policy)}, 'sda1',
                policy)
        finally:
            object_server.http_connect = orig_http_connect
        self.assertEqual(
            given_args,
            ['127.0.0.1', '1234', 'sdc1', 1, 'PUT', '/a/c/o', {
                'x-timestamp': '1', 'x-out': 'set',
                'user-agent': 'object-server %s' % os.getpid(),
                'X-Backend-Storage-Policy-Index': int(policy)}])

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one'),
                     StoragePolicy(37, 'fantastico')])
    def test_updating_multiple_delete_at_container_servers(self):
        # update router post patch
        self.object_controller._diskfile_router = diskfile.DiskFileRouter(
            self.conf, self.object_controller.logger)
        policy = random.choice(list(POLICIES))
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
                dict((k, v) for k, v in captured_args.items()
                     if v is not None))

            return SuccessfulFakeConn()

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'Content-Type': 'application/burrito',
                     'Content-Length': '0',
                     'X-Backend-Storage-Policy-Index': int(policy),
                     'X-Container-Partition': '20',
                     'X-Container-Host': '1.2.3.4:5',
                     'X-Container-Device': 'sdb1',
                     'X-Delete-At': 9999999999,
                     'X-Delete-At-Container': '9999999960',
                     'X-Delete-At-Host': "10.1.1.1:6201,10.2.2.2:6202",
                     'X-Delete-At-Partition': '6237',
                     'X-Delete-At-Device': 'sdp,sdq'})

        with mock.patch.object(
                object_server, 'http_connect', fake_http_connect):
            with fake_spawn():
                resp = req.get_response(self.object_controller)

        self.assertEqual(resp.status_int, 201)

        http_connect_args.sort(key=operator.itemgetter('ipaddr'))

        self.assertEqual(len(http_connect_args), 3)
        self.assertEqual(
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
                 'x-timestamp': utils.Timestamp('12345').internal,
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'object-server %d' % os.getpid(),
                 'X-Backend-Storage-Policy-Index': int(policy),
                 'x-trans-id': '-'})})
        self.assertEqual(
            http_connect_args[1],
            {'ipaddr': '10.1.1.1',
             'port': '6201',
             'path': '/exp/9999999960/9999999999-a/c/o',
             'device': 'sdp',
             'partition': '6237',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'text/plain',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': utils.Timestamp('12345').internal,
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'object-server %d' % os.getpid(),
                 # system account storage policy is 0
                 'X-Backend-Storage-Policy-Index': 0,
                 'x-trans-id': '-'})})
        self.assertEqual(
            http_connect_args[2],
            {'ipaddr': '10.2.2.2',
             'port': '6202',
             'path': '/exp/9999999960/9999999999-a/c/o',
             'device': 'sdq',
             'partition': '6237',
             'method': 'PUT',
             'ssl': False,
             'headers': HeaderKeyDict({
                 'x-content-type': 'text/plain',
                 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                 'x-size': '0',
                 'x-timestamp': utils.Timestamp('12345').internal,
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'object-server %d' % os.getpid(),
                 # system account storage policy is 0
                 'X-Backend-Storage-Policy-Index': 0,
                 'x-trans-id': '-'})})

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one'),
                     StoragePolicy(26, 'twice-thirteen')])
    def test_updating_multiple_container_servers(self):
        # update router post patch
        self.object_controller._diskfile_router = diskfile.DiskFileRouter(
            self.conf, self.object_controller.logger)
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

            return SuccessfulFakeConn()

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'Content-Type': 'application/burrito',
                     'Content-Length': '0',
                     'X-Backend-Storage-Policy-Index': '26',
                     'X-Container-Partition': '20',
                     'X-Container-Host': '1.2.3.4:5, 6.7.8.9:10',
                     'X-Container-Device': 'sdb1, sdf1'})

        with mock.patch.object(
                object_server, 'http_connect', fake_http_connect):
            with fake_spawn():
                req.get_response(self.object_controller)

        http_connect_args.sort(key=operator.itemgetter('ipaddr'))

        self.assertEqual(len(http_connect_args), 2)
        self.assertEqual(
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
                 'x-timestamp': utils.Timestamp('12345').internal,
                 'X-Backend-Storage-Policy-Index': '26',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'object-server %d' % os.getpid(),
                 'x-trans-id': '-'})})
        self.assertEqual(
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
                 'x-timestamp': utils.Timestamp('12345').internal,
                 'X-Backend-Storage-Policy-Index': '26',
                 'referer': 'PUT http://localhost/sda1/p/a/c/o',
                 'user-agent': 'object-server %d' % os.getpid(),
                 'x-trans-id': '-'})})

    def test_object_delete_at_async_update(self):
        policy = random.choice(list(POLICIES))

        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))

        # put everything in the future; otherwise setting X-Delete-At may fail
        self.ts = make_timestamp_iter(10)

        put_timestamp = next(self.ts).internal
        delete_at_timestamp = utils.normalize_delete_at_timestamp(
            next(self.ts).normal)
        delete_at_container = (
            int(delete_at_timestamp) /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        headers = {
            'Content-Type': 'text/plain',
            'X-Timestamp': put_timestamp,
            'X-Container-Host': '10.0.0.1:6201',
            'X-Container-Device': 'sda1',
            'X-Container-Partition': 'p',
            'X-Delete-At': delete_at_timestamp,
            'X-Delete-At-Container': delete_at_container,
            'X-Delete-At-Partition': 'p',
            'X-Delete-At-Host': '10.0.0.2:6202',
            'X-Delete-At-Device': 'sda1',
            'X-Backend-Storage-Policy-Index': int(policy)}
        if policy.policy_type == EC_POLICY:
            headers['X-Object-Sysmeta-Ec-Frag-Index'] = '2'
        req = Request.blank(
            '/sda1/p/a/c/o', method='PUT', body='', headers=headers)
        with mocked_http_conn(
                500, 500, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
            self.assertEqual(201, resp.status_int, resp.body)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(2, len(container_updates))
        delete_at_update, container_update = container_updates
        # delete_at_update
        ip, port, method, path, headers = delete_at_update
        self.assertEqual(ip, '10.0.0.2')
        self.assertEqual(port, '6202')
        self.assertEqual(method, 'PUT')
        self.assertEqual(path, '/sda1/p/.expiring_objects/%s/%s-a/c/o' %
                         (delete_at_container, delete_at_timestamp))
        expected = {
            'X-Timestamp': put_timestamp,
            # system account storage policy is 0
            'X-Backend-Storage-Policy-Index': 0,
        }
        for key, value in expected.items():
            self.assertEqual(headers[key], str(value))
        # container_update
        ip, port, method, path, headers = container_update
        self.assertEqual(ip, '10.0.0.1')
        self.assertEqual(port, '6201')
        self.assertEqual(method, 'PUT')
        self.assertEqual(path, '/sda1/p/a/c/o')
        expected = {
            'X-Timestamp': put_timestamp,
            'X-Backend-Storage-Policy-Index': int(policy),
        }
        for key, value in expected.items():
            self.assertEqual(headers[key], str(value))
        # check async pendings
        async_dir = os.path.join(self.testdir, 'sda1',
                                 diskfile.get_async_dir(policy))
        found_files = []
        for root, dirs, files in os.walk(async_dir):
            for f in files:
                async_file = os.path.join(root, f)
                found_files.append(async_file)
                data = pickle.load(open(async_file))
                if data['account'] == 'a':
                    self.assertEqual(
                        int(data['headers']
                            ['X-Backend-Storage-Policy-Index']), int(policy))
                elif data['account'] == '.expiring_objects':
                    self.assertEqual(
                        int(data['headers']
                            ['X-Backend-Storage-Policy-Index']), 0)
                else:
                    self.fail('unexpected async pending data')
        self.assertEqual(2, len(found_files))

    def test_async_update_saves_on_exception(self):
        policy = random.choice(list(POLICIES))
        self._stage_tmp_dir(policy)
        _prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_PREFIX = ''

        def fake_http_connect(*args):
            raise Exception('test')

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.async_update(
                'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                {'x-timestamp': '1', 'x-out': 'set',
                 'X-Backend-Storage-Policy-Index': int(policy)}, 'sda1',
                policy)
        finally:
            object_server.http_connect = orig_http_connect
            utils.HASH_PATH_PREFIX = _prefix
        async_dir = diskfile.get_async_dir(policy)
        self.assertEqual(
            pickle.load(open(os.path.join(
                self.testdir, 'sda1', async_dir, 'a83',
                '06fbf0b514e5199dfc4e00f42eb5ea83-%s' %
                utils.Timestamp(1).internal))),
            {'headers': {'x-timestamp': '1', 'x-out': 'set',
                         'user-agent': 'object-server %s' % os.getpid(),
                         'X-Backend-Storage-Policy-Index': int(policy)},
             'account': 'a', 'container': 'c', 'obj': 'o', 'op': 'PUT'})

    def test_async_update_saves_on_non_2xx(self):
        policy = random.choice(list(POLICIES))
        self._stage_tmp_dir(policy)
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
                    {'x-timestamp': '1', 'x-out': str(status),
                     'X-Backend-Storage-Policy-Index': int(policy)}, 'sda1',
                    policy)
                async_dir = diskfile.get_async_dir(policy)
                self.assertEqual(
                    pickle.load(open(os.path.join(
                        self.testdir, 'sda1', async_dir, 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-%s' %
                        utils.Timestamp(1).internal))),
                    {'headers': {'x-timestamp': '1', 'x-out': str(status),
                                 'user-agent':
                                 'object-server %s' % os.getpid(),
                                 'X-Backend-Storage-Policy-Index':
                                 int(policy)},
                     'account': 'a', 'container': 'c', 'obj': 'o',
                     'op': 'PUT'})
        finally:
            object_server.http_connect = orig_http_connect
            utils.HASH_PATH_PREFIX = _prefix

    def test_async_update_does_not_save_on_2xx(self):
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
            for status in (200, 299):
                object_server.http_connect = fake_http_connect(status)
                self.object_controller.async_update(
                    'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                    {'x-timestamp': '1', 'x-out': str(status)}, 'sda1', 0)
                self.assertFalse(
                    os.path.exists(os.path.join(
                        self.testdir, 'sda1', 'async_pending', 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000')))
        finally:
            object_server.http_connect = orig_http_connect
            utils.HASH_PATH_PREFIX = _prefix

    def test_async_update_saves_on_timeout(self):
        policy = random.choice(list(POLICIES))
        self._stage_tmp_dir(policy)
        _prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_PREFIX = ''

        def fake_http_connect():

            class FakeConn(object):

                def getresponse(self):
                    return sleep(1)

            return lambda *args: FakeConn()

        orig_http_connect = object_server.http_connect
        try:
            for status in (200, 299):
                object_server.http_connect = fake_http_connect()
                self.object_controller.node_timeout = 0.001
                self.object_controller.async_update(
                    'PUT', 'a', 'c', 'o', '127.0.0.1:1234', 1, 'sdc1',
                    {'x-timestamp': '1', 'x-out': str(status)}, 'sda1',
                    policy)
                async_dir = diskfile.get_async_dir(policy)
                self.assertTrue(
                    os.path.exists(os.path.join(
                        self.testdir, 'sda1', async_dir, 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-%s' %
                        utils.Timestamp(1).internal)))
        finally:
            object_server.http_connect = orig_http_connect
            utils.HASH_PATH_PREFIX = _prefix

    def test_container_update_no_async_update(self):
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.container_update(
            'PUT', 'a', 'c', 'o', req, {
                'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                'x-content-type': 'text/plain', 'x-timestamp': '1'},
            'sda1', policy)
        self.assertEqual(given_args, [])

    def test_container_update_success(self):
        container_updates = []

        def capture_updates(ip, port, method, path, headers, *args, **kwargs):
            container_updates.append((ip, port, method, path, headers))

        req = Request.blank(
            '/sda1/0/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '123',
                     'X-Container-Host': 'chost:cport',
                     'X-Container-Partition': 'cpartition',
                     'X-Container-Device': 'cdevice',
                     'Content-Type': 'text/plain'}, body='')
        with mocked_http_conn(200, give_connect=capture_updates) as fake_conn:
            with fake_spawn():
                resp = req.get_response(self.object_controller)
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(len(container_updates), 1)
        ip, port, method, path, headers = container_updates[0]
        self.assertEqual(ip, 'chost')
        self.assertEqual(port, 'cport')
        self.assertEqual(method, 'PUT')
        self.assertEqual(path, '/cdevice/cpartition/a/c/o')
        self.assertEqual(headers, HeaderKeyDict({
            'user-agent': 'object-server %s' % os.getpid(),
            'x-size': '0',
            'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'x-content-type': 'text/plain',
            'x-timestamp': utils.Timestamp(1).internal,
            'X-Backend-Storage-Policy-Index': '0',  # default when not given
            'x-trans-id': '123',
            'referer': 'PUT http://localhost/sda1/0/a/c/o'}))

    def test_PUT_container_update_overrides(self):

        def do_test(override_headers):
            container_updates = []

            def capture_updates(
                    ip, port, method, path, headers, *args, **kwargs):
                container_updates.append((ip, port, method, path, headers))

            ts_put = next(self.ts)
            headers = {
                'X-Timestamp': ts_put.internal,
                'X-Trans-Id': '123',
                'X-Container-Host': 'chost:cport',
                'X-Container-Partition': 'cpartition',
                'X-Container-Device': 'cdevice',
                'Content-Type': 'text/plain',
            }
            headers.update(override_headers)
            req = Request.blank('/sda1/0/a/c/o', method='PUT',
                                headers=headers, body='')
            with mocked_http_conn(
                    200, give_connect=capture_updates) as fake_conn:
                with fake_spawn():
                    resp = req.get_response(self.object_controller)
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
            self.assertEqual(resp.status_int, 201)
            self.assertEqual(len(container_updates), 1)
            ip, port, method, path, headers = container_updates[0]
            self.assertEqual(ip, 'chost')
            self.assertEqual(port, 'cport')
            self.assertEqual(method, 'PUT')
            self.assertEqual(path, '/cdevice/cpartition/a/c/o')
            self.assertEqual(headers, HeaderKeyDict({
                'user-agent': 'object-server %s' % os.getpid(),
                'x-size': '0',
                'x-etag': 'override_etag',
                'x-content-type': 'override_val',
                'x-timestamp': ts_put.internal,
                'X-Backend-Storage-Policy-Index': '0',  # default
                'x-trans-id': '123',
                'referer': 'PUT http://localhost/sda1/0/a/c/o',
                'x-foo': 'bar'}))

        # EC policy override headers
        do_test({
            'X-Backend-Container-Update-Override-Etag': 'override_etag',
            'X-Backend-Container-Update-Override-Content-Type': 'override_val',
            'X-Backend-Container-Update-Override-Foo': 'bar',
            'X-Backend-Container-Ignored': 'ignored'})

        # middleware override headers
        do_test({
            'X-Object-Sysmeta-Container-Update-Override-Etag': 'override_etag',
            'X-Object-Sysmeta-Container-Update-Override-Content-Type':
                'override_val',
            'X-Object-Sysmeta-Container-Update-Override-Foo': 'bar',
            'X-Object-Sysmeta-Ignored': 'ignored'})

        # middleware override headers take precedence over EC policy headers
        do_test({
            'X-Object-Sysmeta-Container-Update-Override-Etag': 'override_etag',
            'X-Object-Sysmeta-Container-Update-Override-Content-Type':
                'override_val',
            'X-Object-Sysmeta-Container-Update-Override-Foo': 'bar',
            'X-Backend-Container-Update-Override-Etag': 'ignored',
            'X-Backend-Container-Update-Override-Content-Type': 'ignored',
            'X-Backend-Container-Update-Override-Foo': 'ignored'})

    def test_container_update_async(self):
        policy = random.choice(list(POLICIES))
        req = Request.blank(
            '/sda1/0/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '123',
                     'X-Container-Host': 'chost:cport',
                     'X-Container-Partition': 'cpartition',
                     'X-Container-Device': 'cdevice',
                     'Content-Type': 'text/plain',
                     'X-Object-Sysmeta-Ec-Frag-Index': 0,
                     'X-Backend-Storage-Policy-Index': int(policy)}, body='')
        given_args = []

        def fake_pickle_async_update(*args):
            given_args[:] = args
        diskfile_mgr = self.object_controller._diskfile_router[policy]
        diskfile_mgr.pickle_async_update = fake_pickle_async_update
        with mocked_http_conn(500) as fake_conn, fake_spawn():
            resp = req.get_response(self.object_controller)
        # fake_spawn() above waits on greenthreads to finish;
        # don't start making assertions until then
        self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(len(given_args), 7)
        (objdevice, account, container, obj, data, timestamp,
         policy) = given_args
        self.assertEqual(objdevice, 'sda1')
        self.assertEqual(account, 'a')
        self.assertEqual(container, 'c')
        self.assertEqual(obj, 'o')
        self.assertEqual(timestamp, utils.Timestamp(1).internal)
        self.assertEqual(policy, policy)
        self.assertEqual(data, {
            'headers': HeaderKeyDict({
                'X-Size': '0',
                'User-Agent': 'object-server %s' % os.getpid(),
                'X-Content-Type': 'text/plain',
                'X-Timestamp': utils.Timestamp(1).internal,
                'X-Trans-Id': '123',
                'Referer': 'PUT http://localhost/sda1/0/a/c/o',
                'X-Backend-Storage-Policy-Index': int(policy),
                'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e'}),
            'obj': 'o',
            'account': 'a',
            'container': 'c',
            'op': 'PUT'})

    def test_container_update_as_greenthread(self):
        greenthreads = []
        saved_spawn_calls = []
        called_async_update_args = []

        def local_fake_spawn(func, *a, **kw):
            saved_spawn_calls.append((func, a, kw))
            return mock.MagicMock()

        def local_fake_async_update(*a, **kw):
            # just capture the args to see that we would have called
            called_async_update_args.append([a, kw])

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'Content-Type': 'application/burrito',
                     'Content-Length': '0',
                     'X-Backend-Storage-Policy-Index': 0,
                     'X-Container-Partition': '20',
                     'X-Container-Host': '1.2.3.4:5',
                     'X-Container-Device': 'sdb1'})
        with mock.patch.object(object_server, 'spawn',
                               local_fake_spawn):
            with mock.patch.object(self.object_controller,
                                   'async_update',
                                   local_fake_async_update):
                resp = req.get_response(self.object_controller)
        # check the response is completed and successful
        self.assertEqual(resp.status_int, 201)
        # check that async_update hasn't been called
        self.assertFalse(len(called_async_update_args))
        # now do the work in greenthreads
        for func, a, kw in saved_spawn_calls:
            gt = spawn(func, *a, **kw)
            greenthreads.append(gt)
        # wait for the greenthreads to finish
        for gt in greenthreads:
            gt.wait()
        # check that the calls to async_update have happened
        headers_out = {'X-Size': '0',
                       'X-Content-Type': 'application/burrito',
                       'X-Timestamp': '0000012345.00000',
                       'X-Trans-Id': '-',
                       'Referer': 'PUT http://localhost/sda1/p/a/c/o',
                       'X-Backend-Storage-Policy-Index': '0',
                       'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e'}
        expected = [('PUT', 'a', 'c', 'o', '1.2.3.4:5', '20', 'sdb1',
                     headers_out, 'sda1', POLICIES[0]),
                    {'logger_thread_locals': (None, None)}]
        self.assertEqual(called_async_update_args, [expected])

    def test_container_update_as_greenthread_with_timeout(self):
        '''
        give it one container to update (for only one greenthred)
        fake the greenthred so it will raise a timeout
        test that the right message is logged and the method returns None
        '''
        called_async_update_args = []

        def local_fake_spawn(func, *a, **kw):
            m = mock.MagicMock()

            def wait_with_error():
                raise Timeout()
            m.wait = wait_with_error  # because raise can't be in a lambda
            return m

        def local_fake_async_update(*a, **kw):
            # just capture the args to see that we would have called
            called_async_update_args.append([a, kw])

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': '12345',
                     'Content-Type': 'application/burrito',
                     'Content-Length': '0',
                     'X-Backend-Storage-Policy-Index': 0,
                     'X-Container-Partition': '20',
                     'X-Container-Host': '1.2.3.4:5',
                     'X-Container-Device': 'sdb1'})
        with mock.patch.object(object_server, 'spawn',
                               local_fake_spawn):
            with mock.patch.object(self.object_controller,
                                   'container_update_timeout',
                                   1.414213562):
                resp = req.get_response(self.object_controller)
        # check the response is completed and successful
        self.assertEqual(resp.status_int, 201)
        # check that the timeout was logged
        expected_logged_error = "Container update timeout (1.4142s) " \
            "waiting for [('1.2.3.4:5', 'sdb1')]"
        self.assertTrue(
            expected_logged_error in
            self.object_controller.logger.get_lines_for_level('debug'))

    def test_container_update_bad_args(self):
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '123',
                     'X-Container-Host': 'chost,badhost',
                     'X-Container-Partition': 'cpartition',
                     'X-Container-Device': 'cdevice',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        with mock.patch.object(self.object_controller, 'async_update',
                               fake_async_update):
            self.object_controller.container_update(
                'PUT', 'a', 'c', 'o', req, {
                    'x-size': '0',
                    'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-content-type': 'text/plain', 'x-timestamp': '1'},
                'sda1', policy)
        self.assertEqual(given_args, [])
        errors = self.object_controller.logger.get_lines_for_level('error')
        self.assertEqual(len(errors), 1)
        msg = errors[0]
        self.assertTrue('Container update failed' in msg)
        self.assertTrue('different numbers of hosts and devices' in msg)
        self.assertTrue('chost,badhost' in msg)
        self.assertTrue('cdevice' in msg)

    def test_delete_at_update_cleans_old_entries(self):
        # Test how delete_at_update works with a request to overwrite an object
        # with delete-at metadata
        policy = random.choice(list(POLICIES))

        def do_test(method, headers, expected_args):
            given_args = []

            def fake_async_update(*args):
                given_args.extend(args)

            headers.update({'X-Timestamp': 1,
                            'X-Trans-Id': '123',
                            'X-Backend-Storage-Policy-Index': int(policy)})
            req = Request.blank(
                '/v1/a/c/o',
                environ={'REQUEST_METHOD': method},
                headers=headers)
            with mock.patch.object(self.object_controller, 'async_update',
                                   fake_async_update):
                self.object_controller.delete_at_update(
                    'DELETE', 2, 'a', 'c', 'o', req, 'sda1', policy)
            self.assertEqual(expected_args, given_args)

        for method in ('PUT', 'POST', 'DELETE'):
            expected_args = [
                'DELETE', '.expiring_objects', '0000000000',
                '0000000002-a/c/o', None, None,
                None, HeaderKeyDict({
                    'X-Backend-Storage-Policy-Index': 0,
                    'x-timestamp': utils.Timestamp('1').internal,
                    'x-trans-id': '123',
                    'referer': '%s http://localhost/v1/a/c/o' % method}),
                'sda1', policy]
            # async_update should be called by default...
            do_test(method, {}, expected_args)
            do_test(method, {'X-Backend-Clean-Expiring-Object-Queue': 'true'},
                    expected_args)
            do_test(method, {'X-Backend-Clean-Expiring-Object-Queue': 't'},
                    expected_args)
            # ...unless header has a false value
            do_test(method, {'X-Backend-Clean-Expiring-Object-Queue': 'false'},
                    [])
            do_test(method, {'X-Backend-Clean-Expiring-Object-Queue': 'f'}, [])

    def test_delete_at_negative(self):
        # Test how delete_at_update works when issued a delete for old
        # expiration info after a new put with no new expiration info.
        # Test negative is reset to 0
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234', 'X-Backend-Storage-Policy-Index':
                     int(policy)})
        self.object_controller.delete_at_update(
            'DELETE', -2, 'a', 'c', 'o', req, 'sda1', policy)
        self.assertEqual(given_args, [
            'DELETE', '.expiring_objects', '0000000000', '0000000000-a/c/o',
            None, None, None,
            HeaderKeyDict({
                # the expiring objects account is always 0
                'X-Backend-Storage-Policy-Index': 0,
                'x-timestamp': utils.Timestamp('1').internal,
                'x-trans-id': '1234',
                'referer': 'PUT http://localhost/v1/a/c/o'}),
            'sda1', policy])

    def test_delete_at_cap(self):
        # Test how delete_at_update works when issued a delete for old
        # expiration info after a new put with no new expiration info.
        # Test past cap is reset to cap
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update(
            'DELETE', 12345678901, 'a', 'c', 'o', req, 'sda1', policy)
        expiring_obj_container = given_args.pop(2)
        expected_exp_cont = utils.get_expirer_container(
            utils.normalize_delete_at_timestamp(12345678901),
            86400, 'a', 'c', 'o')
        self.assertEqual(expiring_obj_container, expected_exp_cont)

        self.assertEqual(given_args, [
            'DELETE', '.expiring_objects', '9999999999-a/c/o',
            None, None, None,
            HeaderKeyDict({
                'X-Backend-Storage-Policy-Index': 0,
                'x-timestamp': utils.Timestamp('1').internal,
                'x-trans-id': '1234',
                'referer': 'PUT http://localhost/v1/a/c/o'}),
            'sda1', policy])

    def test_delete_at_update_put_with_info(self):
        # Keep next test,
        # test_delete_at_update_put_with_info_but_missing_container, in sync
        # with this one but just missing the X-Delete-At-Container header.
        policy = random.choice(list(POLICIES))
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
                     'X-Delete-At-Device': 'sdc1',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
                                                req, 'sda1', policy)
        self.assertEqual(
            given_args, [
                'PUT', '.expiring_objects', '0000000000', '0000000002-a/c/o',
                '127.0.0.1:1234',
                '3', 'sdc1', HeaderKeyDict({
                    # the .expiring_objects account is always policy-0
                    'X-Backend-Storage-Policy-Index': 0,
                    'x-size': '0',
                    'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-content-type': 'text/plain',
                    'x-timestamp': utils.Timestamp('1').internal,
                    'x-trans-id': '1234',
                    'referer': 'PUT http://localhost/v1/a/c/o'}),
                'sda1', policy])

    def test_delete_at_update_put_with_info_but_missing_container(self):
        # Same as previous test, test_delete_at_update_put_with_info, but just
        # missing the X-Delete-At-Container header.
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.logger = self.logger
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Delete-At-Host': '127.0.0.1:1234',
                     'X-Delete-At-Partition': '3',
                     'X-Delete-At-Device': 'sdc1',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
                                                req, 'sda1', policy)
        self.assertEqual(
            self.logger.get_lines_for_level('warning'),
            ['X-Delete-At-Container header must be specified for expiring '
             'objects background PUT to work properly. Making best guess as '
             'to the container name for now.'])
        self.assertEqual(
            given_args, [
                'PUT', '.expiring_objects', '0000000000', '0000000002-a/c/o',
                '127.0.0.1:1234',
                '3', 'sdc1', HeaderKeyDict({
                    # the .expiring_objects account is always policy-0
                    'X-Backend-Storage-Policy-Index': 0,
                    'x-size': '0',
                    'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-content-type': 'text/plain',
                    'x-timestamp': utils.Timestamp('1').internal,
                    'x-trans-id': '1234',
                    'referer': 'PUT http://localhost/v1/a/c/o'}),
                'sda1', policy])

    def test_delete_at_update_put_with_info_but_missing_host(self):
        # Same as test_delete_at_update_put_with_info, but just
        # missing the X-Delete-At-Host header.
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.logger = self.logger
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Delete-At-Container': '0',
                     'X-Delete-At-Partition': '3',
                     'X-Delete-At-Device': 'sdc1',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
                                                req, 'sda1', policy)
        self.assertFalse(self.logger.get_lines_for_level('warning'))
        self.assertEqual(given_args, [])

    def test_delete_at_update_put_with_info_but_empty_host(self):
        # Same as test_delete_at_update_put_with_info, but empty
        # X-Delete-At-Host header and no X-Delete-At-Partition nor
        # X-Delete-At-Device.
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.logger = self.logger
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Delete-At-Container': '0',
                     'X-Delete-At-Host': '',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
                                                req, 'sda1', policy)
        self.assertFalse(self.logger.get_lines_for_level('warning'))
        self.assertEqual(
            given_args, [
                'PUT', '.expiring_objects', '0000000000', '0000000002-a/c/o',
                None,
                None, None, HeaderKeyDict({
                    # the .expiring_objects account is always policy-0
                    'X-Backend-Storage-Policy-Index': 0,
                    'x-size': '0',
                    'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                    'x-content-type': 'text/plain',
                    'x-timestamp': utils.Timestamp('1').internal,
                    'x-trans-id': '1234',
                    'referer': 'PUT http://localhost/v1/a/c/o'}),
                'sda1', policy])

    def test_delete_at_update_delete(self):
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update('DELETE', 2, 'a', 'c', 'o',
                                                req, 'sda1', policy)
        self.assertEqual(
            given_args, [
                'DELETE', '.expiring_objects', '0000000000',
                '0000000002-a/c/o', None, None,
                None, HeaderKeyDict({
                    'X-Backend-Storage-Policy-Index': 0,
                    'x-timestamp': utils.Timestamp('1').internal,
                    'x-trans-id': '1234',
                    'referer': 'DELETE http://localhost/v1/a/c/o'}),
                'sda1', policy])

    def test_delete_backend_replication(self):
        # If X-Backend-Replication: True delete_at_update should completely
        # short-circuit.
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        req = Request.blank(
            '/v1/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': 1,
                     'X-Trans-Id': '1234',
                     'X-Backend-Replication': 'True',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        self.object_controller.delete_at_update(
            'DELETE', -2, 'a', 'c', 'o', req, 'sda1', policy)
        self.assertEqual(given_args, [])

    def test_POST_calls_delete_at(self):
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Backend-Storage-Policy-Index': int(policy),
                     'X-Object-Sysmeta-Ec-Frag-Index': 2})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(given_args, [])

        sleep(.00001)
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/x-test',
                     'X-Backend-Storage-Policy-Index': int(policy)})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(given_args, [])

        sleep(.00001)
        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = str(int(time() + 1000))
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'application/x-test',
                     'X-Delete-At': delete_at_timestamp1,
                     'X-Backend-Storage-Policy-Index': int(policy)})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(
            given_args, [
                'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
                given_args[5], 'sda1', policy])

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
                     'X-Delete-At': delete_at_timestamp2,
                     'X-Backend-Storage-Policy-Index': int(policy)})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(
            given_args, [
                'PUT', int(delete_at_timestamp2), 'a', 'c', 'o',
                given_args[5], 'sda1', policy,
                'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
                given_args[5], 'sda1', policy])

    def test_PUT_calls_delete_at(self):
        policy = random.choice(list(POLICIES))
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Backend-Storage-Policy-Index': int(policy),
                     'X-Object-Sysmeta-Ec-Frag-Index': 4})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(given_args, [])

        sleep(.00001)
        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = str(int(time() + 1000))
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': delete_at_timestamp1,
                     'X-Backend-Storage-Policy-Index': int(policy),
                     'X-Object-Sysmeta-Ec-Frag-Index': 3})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(
            given_args, [
                'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
                given_args[5], 'sda1', policy])

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
                     'X-Delete-At': delete_at_timestamp2,
                     'X-Backend-Storage-Policy-Index': int(policy),
                     'X-Object-Sysmeta-Ec-Frag-Index': 3})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(
            given_args, [
                'PUT', int(delete_at_timestamp2), 'a', 'c', 'o',
                given_args[5], 'sda1', policy,
                'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
                given_args[5], 'sda1', policy])

    def test_GET_but_expired(self):
        # Start off with an existing object that will expire
        now = time()
        delete_at_timestamp = int(now + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(now),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # It expires in the future, so it's accessible via GET
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': normalize_timestamp(now)})
        with mock.patch('swift.obj.server.time.time', return_value=now):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # It expires in the past, so it's not accessible via GET...
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': normalize_timestamp(
                delete_at_timestamp + 1)})
        with mock.patch('swift.obj.server.time.time',
                        return_value=delete_at_timestamp + 1):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         utils.Timestamp(now))

        with mock.patch('swift.obj.server.time.time',
                        return_value=delete_at_timestamp + 1):
            # ...unless X-Backend-Replication is sent
            expected = {
                'GET': 'TEST',
                'HEAD': '',
            }
            for meth, expected_body in expected.items():
                req = Request.blank(
                    '/sda1/p/a/c/o', method=meth,
                    headers={'X-Timestamp':
                             normalize_timestamp(delete_at_timestamp + 1),
                             'X-Backend-Replication': 'True'})
                resp = req.get_response(self.object_controller)
                self.assertEqual(resp.status_int, 200)
                self.assertEqual(expected_body, resp.body)

    def test_HEAD_but_expired(self):
        # We have an object that expires in the future
        now = time()
        delete_at_timestamp = int(now + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(now),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # It's accessible since it expires in the future
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'X-Timestamp': normalize_timestamp(now)})
        with mock.patch('swift.obj.server.time.time', return_value=now):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

        # It's not accessible now since it expires in the past
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'X-Timestamp': normalize_timestamp(
                delete_at_timestamp + 1)})
        with mock.patch('swift.obj.server.time.time',
                        return_value=delete_at_timestamp + 1):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Backend-Timestamp'],
                         utils.Timestamp(now))

    def test_POST_but_expired(self):
        now = time()
        delete_at_timestamp = int(now + 100)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)

        # We recreate the test object every time to ensure a clean test; a
        # POST may change attributes of the object, so it's not safe to
        # re-use.
        def recreate_test_object(when):
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(when),
                         'X-Delete-At': str(delete_at_timestamp),
                         'X-Delete-At-Container': delete_at_container,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
            req.body = 'TEST'
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 201)

        # You can POST to a not-yet-expired object
        recreate_test_object(now)
        the_time = now + 1
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(the_time)})
        with mock.patch('swift.obj.server.time.time', return_value=the_time):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        # You cannot POST to an expired object
        now += 2
        recreate_test_object(now)
        the_time = delete_at_timestamp + 1
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(the_time)})
        with mock.patch('swift.obj.server.time.time', return_value=the_time):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 404)

    def test_DELETE_can_skip_updating_expirer_queue(self):
        policy = POLICIES.get_by_index(0)
        test_time = time()
        put_time = test_time
        delete_time = test_time + 1
        delete_at_timestamp = int(test_time + 10000)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(put_time),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'

        # Mock out async_update so we don't get any async_pending files.
        with mock.patch.object(self.object_controller, 'async_update'):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(delete_time),
                     'X-Backend-Clean-Expiring-Object-Queue': 'false',
                     'X-If-Delete-At': str(delete_at_timestamp)})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

        async_pending_dir = os.path.join(
            self.testdir, 'sda1', diskfile.get_async_dir(policy))
        # empty dir or absent dir, either is fine
        try:
            self.assertEqual([], os.listdir(async_pending_dir))
        except OSError as err:
            self.assertEqual(err.errno, errno.ENOENT)

    def test_x_if_delete_at_formats(self):
        policy = POLICIES.get_by_index(0)
        test_time = time()
        put_time = test_time
        delete_time = test_time + 1
        delete_at_timestamp = int(test_time + 10000)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)

        def do_test(if_delete_at, expected_status):
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(put_time),
                         'X-Delete-At': str(delete_at_timestamp),
                         'X-Delete-At-Container': delete_at_container,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
            req.body = 'TEST'

            # Mock out async_update so we don't get any async_pending files.
            with mock.patch.object(self.object_controller, 'async_update'):
                resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 201)

            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': normalize_timestamp(delete_time),
                         'X-Backend-Clean-Expiring-Object-Queue': 'false',
                         'X-If-Delete-At': if_delete_at})
            # Again, we don't care about async_pending files (for this test)
            with mock.patch.object(self.object_controller, 'async_update'):
                resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, expected_status)

            # Clean up the tombstone
            objfile = self.df_mgr.get_diskfile('sda1', 'p', 'a', 'c', 'o',
                                               policy=policy)
            files = os.listdir(objfile._datadir)
            self.assertEqual(len(files), 1,
                             'Expected to find one file, got %r' % files)
            if expected_status == 204:
                self.assertTrue(files[0].endswith('.ts'),
                                'Expected a tombstone, found %r' % files[0])
            else:
                self.assertTrue(files[0].endswith('.data'),
                                'Expected a data file, found %r' % files[0])
            os.unlink(os.path.join(objfile._datadir, files[0]))

        # More as a reminder than anything else
        self.assertIsInstance(delete_at_timestamp, int)

        do_test(str(delete_at_timestamp), 204)
        do_test(str(delete_at_timestamp) + ':', 400)
        do_test(Timestamp(delete_at_timestamp).isoformat, 400)
        do_test(Timestamp(delete_at_timestamp).normal, 204)
        do_test(Timestamp(delete_at_timestamp, delta=1).normal, 412)
        do_test(Timestamp(delete_at_timestamp, delta=-1).normal, 412)
        do_test(Timestamp(delete_at_timestamp, offset=1).internal, 412)
        do_test(Timestamp(delete_at_timestamp, offset=15).internal, 412)

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
        self.assertEqual(resp.status_int, 201)

        orig_time = object_server.time.time
        try:
            t = test_time + 100
            object_server.time.time = lambda: float(t)
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_DELETE_if_delete_at_expired_still_deletes(self):
        test_time = time() + 10
        test_timestamp = normalize_timestamp(test_time)
        delete_at_time = int(test_time + 10)
        delete_at_timestamp = str(delete_at_time)
        delete_at_container = str(
            delete_at_time /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': test_timestamp,
                     'X-Delete-At': delete_at_timestamp,
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # sanity
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': test_timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, 'TEST')
        objfile = os.path.join(
            self.testdir, 'sda1',
            storage_directory(diskfile.get_data_dir(POLICIES[0]), 'p',
                              hash_path('a', 'c', 'o')),
            utils.Timestamp(test_timestamp).internal + '.data')
        self.assertTrue(os.path.isfile(objfile))

        # move time past expiry
        with mock.patch('swift.obj.diskfile.time') as mock_time:
            mock_time.time.return_value = test_time + 100
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Timestamp': test_timestamp})
            resp = req.get_response(self.object_controller)
            # request will 404
            self.assertEqual(resp.status_int, 404)
            # but file still exists
            self.assertTrue(os.path.isfile(objfile))

            # make the x-if-delete-at with some wrong bits
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': delete_at_timestamp,
                         'X-If-Delete-At': int(time() + 1)})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 412)
            self.assertTrue(os.path.isfile(objfile))

            # make the x-if-delete-at with all the right bits
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': delete_at_timestamp,
                         'X-If-Delete-At': delete_at_timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 204)
            self.assertFalse(os.path.isfile(objfile))

            # make the x-if-delete-at with all the right bits (again)
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': delete_at_timestamp,
                         'X-If-Delete-At': delete_at_timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 409)
            self.assertFalse(os.path.isfile(objfile))

            # overwrite with new content
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={
                    'X-Timestamp': str(test_time + 100),
                    'Content-Length': '0',
                    'Content-Type': 'application/octet-stream'})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 201, resp.body)

            # simulate processing a stale expirer queue entry
            req = Request.blank(
                '/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': delete_at_timestamp,
                         'X-If-Delete-At': delete_at_timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 409)

            # make the x-if-delete-at for some not found
            req = Request.blank(
                '/sda1/p/a/c/o-not-found',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': delete_at_timestamp,
                         'X-If-Delete-At': delete_at_timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 404)

    def test_DELETE_if_delete_at(self):
        test_time = time() + 10000
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 99),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 98)})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

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
        self.assertEqual(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 95),
                     'X-If-Delete-At': str(int(test_time))})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 95)})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

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
        self.assertEqual(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': str(int(test_time))})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 412)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': delete_at_timestamp})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)

        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': 'abc'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)

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
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertEqual(given_args, [
            'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
            given_args[5], 'sda1', POLICIES[0]])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Type': 'application/octet-stream'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(given_args, [
            'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
            given_args[5], 'sda1', POLICIES[0]])

    def test_PUT_can_skip_updating_expirer_queue(self):
        policy = POLICIES.get_by_index(0)
        test_time = time()
        put_time = test_time
        overwrite_time = test_time + 1
        delete_at_timestamp = int(test_time + 10000)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(put_time),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'

        # Mock out async_update so we don't get any async_pending files.
        with mock.patch.object(self.object_controller, 'async_update'):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # Overwrite with a non-expiring object
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(overwrite_time),
                     'X-Backend-Clean-Expiring-Object-Queue': 'false',
                     'Content-Length': '9',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'new stuff'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        async_pending_dir = os.path.join(
            self.testdir, 'sda1', diskfile.get_async_dir(policy))
        # empty dir or absent dir, either is fine
        try:
            self.assertEqual([], os.listdir(async_pending_dir))
        except OSError as err:
            self.assertEqual(err.errno, errno.ENOENT)

    def test_PUT_can_skip_deleting_expirer_queue_but_still_inserts(self):
        policy = POLICIES.get_by_index(0)
        test_time = time()
        put_time = test_time
        overwrite_time = test_time + 1
        delete_at_timestamp_1 = int(test_time + 10000)
        delete_at_timestamp_2 = int(test_time + 20000)
        delete_at_container_1 = str(
            delete_at_timestamp_1 /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        delete_at_container_2 = str(
            delete_at_timestamp_2 /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(put_time),
                     'X-Delete-At': str(delete_at_timestamp_1),
                     'X-Delete-At-Container': delete_at_container_1,
                     'X-Delete-At-Host': '1.2.3.4',
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'

        # Mock out async_update so we don't get any async_pending files.
        with mock.patch.object(self.object_controller, 'async_update'):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # Overwrite with an expiring object
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(overwrite_time),
                     'X-Backend-Clean-Expiring-Object-Queue': 'false',
                     'X-Delete-At': str(delete_at_timestamp_2),
                     'X-Delete-At-Container': delete_at_container_2,
                     'X-Delete-At-Host': '1.2.3.4',
                     'Content-Length': '9',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'new stuff'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        async_pendings = []
        async_pending_dir = os.path.join(
            self.testdir, 'sda1', diskfile.get_async_dir(policy))
        for dirpath, _, filenames in os.walk(async_pending_dir):
            for filename in filenames:
                async_pendings.append(os.path.join(dirpath, filename))

        self.assertEqual(len(async_pendings), 1)

        async_pending_ops = []
        for pending_file in async_pendings:
            with open(pending_file) as fh:
                async_pending = pickle.load(fh)
                async_pending_ops.append(async_pending['op'])
        self.assertEqual(async_pending_ops, ['PUT'])

    def test_PUT_delete_at_in_past(self):
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'X-Delete-At': str(int(time() - 1)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue('X-Delete-At in past' in resp.body)

    def test_POST_can_skip_updating_expirer_queue(self):
        policy = POLICIES.get_by_index(0)
        test_time = time()
        put_time = test_time
        overwrite_time = test_time + 1
        delete_at_timestamp = int(test_time + 10000)
        delete_at_container = str(
            delete_at_timestamp /
            self.object_controller.expiring_objects_container_divisor *
            self.object_controller.expiring_objects_container_divisor)
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(put_time),
                     'X-Delete-At': str(delete_at_timestamp),
                     'X-Delete-At-Container': delete_at_container,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'

        # Mock out async_update so we don't get any async_pending files.
        with mock.patch.object(self.object_controller, 'async_update'):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        # POST to remove X-Delete-At
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(overwrite_time),
                     'X-Backend-Clean-Expiring-Object-Queue': 'false',
                     'X-Delete-At': ''})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 202)

        async_pending_dir = os.path.join(
            self.testdir, 'sda1', diskfile.get_async_dir(policy))
        # empty dir or absent dir, either is fine
        try:
            self.assertEqual([], os.listdir(async_pending_dir))
        except OSError as err:
            self.assertEqual(err.errno, errno.ENOENT)

    def test_POST_delete_at_in_past(self):
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time() + 1),
                     'X-Delete-At': str(int(time() - 1))})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue('X-Delete-At in past' in resp.body)

    def test_REPLICATE_works(self):

        def fake_get_hashes(*args, **kwargs):
            return 0, {1: 2}

        def my_tpool_execute(func, *args, **kwargs):
            return func(*args, **kwargs)

        was_get_hashes = diskfile.DiskFileManager._get_hashes
        was_tpool_exe = tpool.execute
        try:
            diskfile.DiskFileManager._get_hashes = fake_get_hashes
            tpool.execute = my_tpool_execute
            req = Request.blank('/sda1/p/suff',
                                environ={'REQUEST_METHOD': 'REPLICATE'},
                                headers={})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 200)
            p_data = pickle.loads(resp.body)
            self.assertEqual(p_data, {1: 2})
        finally:
            tpool.execute = was_tpool_exe
            diskfile.DiskFileManager._get_hashes = was_get_hashes

    def test_REPLICATE_timeout(self):

        def fake_get_hashes(*args, **kwargs):
            raise Timeout()

        def my_tpool_execute(func, *args, **kwargs):
            return func(*args, **kwargs)

        was_get_hashes = diskfile.DiskFileManager._get_hashes
        was_tpool_exe = tpool.execute
        try:
            diskfile.DiskFileManager._get_hashes = fake_get_hashes
            tpool.execute = my_tpool_execute
            req = Request.blank('/sda1/p/suff',
                                environ={'REQUEST_METHOD': 'REPLICATE'},
                                headers={})
            self.assertRaises(Timeout, self.object_controller.REPLICATE, req)
        finally:
            tpool.execute = was_tpool_exe
            diskfile.DiskFileManager._get_hashes = was_get_hashes

    def test_REPLICATE_reclaims_tombstones(self):
        conf = {'devices': self.testdir, 'mount_check': False,
                'reclaim_age': 100}
        self.object_controller = object_server.ObjectController(
            conf, logger=self.logger)
        for policy in self.iter_policies():
            # create a tombstone
            ts = next(self.ts)
            delete_request = Request.blank(
                '/sda1/0/a/c/o', method='DELETE',
                headers={
                    'x-backend-storage-policy-index': int(policy),
                    'x-timestamp': ts.internal,
                })
            resp = delete_request.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 404)
            objfile = self.df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                               policy=policy)
            tombstone_file = os.path.join(objfile._datadir,
                                          '%s.ts' % ts.internal)
            self.assertTrue(os.path.exists(tombstone_file))

            # REPLICATE will hash it
            req = Request.blank(
                '/sda1/0', method='REPLICATE',
                headers={
                    'x-backend-storage-policy-index': int(policy),
                })
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 200)
            suffix = pickle.loads(resp.body).keys()[0]
            self.assertEqual(suffix, os.path.basename(
                os.path.dirname(objfile._datadir)))
            # tombstone still exists
            self.assertTrue(os.path.exists(tombstone_file))

            # after reclaim REPLICATE will rehash
            replicate_request = Request.blank(
                '/sda1/0/%s' % suffix, method='REPLICATE',
                headers={
                    'x-backend-storage-policy-index': int(policy),
                })
            the_future = time() + 200
            with mock.patch('swift.obj.diskfile.time.time') as mock_time:
                mock_time.return_value = the_future
                resp = replicate_request.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual({}, pickle.loads(resp.body))
            # and tombstone is reaped!
            self.assertFalse(os.path.exists(tombstone_file))

            # N.B. with a small reclaim age like this - if proxy clocks get far
            # enough out of whack ...
            with mock.patch('swift.obj.diskfile.time.time') as mock_time:
                mock_time.return_value = the_future
                resp = delete_request.get_response(self.object_controller)
                # we won't even create the tombstone
                self.assertFalse(os.path.exists(tombstone_file))
                # hashdir sticks around tho
                self.assertTrue(os.path.exists(objfile._datadir))
                # REPLICATE will clean it all up
                resp = replicate_request.get_response(self.object_controller)
                self.assertEqual(resp.status_int, 200)
                self.assertEqual({}, pickle.loads(resp.body))
                self.assertFalse(os.path.exists(objfile._datadir))

    def test_SSYNC_can_be_called(self):
        req = Request.blank('/sda1/0',
                            environ={'REQUEST_METHOD': 'SSYNC'},
                            headers={})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)

    def test_PUT_with_full_drive(self):

        class IgnoredBody(object):

            def __init__(self):
                self.read_called = False

            def read(self, size=-1):
                if not self.read_called:
                    self.read_called = True
                    return 'VERIFY'
                return ''

        def fake_fallocate(fd, size):
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

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
            self.assertEqual(resp.status_int, 507)
            self.assertFalse(body_reader.read_called)
        finally:
            diskfile.fallocate = orig_fallocate

    def test_global_conf_callback_does_nothing(self):
        preloaded_app_conf = {}
        global_conf = {}
        object_server.global_conf_callback(preloaded_app_conf, global_conf)
        self.assertEqual(preloaded_app_conf, {})
        self.assertEqual(global_conf.keys(), ['replication_semaphore'])
        try:
            value = global_conf['replication_semaphore'][0].get_value()
        except NotImplementedError:
            # On some operating systems (at a minimum, OS X) it's not possible
            # to introspect the value of a semaphore
            raise unittest.SkipTest
        else:
            self.assertEqual(value, 4)

    def test_global_conf_callback_replication_semaphore(self):
        preloaded_app_conf = {'replication_concurrency': 123}
        global_conf = {}
        with mock.patch.object(
                object_server.multiprocessing, 'BoundedSemaphore',
                return_value='test1') as mocked_Semaphore:
            object_server.global_conf_callback(preloaded_app_conf, global_conf)
        self.assertEqual(preloaded_app_conf, {'replication_concurrency': 123})
        self.assertEqual(global_conf, {'replication_semaphore': ['test1']})
        mocked_Semaphore.assert_called_once_with(123)

    def test_handling_of_replication_semaphore_config(self):
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        objsrv = object_server.ObjectController(conf)
        self.assertTrue(objsrv.replication_semaphore is None)
        conf['replication_semaphore'] = ['sema']
        objsrv = object_server.ObjectController(conf)
        self.assertEqual(objsrv.replication_semaphore, 'sema')

    def test_serv_reserv(self):
        # Test replication_server flag was set from configuration file.
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertEqual(
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
        repl_methods = ['REPLICATE', 'SSYNC']
        for method_name in obj_methods:
            method = getattr(self.object_controller, method_name)
            self.assertFalse(hasattr(method, 'replication'))
        for method_name in repl_methods:
            method = getattr(self.object_controller, method_name)
            self.assertEqual(method.replication, True)

    def test_correct_allowed_method(self):
        # Test correct work for allowed method using
        # swift.obj.server.ObjectController.__call__
        inbuf = WsgiBytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.app_factory(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            # Sends args to outbuf
            outbuf.writelines(args)

        method = 'PUT'
        env = {'REQUEST_METHOD': method,
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
               'wsgi.run_once': False}

        method_res = mock.MagicMock()
        mock_method = public(lambda x:
                             mock.MagicMock(return_value=method_res))
        with mock.patch.object(self.object_controller, method,
                               new=mock_method):
            response = self.object_controller(env, start_response)
            self.assertEqual(response, method_res)

    def test_not_allowed_method(self):
        # Test correct work for NOT allowed method using
        # swift.obj.server.ObjectController.__call__
        inbuf = WsgiBytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'}, logger=self.logger)

        def start_response(*args):
            # Sends args to outbuf
            outbuf.writelines(args)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
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
               'wsgi.run_once': False}

        answer = ['<html><h1>Method Not Allowed</h1><p>The method is not '
                  'allowed for this resource.</p></html>']
        mock_method = replication(public(lambda x: mock.MagicMock()))
        with mock.patch.object(self.object_controller, method,
                               new=mock_method):
            mock_method.replication = True
            with mock.patch('time.gmtime',
                            mock.MagicMock(side_effect=[gmtime(10001.0)])):
                with mock.patch('time.time',
                                mock.MagicMock(side_effect=[10000.0,
                                                            10001.0])):
                    with mock.patch('os.getpid',
                                    mock.MagicMock(return_value=1234)):
                        response = self.object_controller.__call__(
                            env, start_response)
                        self.assertEqual(response, answer)
                        self.assertEqual(
                            self.logger.get_lines_for_level('info'),
                            ['None - - [01/Jan/1970:02:46:41 +0000] "PUT'
                             ' /sda1/p/a/c/o" 405 91 "-" "-" "-" 1.0000 "-"'
                             ' 1234 -'])

    def test_call_incorrect_replication_method(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'true'}, logger=FakeLogger())

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

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
            self.object_controller(env, start_response)
            self.assertEqual(errbuf.getvalue(), '')
            self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_not_utf8_and_not_logging_requests(self):
        inbuf = WsgiBytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false', 'log_requests': 'false'},
            logger=FakeLogger())

        def start_response(*args):
            # Sends args to outbuf
            outbuf.writelines(args)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
               'SCRIPT_NAME': '',
               'PATH_INFO': '/sda1/p/a/c/\x00%20/%',
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

        answer = ['Invalid UTF8 or contains NULL']
        mock_method = public(lambda x: mock.MagicMock())
        with mock.patch.object(self.object_controller, method,
                               new=mock_method):
            response = self.object_controller.__call__(env, start_response)
            self.assertEqual(response, answer)
            self.assertEqual(self.logger.get_lines_for_level('info'), [])

    def test__call__returns_500(self):
        inbuf = WsgiBytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.logger = debug_logger('test')
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false', 'log_requests': 'false'},
            logger=self.logger)

        def start_response(*args):
            # Sends args to outbuf
            outbuf.writelines(args)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
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
               'wsgi.run_once': False}

        @public
        def mock_put_method(*args, **kwargs):
            raise Exception()

        with mock.patch.object(self.object_controller, method,
                               new=mock_put_method):
            response = self.object_controller.__call__(env, start_response)
            self.assertTrue(response[0].startswith(
                'Traceback (most recent call last):'))
            self.assertEqual(self.logger.get_lines_for_level('error'), [
                'ERROR __call__ error with %(method)s %(path)s : ' % {
                    'method': 'PUT', 'path': '/sda1/p/a/c/o'},
            ])
            self.assertEqual(self.logger.get_lines_for_level('info'), [])

    def test_PUT_slow(self):
        inbuf = WsgiBytesIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.object_controller = object_server.ObjectController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false', 'log_requests': 'false',
             'slow': '10'},
            logger=self.logger)

        def start_response(*args):
            # Sends args to outbuf
            outbuf.writelines(args)

        method = 'PUT'

        env = {'REQUEST_METHOD': method,
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
               'wsgi.run_once': False}

        mock_method = public(lambda x: mock.MagicMock())
        with mock.patch.object(self.object_controller, method,
                               new=mock_method):
            with mock.patch('time.time',
                            mock.MagicMock(side_effect=[10000.0,
                                                        10001.0])):
                with mock.patch('swift.obj.server.sleep',
                                mock.MagicMock()) as ms:
                    self.object_controller.__call__(env, start_response)
                    ms.assert_called_with(9)
                    self.assertEqual(self.logger.get_lines_for_level('info'),
                                     [])

    def test_log_line_format(self):
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD', 'REMOTE_ADDR': '1.2.3.4'})
        self.object_controller.logger = self.logger
        with mock.patch(
                'time.gmtime', mock.MagicMock(side_effect=[gmtime(10001.0)])):
            with mock.patch(
                    'time.time',
                    mock.MagicMock(side_effect=[10000.0, 10001.0, 10002.0])):
                with mock.patch(
                        'os.getpid', mock.MagicMock(return_value=1234)):
                    req.get_response(self.object_controller)
        self.assertEqual(
            self.logger.get_lines_for_level('info'),
            ['1.2.3.4 - - [01/Jan/1970:02:46:41 +0000] "HEAD /sda1/p/a/c/o" '
             '404 - "-" "-" "-" 2.0000 "-" 1234 -'])

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one', False)])
    def test_dynamic_datadir(self):
        # update router post patch
        self.object_controller._diskfile_router = diskfile.DiskFileRouter(
            self.conf, self.object_controller.logger)
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'Foo': 'fooheader',
                                     'Baz': 'bazheader',
                                     'X-Backend-Storage-Policy-Index': 1,
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        object_dir = self.testdir + "/sda1/objects-1"
        self.assertFalse(os.path.isdir(object_dir))
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertTrue(os.path.isdir(object_dir))

        # make sure no idx in header uses policy 0 data_dir
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test',
                                     'Foo': 'fooheader',
                                     'Baz': 'bazheader',
                                     'X-Object-Meta-1': 'One',
                                     'X-Object-Meta-Two': 'Two'})
        req.body = 'VERIFY'
        object_dir = self.testdir + "/sda1/objects"
        self.assertFalse(os.path.isdir(object_dir))
        with mock.patch.object(POLICIES, 'get_by_index',
                               lambda _: True):
            resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 201)
        self.assertTrue(os.path.isdir(object_dir))

    def test_storage_policy_index_is_validated(self):
        # sanity check that index for existing policy is ok
        methods = ('PUT', 'POST', 'GET', 'HEAD', 'REPLICATE', 'DELETE')
        valid_indices = sorted([int(policy) for policy in POLICIES])
        for index in valid_indices:
            object_dir = self.testdir + "/sda1/objects"
            if index > 0:
                object_dir = "%s-%s" % (object_dir, index)
            self.assertFalse(os.path.isdir(object_dir))
            for method in methods:
                headers = {
                    'X-Timestamp': next(self.ts).internal,
                    'Content-Type': 'application/x-test',
                    'X-Backend-Storage-Policy-Index': index}
                if POLICIES[index].policy_type == EC_POLICY:
                    headers['X-Object-Sysmeta-Ec-Frag-Index'] = '2'
                req = Request.blank(
                    '/sda1/p/a/c/o',
                    environ={'REQUEST_METHOD': method},
                    headers=headers)
                req.body = 'VERIFY'
                resp = req.get_response(self.object_controller)
                self.assertTrue(is_success(resp.status_int),
                                '%s method failed: %r' % (method, resp.status))

        # index for non-existent policy should return 503
        index = valid_indices[-1] + 1
        for method in methods:
            req = Request.blank('/sda1/p/a/c/o',
                                environ={'REQUEST_METHOD': method},
                                headers={
                                    'X-Timestamp': next(self.ts).internal,
                                    'Content-Type': 'application/x-test',
                                    'X-Backend-Storage-Policy-Index': index})
            req.body = 'VERIFY'
            object_dir = self.testdir + "/sda1/objects-%s" % index
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 503)
            self.assertFalse(os.path.isdir(object_dir))

    def test_race_doesnt_quarantine(self):
        existing_timestamp = normalize_timestamp(time())
        delete_timestamp = normalize_timestamp(time() + 1)
        put_timestamp = normalize_timestamp(time() + 2)

        # make a .ts
        req = Request.blank(
            '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': existing_timestamp})
        req.get_response(self.object_controller)

        # force a PUT between the listdir and read_metadata of a DELETE
        put_once = [False]
        orig_listdir = os.listdir

        def mock_listdir(path):
            listing = orig_listdir(path)
            if not put_once[0]:
                put_once[0] = True
                req = Request.blank(
                    '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Timestamp': put_timestamp,
                             'Content-Length': '9',
                             'Content-Type': 'application/octet-stream'})
                req.body = 'some data'
                resp = req.get_response(self.object_controller)
                self.assertEqual(resp.status_int, 201)
            return listing

        with mock.patch('os.listdir', mock_listdir):
            req = Request.blank(
                '/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': delete_timestamp})
            resp = req.get_response(self.object_controller)
            self.assertEqual(resp.status_int, 404)

        qdir = os.path.join(self.testdir, 'sda1', 'quarantined')
        self.assertFalse(os.path.exists(qdir))

        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.object_controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['X-Timestamp'], put_timestamp)

    def test_multiphase_put_draining(self):
        # We want to ensure that we read the whole response body even if
        # it's multipart MIME and there's document parts that we don't
        # expect or understand. This'll help save our bacon if we ever jam
        # more stuff in there.
        in_a_timeout = [False]

        # inherit from BaseException so we get a stack trace when the test
        # fails instead of just a 500
        class NotInATimeout(BaseException):
            pass

        class FakeTimeout(BaseException):
            def __enter__(self):
                in_a_timeout[0] = True

            def __exit__(self, typ, value, tb):
                in_a_timeout[0] = False

        class PickyWsgiBytesIO(WsgiBytesIO):
            def read(self, *a, **kw):
                if not in_a_timeout[0]:
                    raise NotInATimeout()
                return WsgiBytesIO.read(self, *a, **kw)

            def readline(self, *a, **kw):
                if not in_a_timeout[0]:
                    raise NotInATimeout()
                return WsgiBytesIO.readline(self, *a, **kw)

        test_data = 'obj data'
        footer_meta = {
            "X-Object-Sysmeta-Ec-Frag-Index": "7",
            "Etag": md5(test_data).hexdigest(),
        }
        footer_json = json.dumps(footer_meta)
        footer_meta_cksum = md5(footer_json).hexdigest()
        test_doc = "\r\n".join((
            "--boundary123",
            "X-Document: object body",
            "",
            test_data,
            "--boundary123",
            "X-Document: object metadata",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_json,
            "--boundary123",
            "X-Document: we got cleverer",
            "",
            "stuff stuff meaningless stuuuuuuuuuuff",
            "--boundary123",
            "X-Document: we got even cleverer; can you believe it?",
            "Waneshaft: ambifacient lunar",
            "Casing: malleable logarithmic",
            "",
            "potato potato potato potato potato potato potato",
            "--boundary123--"
        ))
        if six.PY3:
            test_doc = test_doc.encode('utf-8')

        # phase1 - PUT request with object metadata in footer and
        # multiphase commit conversation
        put_timestamp = utils.Timestamp.now().internal
        headers = {
            'Content-Type': 'text/plain',
            'X-Timestamp': put_timestamp,
            'Transfer-Encoding': 'chunked',
            'Expect': '100-continue',
            'X-Backend-Storage-Policy-Index': '1',
            'X-Backend-Obj-Content-Length': len(test_data),
            'X-Backend-Obj-Metadata-Footer': 'yes',
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary123',
        }
        wsgi_input = PickyWsgiBytesIO(test_doc)
        req = Request.blank(
            "/sda1/0/a/c/o",
            environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': wsgi_input},
            headers=headers)

        app = object_server.ObjectController(self.conf, logger=self.logger)
        with mock.patch('swift.obj.server.ChunkReadTimeout', FakeTimeout):
            resp = req.get_response(app)
        self.assertEqual(resp.status_int, 201)  # sanity check

        in_a_timeout[0] = True  # so we can check without an exception
        self.assertEqual(wsgi_input.read(), '')  # we read all the bytes


@patch_policies(test_policies)
class TestObjectServer(unittest.TestCase):

    def setUp(self):
        skip_if_no_xattrs()
        # dirs
        self.tmpdir = mkdtemp()
        self.tempdir = os.path.join(self.tmpdir, 'tmp_test_obj_server')

        self.devices = os.path.join(self.tempdir, 'srv/node')
        for device in ('sda1', 'sdb1'):
            os.makedirs(os.path.join(self.devices, device))

        self.conf = {
            'devices': self.devices,
            'swift_dir': self.tempdir,
            'mount_check': 'false',
        }
        self.logger = debug_logger('test-object-server')
        self.app = object_server.ObjectController(
            self.conf, logger=self.logger)
        sock = listen_zero()
        self.server = spawn(wsgi.server, sock, self.app, utils.NullLogger())
        self.port = sock.getsockname()[1]

    def tearDown(self):
        rmtree(self.tmpdir)

    def test_not_found(self):
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'GET', '/a/c/o')
        resp = conn.getresponse()
        self.assertEqual(resp.status, 404)
        resp.read()
        resp.close()

    def test_expect_on_put(self):
        test_body = 'test'
        headers = {
            'Expect': '100-continue',
            'Content-Length': len(test_body),
            'Content-Type': 'application/test',
            'X-Timestamp': utils.Timestamp.now().internal,
        }
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'PUT', '/a/c/o', headers=headers)
        resp = conn.getexpect()
        self.assertEqual(resp.status, 100)
        conn.send(test_body)
        resp = conn.getresponse()
        self.assertEqual(resp.status, 201)
        resp.read()
        resp.close()

    def test_expect_on_put_footer(self):
        test_body = 'test'
        headers = {
            'Expect': '100-continue',
            'Content-Length': len(test_body),
            'Content-Type': 'application/test',
            'X-Timestamp': utils.Timestamp.now().internal,
            'X-Backend-Obj-Metadata-Footer': 'yes',
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary123',
        }
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'PUT', '/a/c/o', headers=headers)
        resp = conn.getexpect()
        self.assertEqual(resp.status, 100)
        headers = HeaderKeyDict(resp.getheaders())
        self.assertEqual(headers['X-Obj-Metadata-Footer'], 'yes')
        resp.close()

    def test_expect_on_put_conflict(self):
        test_body = 'test'
        put_timestamp = utils.Timestamp.now()
        headers = {
            'Expect': '100-continue',
            'Content-Length': len(test_body),
            'Content-Type': 'application/test',
            'X-Timestamp': put_timestamp.internal,
        }
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'PUT', '/a/c/o', headers=headers)
        resp = conn.getexpect()
        self.assertEqual(resp.status, 100)
        conn.send(test_body)
        resp = conn.getresponse()
        self.assertEqual(resp.status, 201)
        resp.read()
        resp.close()

        # and again with same timestamp
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'PUT', '/a/c/o', headers=headers)
        resp = conn.getexpect()
        self.assertEqual(resp.status, 409)
        headers = HeaderKeyDict(resp.getheaders())
        self.assertEqual(headers['X-Backend-Timestamp'], put_timestamp)
        resp.read()
        resp.close()

    def test_multiphase_put_no_mime_boundary(self):
        test_data = 'obj data'
        put_timestamp = utils.Timestamp.now().internal
        headers = {
            'Content-Type': 'text/plain',
            'X-Timestamp': put_timestamp,
            'Transfer-Encoding': 'chunked',
            'Expect': '100-continue',
            'X-Backend-Obj-Content-Length': len(test_data),
            'X-Backend-Obj-Multiphase-Commit': 'yes',
        }
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'PUT', '/a/c/o', headers=headers)
        resp = conn.getexpect()
        self.assertEqual(resp.status, 400)
        resp.read()
        resp.close()

    def test_expect_on_multiphase_put_diconnect(self):
        put_timestamp = utils.Timestamp.now().internal
        headers = {
            'Content-Type': 'text/plain',
            'X-Timestamp': put_timestamp,
            'Transfer-Encoding': 'chunked',
            'Expect': '100-continue',
            'X-Backend-Obj-Content-Length': 0,
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary123',
            'X-Backend-Obj-Multiphase-Commit': 'yes',
        }
        conn = bufferedhttp.http_connect('127.0.0.1', self.port, 'sda1', '0',
                                         'PUT', '/a/c/o', headers=headers)
        resp = conn.getexpect()
        self.assertEqual(resp.status, 100)
        headers = HeaderKeyDict(resp.getheaders())
        self.assertEqual(headers['X-Obj-Multiphase-Commit'], 'yes')

        conn.send('c\r\n--boundary123\r\n')

        # disconnect client
        conn.sock.fd._sock.close()
        for i in range(2):
            sleep(0)
        self.assertFalse(self.logger.get_lines_for_level('error'))
        for line in self.logger.get_lines_for_level('info'):
            self.assertIn(' 499 ', line)

    def find_files(self):
        ignore_files = {'.lock', 'hashes.invalid'}
        found_files = defaultdict(list)
        for root, dirs, files in os.walk(self.devices):
            for filename in files:
                if filename in ignore_files:
                    continue
                _name, ext = os.path.splitext(filename)
                file_path = os.path.join(root, filename)
                found_files[ext].append(file_path)
        return found_files

    @contextmanager
    def _check_multiphase_put_commit_handling(self,
                                              test_doc=None,
                                              headers=None,
                                              finish_body=True):
        """
        This helper will setup a multiphase chunked PUT request and yield at
        the context at the commit phase (after getting the second expect-100
        continue response.

        It can setup a reasonable stub request, but you can over-ride some
        characteristics of the request via kwargs.

        :param test_doc: first part of the mime conversation before the object
                         server will send the 100-continue, this includes the
                         object body
        :param headers: headers to send along with the initial request; some
                        object-metadata (e.g.  X-Backend-Obj-Content-Length)
                        is generally expected to match the test_doc)
        :param finish_body: boolean, if true send "0\r\n\r\n" after test_doc
                            and wait for 100-continue before yielding context
        """
        test_data = encode_frag_archive_bodies(POLICIES[1], 'obj data')[0]
        footer_meta = {
            "X-Object-Sysmeta-Ec-Frag-Index": "2",
            "Etag": md5(test_data).hexdigest(),
        }
        footer_json = json.dumps(footer_meta)
        footer_meta_cksum = md5(footer_json).hexdigest()
        test_doc = test_doc or "\r\n".join((
            "--boundary123",
            "X-Document: object body",
            "",
            test_data,
            "--boundary123",
            "X-Document: object metadata",
            "Content-MD5: " + footer_meta_cksum,
            "",
            footer_json,
            "--boundary123",
        ))

        # phase1 - PUT request with object metadata in footer and
        # multiphase commit conversation
        headers = headers or {
            'Content-Type': 'text/plain',
            'Transfer-Encoding': 'chunked',
            'Expect': '100-continue',
            'X-Backend-Storage-Policy-Index': '1',
            'X-Backend-Obj-Content-Length': len(test_data),
            'X-Backend-Obj-Metadata-Footer': 'yes',
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary123',
            'X-Backend-Obj-Multiphase-Commit': 'yes',
        }
        put_timestamp = utils.Timestamp(headers.setdefault(
            'X-Timestamp', utils.Timestamp.now().internal))
        container_update = \
            'swift.obj.server.ObjectController.container_update'
        with mock.patch(container_update) as _container_update:
            conn = bufferedhttp.http_connect(
                '127.0.0.1', self.port, 'sda1', '0',
                'PUT', '/a/c/o', headers=headers)
            resp = conn.getexpect()
            self.assertEqual(resp.status, 100)
            expect_headers = HeaderKeyDict(resp.getheaders())

            to_send = "%x\r\n%s\r\n" % (len(test_doc), test_doc)
            conn.send(to_send)
            if finish_body:
                conn.send("0\r\n\r\n")
                # verify 100-continue response to mark end of phase1
                resp = conn.getexpect()
                self.assertEqual(resp.status, 100)

            # yield relevant context for test
            yield {
                'conn': conn,
                'expect_headers': expect_headers,
                'put_timestamp': put_timestamp,
                'mock_container_update': _container_update,
            }

        # give the object server a little time to trampoline enough to
        # recognize request has finished, or socket has closed or whatever
        sleep(0.1)

    def test_multiphase_put_client_disconnect_right_before_commit(self):
        with self._check_multiphase_put_commit_handling() as context:
            conn = context['conn']
            # just bail stright out
            conn.sock.fd._sock.close()

        put_timestamp = context['put_timestamp']
        _container_update = context['mock_container_update']

        # and make sure it demonstrates the client disconnect
        log_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(len(log_lines), 1)
        self.assertIn(' 499 ', log_lines[0])

        # verify successful object data file write
        found_files = self.find_files()
        # non durable .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And no container update
        self.assertFalse(_container_update.called)

    def test_multiphase_put_client_disconnect_in_the_middle_of_commit(self):
        with self._check_multiphase_put_commit_handling() as context:
            conn = context['conn']
            # start commit confirmation to start phase2
            commit_confirmation_doc = "\r\n".join((
                "X-Document: put commit",
                "",
                "commit_confirmation",
                "--boundary123--",
            ))
            # but don't quite the commit body
            to_send = "%x\r\n%s" % \
                (len(commit_confirmation_doc), commit_confirmation_doc[:-1])
            conn.send(to_send)

            # and then bail out
            conn.sock.fd._sock.close()

        put_timestamp = context['put_timestamp']
        _container_update = context['mock_container_update']

        # and make sure it demonstrates the client disconnect
        log_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(len(log_lines), 1)
        self.assertIn(' 499 ', log_lines[0])

        # verify successful object data file write
        found_files = self.find_files()
        # non durable .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And no container update
        self.assertFalse(_container_update.called)

    def test_multiphase_put_no_metadata_replicated(self):
        test_data = 'obj data'
        test_doc = "\r\n".join((
            "--boundary123",
            "X-Document: object body",
            "",
            test_data,
            "--boundary123",
        ))

        put_timestamp = utils.Timestamp.now().internal
        headers = {
            'Content-Type': 'text/plain',
            'X-Timestamp': put_timestamp,
            'Transfer-Encoding': 'chunked',
            'Expect': '100-continue',
            'X-Backend-Obj-Content-Length': len(test_data),
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary123',
            'X-Backend-Obj-Multiphase-Commit': 'yes',
        }
        with self._check_multiphase_put_commit_handling(
                test_doc=test_doc, headers=headers) as context:
            expect_headers = context['expect_headers']
            self.assertEqual(expect_headers['X-Obj-Multiphase-Commit'], 'yes')
            # N.B. no X-Obj-Metadata-Footer header
            self.assertNotIn('X-Obj-Metadata-Footer', expect_headers)

            conn = context['conn']
            # send commit confirmation to start phase2
            commit_confirmation_doc = "\r\n".join((
                "X-Document: put commit",
                "",
                "commit_confirmation",
                "--boundary123--",
            ))
            to_send = "%x\r\n%s\r\n0\r\n\r\n" % \
                (len(commit_confirmation_doc), commit_confirmation_doc)
            conn.send(to_send)

            # verify success (2xx) to make end of phase2
            resp = conn.getresponse()
            self.assertEqual(resp.status, 201)
            resp.read()
            resp.close()

        # verify successful object data file write
        put_timestamp = context['put_timestamp']
        found_files = self.find_files()
        # .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And container update was called
        self.assertTrue(context['mock_container_update'].called)

    def test_multiphase_put_metadata_footer(self):
        with self._check_multiphase_put_commit_handling() as context:
            expect_headers = context['expect_headers']
            self.assertEqual(expect_headers['X-Obj-Multiphase-Commit'], 'yes')
            self.assertEqual(expect_headers['X-Obj-Metadata-Footer'], 'yes')

            conn = context['conn']
            # send commit confirmation to start phase2
            commit_confirmation_doc = "\r\n".join((
                "X-Document: put commit",
                "",
                "commit_confirmation",
                "--boundary123--",
            ))
            to_send = "%x\r\n%s\r\n0\r\n\r\n" % \
                (len(commit_confirmation_doc), commit_confirmation_doc)
            conn.send(to_send)

            # verify success (2xx) to make end of phase2
            resp = conn.getresponse()
            self.assertEqual(resp.status, 201)
            resp.read()
            resp.close()

        # verify successful object data and durable state file write
        put_timestamp = context['put_timestamp']
        found_files = self.find_files()
        # .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2#d.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))

        with open(obj_datafile) as fd:
            actual_meta = diskfile.read_metadata(fd)
        expected_meta = {'Content-Length': '82',
                         'name': '/a/c/o',
                         'X-Object-Sysmeta-Ec-Frag-Index': '2',
                         'X-Timestamp': put_timestamp.normal,
                         'Content-Type': 'text/plain'}
        for k, v in actual_meta.items():
            self.assertIsInstance(k, six.binary_type)
            self.assertIsInstance(v, six.binary_type)
        self.assertIsNotNone(actual_meta.pop('ETag', None))
        self.assertEqual(expected_meta, actual_meta)
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And container update was called
        self.assertTrue(context['mock_container_update'].called)

    def test_multiphase_put_metadata_footer_disconnect(self):
        test_data = 'obj data'
        test_doc = "\r\n".join((
            "--boundary123",
            "X-Document: object body",
            "",
            test_data,
            "--boundary123",
        ))
        # eventlet.wsgi won't return < network_chunk_size from a chunked read
        self.app.network_chunk_size = 16
        with self._check_multiphase_put_commit_handling(
                test_doc=test_doc, finish_body=False) as context:
            conn = context['conn']

            # make footer doc
            footer_meta = {
                "X-Object-Sysmeta-Ec-Frag-Index": "2",
                "Etag": md5(test_data).hexdigest(),
            }
            footer_json = json.dumps(footer_meta)
            footer_meta_cksum = md5(footer_json).hexdigest()

            # send most of the footer doc
            footer_doc = "\r\n".join((
                "X-Document: object metadata",
                "Content-MD5: " + footer_meta_cksum,
                "",
                footer_json,
            ))

            # but don't send final boundary nor last chunk
            to_send = "%x\r\n%s\r\n" % \
                (len(footer_doc), footer_doc)
            conn.send(to_send)

            # and then bail out
            conn.sock.fd._sock.close()

        # and make sure it demonstrates the client disconnect
        log_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(len(log_lines), 1)
        self.assertIn(' 499 ', log_lines[0])

        # no artifacts left on disk
        found_files = self.find_files()
        self.assertFalse(found_files)
        # ... and no container update
        _container_update = context['mock_container_update']
        self.assertFalse(_container_update.called)

    def test_multiphase_put_ec_fragment_in_headers_no_footers(self):
        test_data = 'obj data'
        test_doc = "\r\n".join((
            "--boundary123",
            "X-Document: object body",
            "",
            test_data,
            "--boundary123",
        ))

        # phase1 - PUT request with multiphase commit conversation
        # no object metadata in footer
        put_timestamp = utils.Timestamp.now().internal
        headers = {
            'Content-Type': 'text/plain',
            'X-Timestamp': put_timestamp,
            'Transfer-Encoding': 'chunked',
            'Expect': '100-continue',
            # normally the frag index gets sent in the MIME footer (which this
            # test doesn't have, see `test_multiphase_put_metadata_footer`),
            # but the proxy *could* send the frag index in the headers and
            # this test verifies that would work.
            'X-Object-Sysmeta-Ec-Frag-Index': '2',
            'X-Backend-Storage-Policy-Index': '1',
            'X-Backend-Obj-Content-Length': len(test_data),
            'X-Backend-Obj-Multipart-Mime-Boundary': 'boundary123',
            'X-Backend-Obj-Multiphase-Commit': 'yes',
        }
        with self._check_multiphase_put_commit_handling(
                test_doc=test_doc, headers=headers) as context:
            expect_headers = context['expect_headers']
            self.assertEqual(expect_headers['X-Obj-Multiphase-Commit'], 'yes')
            # N.B. no X-Obj-Metadata-Footer header
            self.assertNotIn('X-Obj-Metadata-Footer', expect_headers)

            conn = context['conn']
            # send commit confirmation to start phase2
            commit_confirmation_doc = "\r\n".join((
                "X-Document: put commit",
                "",
                "commit_confirmation",
                "--boundary123--",
            ))
            to_send = "%x\r\n%s\r\n0\r\n\r\n" % \
                (len(commit_confirmation_doc), commit_confirmation_doc)
            conn.send(to_send)

            # verify success (2xx) to make end of phase2
            resp = conn.getresponse()
            self.assertEqual(resp.status, 201)
            resp.read()
            resp.close()

        # verify successful object data and durable state file write
        put_timestamp = context['put_timestamp']
        found_files = self.find_files()
        # .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2#d.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And container update was called
        self.assertTrue(context['mock_container_update'].called)

    def test_multiphase_put_bad_commit_message(self):
        with self._check_multiphase_put_commit_handling() as context:
            conn = context['conn']
            # send commit confirmation to start phase2
            commit_confirmation_doc = "\r\n".join((
                "junkjunk",
                "--boundary123--",
            ))
            to_send = "%x\r\n%s\r\n0\r\n\r\n" % \
                (len(commit_confirmation_doc), commit_confirmation_doc)
            conn.send(to_send)
            resp = conn.getresponse()
            self.assertEqual(resp.status, 500)
            resp.read()
            resp.close()
        put_timestamp = context['put_timestamp']
        _container_update = context['mock_container_update']
        # verify that durable data file was NOT created
        found_files = self.find_files()
        # non durable .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And no container update
        self.assertFalse(_container_update.called)

    def test_multiphase_put_drains_extra_commit_junk(self):
        with self._check_multiphase_put_commit_handling() as context:
            conn = context['conn']
            # send commit confirmation to start phase2
            commit_confirmation_doc = "\r\n".join((
                "X-Document: put commit",
                "",
                "commit_confirmation",
                "--boundary123",
                "X-Document: we got cleverer",
                "",
                "stuff stuff meaningless stuuuuuuuuuuff",
                "--boundary123",
                "X-Document: we got even cleverer; can you believe it?",
                "Waneshaft: ambifacient lunar",
                "Casing: malleable logarithmic",
                "",
                "potato potato potato potato potato potato potato",
                "--boundary123--",
            ))
            to_send = "%x\r\n%s\r\n0\r\n\r\n" % \
                (len(commit_confirmation_doc), commit_confirmation_doc)
            conn.send(to_send)

            # verify success (2xx) to make end of phase2
            resp = conn.getresponse()
            self.assertEqual(resp.status, 201)
            resp.read()

            # make another request to validate the HTTP protocol state
            conn.putrequest('GET', '/sda1/0/a/c/o')
            conn.putheader('X-Backend-Storage-Policy-Index', '1')
            conn.endheaders()
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            resp.read()

            resp.close()

        # verify successful object data and durable state file write
        put_timestamp = context['put_timestamp']
        found_files = self.find_files()
        # .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2#d.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # And container update was called
        self.assertTrue(context['mock_container_update'].called)

    def test_multiphase_put_drains_extra_commit_junk_disconnect(self):
        commit_confirmation_doc = "\r\n".join((
            "X-Document: put commit",
            "",
            "commit_confirmation",
            "--boundary123",
            "X-Document: we got cleverer",
            "",
            "stuff stuff meaningless stuuuuuuuuuuff",
            "--boundary123",
            "X-Document: we got even cleverer; can you believe it?",
            "Waneshaft: ambifacient lunar",
            "Casing: malleable logarithmic",
            "",
            "potato potato potato potato potato potato potato",
        ))
        # eventlet.wsgi won't return < network_chunk_size from a chunked read
        self.app.network_chunk_size = 16
        with self._check_multiphase_put_commit_handling() as context:
            conn = context['conn']
            # send commit confirmation and some other stuff
            # but don't send final boundary or last chunk
            to_send = "%x\r\n%s\r\n" % \
                (len(commit_confirmation_doc), commit_confirmation_doc)
            conn.send(to_send)

            # and then bail out
            conn.sock.fd._sock.close()

        # and make sure it demonstrates the client disconnect
        log_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(len(log_lines), 1)
        self.assertIn(' 499 ', log_lines[0])

        # verify successful object data and durable state file write
        put_timestamp = context['put_timestamp']
        found_files = self.find_files()
        # .data file is there
        self.assertEqual(len(found_files['.data']), 1)
        obj_datafile = found_files['.data'][0]
        self.assertEqual("%s#2#d.data" % put_timestamp.internal,
                         os.path.basename(obj_datafile))
        # but no other files
        self.assertFalse(found_files['.data'][1:])
        found_files.pop('.data')
        self.assertFalse(found_files)
        # but no container update
        self.assertFalse(context['mock_container_update'].called)


@patch_policies
class TestZeroCopy(unittest.TestCase):
    """Test the object server's zero-copy functionality"""

    def _system_can_zero_copy(self):
        if not splice.available:
            return False

        try:
            utils.get_md5_socket()
        except IOError:
            return False

        return True

    def setUp(self):
        skip_if_no_xattrs()
        if not self._system_can_zero_copy():
            raise unittest.SkipTest("zero-copy support is missing")

        self.testdir = mkdtemp(suffix="obj_server_zero_copy")
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))

        conf = {'devices': self.testdir,
                'mount_check': 'false',
                'splice': 'yes',
                'disk_chunk_size': '4096'}
        self.object_controller = object_server.ObjectController(
            conf, logger=debug_logger())
        self.df_mgr = diskfile.DiskFileManager(
            conf, self.object_controller.logger)

        listener = listen_zero()
        port = listener.getsockname()[1]
        self.wsgi_greenlet = spawn(
            wsgi.server, listener, self.object_controller, NullLogger())

        self.http_conn = httplib.HTTPConnection('127.0.0.1', port)
        self.http_conn.connect()

    def tearDown(self):
        """Tear down for testing swift.object.server.ObjectController"""
        self.wsgi_greenlet.kill()
        rmtree(self.testdir)

    def test_GET(self):
        url_path = '/sda1/2100/a/c/o'

        self.http_conn.request('PUT', url_path, 'obj contents',
                               {'X-Timestamp': '127082564.24709',
                                'Content-Type': 'application/test'})
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 201)
        response.read()

        self.http_conn.request('GET', url_path)
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 200)
        contents = response.read()
        self.assertEqual(contents, 'obj contents')

    def test_GET_big(self):
        # Test with a large-ish object to make sure we handle full socket
        # buffers correctly.
        obj_contents = 'A' * 4 * 1024 * 1024  # 4 MiB
        url_path = '/sda1/2100/a/c/o'

        self.http_conn.request('PUT', url_path, obj_contents,
                               {'X-Timestamp': '1402600322.52126',
                                'Content-Type': 'application/test'})
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 201)
        response.read()

        self.http_conn.request('GET', url_path)
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 200)
        contents = response.read()
        self.assertEqual(contents, obj_contents)

    def test_quarantine(self):
        obj_hash = hash_path('a', 'c', 'o')
        url_path = '/sda1/2100/a/c/o'
        ts = '1402601849.47475'

        self.http_conn.request('PUT', url_path, 'obj contents',
                               {'X-Timestamp': ts,
                                'Content-Type': 'application/test'})
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 201)
        response.read()

        # go goof up the file on disk
        fname = os.path.join(self.testdir, 'sda1', 'objects', '2100',
                             obj_hash[-3:], obj_hash, ts + '.data')

        with open(fname, 'rb+') as fh:
            fh.write('XYZ')

        self.http_conn.request('GET', url_path)
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 200)
        contents = response.read()
        self.assertEqual(contents, 'XYZ contents')

        self.http_conn.request('GET', url_path)
        response = self.http_conn.getresponse()
        # it was quarantined by the previous request
        self.assertEqual(response.status, 404)
        response.read()

    def test_quarantine_on_well_formed_zero_byte_file(self):
        # Make sure we work around an oddity in Linux's hash sockets
        url_path = '/sda1/2100/a/c/o'
        ts = '1402700497.71333'

        self.http_conn.request(
            'PUT', url_path, '',
            {'X-Timestamp': ts, 'Content-Length': '0',
             'Content-Type': 'application/test'})
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 201)
        response.read()

        self.http_conn.request('GET', url_path)
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 200)
        contents = response.read()
        self.assertEqual(contents, '')

        self.http_conn.request('GET', url_path)
        response = self.http_conn.getresponse()
        self.assertEqual(response.status, 200)  # still there
        contents = response.read()
        self.assertEqual(contents, '')


class TestConfigOptionHandling(unittest.TestCase):

    def setUp(self):
        self.tmpdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tmpdir)

    def _app_config(self, config):
        contents = dedent(config)
        conf_file = os.path.join(self.tmpdir, 'object-server.conf')
        with open(conf_file, 'w') as f:
            f.write(contents)
        return init_request_processor(conf_file, 'object-server')[:2]

    def test_default(self):
        config = """
        [DEFAULT]

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object
        """
        app, config = self._app_config(config)
        self.assertNotIn('reclaim_age', config)
        for policy in POLICIES:
            self.assertEqual(app._diskfile_router[policy].reclaim_age, 604800)

    def test_option_in_app(self):
        config = """
        [DEFAULT]

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object
        reclaim_age = 100
        """
        app, config = self._app_config(config)
        self.assertEqual(config['reclaim_age'], '100')
        for policy in POLICIES:
            self.assertEqual(app._diskfile_router[policy].reclaim_age, 100)

    def test_option_in_default(self):
        config = """
        [DEFAULT]
        reclaim_age = 200

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object
        """
        app, config = self._app_config(config)
        self.assertEqual(config['reclaim_age'], '200')
        for policy in POLICIES:
            self.assertEqual(app._diskfile_router[policy].reclaim_age, 200)

    def test_option_in_both(self):
        config = """
        [DEFAULT]
        reclaim_age = 300

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object
        reclaim_age = 400
        """
        app, config = self._app_config(config)
        self.assertEqual(config['reclaim_age'], '300')
        for policy in POLICIES:
            self.assertEqual(app._diskfile_router[policy].reclaim_age, 300)

        # use paste "set" syntax to override global config value
        config = """
        [DEFAULT]
        reclaim_age = 500

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object
        set reclaim_age = 600
        """
        app, config = self._app_config(config)
        self.assertEqual(config['reclaim_age'], '600')
        for policy in POLICIES:
            self.assertEqual(app._diskfile_router[policy].reclaim_age, 600)


if __name__ == '__main__':
    unittest.main()
