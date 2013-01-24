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

""" Tests for swift.object_server """

import cPickle as pickle
import operator
import os
import unittest
import email
from shutil import rmtree
from StringIO import StringIO
from time import gmtime, sleep, strftime, time
from tempfile import mkdtemp
from hashlib import md5

from eventlet import sleep, spawn, wsgi, listen, Timeout
from test.unit import FakeLogger
from test.unit import _getxattr as getxattr
from test.unit import _setxattr as setxattr
from test.unit import connect_tcp, readuntil2crlfs
from swift.obj import server as object_server, replicator
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, \
                               NullLogger, storage_directory
from swift.common.exceptions import DiskFileNotExist
from swift.common import constraints
from eventlet import tpool
from swift.common.swob import Request


class TestDiskFile(unittest.TestCase):
    """Test swift.obj.server.DiskFile"""

    def setUp(self):
        """ Set up for testing swift.object_server.ObjectController """
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_obj_server_DiskFile')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))

        def fake_exe(*args, **kwargs):
            pass
        tpool.execute = fake_exe

    def tearDown(self):
        """ Tear down for testing swift.object_server.ObjectController """
        rmtree(os.path.dirname(self.testdir))

    def _create_test_file(self, data, keep_data_fp=True):
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        f.write(data)
        setxattr(f.fileno(), object_server.METADATA_KEY,
                 pickle.dumps({}, object_server.PICKLE_PROTOCOL))
        f.close()
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=keep_data_fp)
        return df

    def test_disk_file_app_iter_corners(self):
        df = self._create_test_file('1234567890')
        self.assertEquals(''.join(df.app_iter_range(0, None)), '1234567890')

        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=True)
        self.assertEqual(''.join(df.app_iter_range(5, None)), '67890')

    def test_disk_file_app_iter_ranges(self):
        df = self._create_test_file('012345678911234567892123456789')
        it = df.app_iter_ranges([(0, 10), (10, 20), (20, 30)], 'plain/text',
                                '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assert_('0123456789' in value)
        self.assert_('1123456789' in value)
        self.assert_('2123456789' in value)

    def test_disk_file_app_iter_ranges_edges(self):
        df = self._create_test_file('012345678911234567892123456789')
        it = df.app_iter_ranges([(3, 10), (0, 2)], 'application/whatever',
                                '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assert_('3456789' in value)
        self.assert_('01' in value)

    def test_disk_file_large_app_iter_ranges(self):
        """
        This test case is to make sure that the disk file app_iter_ranges
        method all the paths being tested.
        """
        long_str = '01234567890' * 65536
        target_strs = ['3456789', long_str[0:65590]]
        df = self._create_test_file(long_str)

        it = df.app_iter_ranges([(3, 10), (0, 65590)], 'plain/text',
                                '5e816ff8b8b8e9a5d355497e5d9e0301', 655360)

        """
        the produced string actually missing the MIME headers
        need to add these headers to make it as real MIME message.
        The body of the message is produced by method app_iter_ranges
        off of DiskFile object.
        """
        header = ''.join(['Content-Type: multipart/byteranges;',
                          'boundary=',
                          '5e816ff8b8b8e9a5d355497e5d9e0301\r\n'])

        value = header + ''.join(it)

        parts = map(lambda p: p.get_payload(decode=True),
                    email.message_from_string(value).walk())[1:3]
        self.assertEqual(parts, target_strs)

    def test_disk_file_app_iter_ranges_empty(self):
        """
        This test case tests when empty value passed into app_iter_ranges
        When ranges passed into the method is either empty array or None,
        this method will yield empty string
        """
        df = self._create_test_file('012345678911234567892123456789')
        it = df.app_iter_ranges([], 'application/whatever',
                                '\r\n--someheader\r\n', 100)
        self.assertEqual(''.join(it), '')

        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=True)
        it = df.app_iter_ranges(None, 'app/something',
                                '\r\n--someheader\r\n', 150)
        self.assertEqual(''.join(it), '')

    def test_disk_file_mkstemp_creates_dir(self):
        tmpdir = os.path.join(self.testdir, 'sda1', 'tmp')
        os.rmdir(tmpdir)
        with object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    'o', FakeLogger()).mkstemp():
            self.assert_(os.path.exists(tmpdir))

    def test_iter_hook(self):
        hook_call_count = [0]
        def hook():
            hook_call_count[0] += 1

        df = self._get_data_file(fsize=65, csize=8, iter_hook=hook)
        print repr(df.__dict__)
        for _ in df:
            pass

        self.assertEquals(hook_call_count[0], 9)

    def test_quarantine(self):
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), object_server.METADATA_KEY,
                 pickle.dumps({}, object_server.PICKLE_PROTOCOL))
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        df.quarantine()
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined',
                                'objects', os.path.basename(os.path.dirname(
                                                            df.data_file)))
        self.assert_(os.path.isdir(quar_dir))

    def test_quarantine_same_file(self):
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), object_server.METADATA_KEY,
                 pickle.dumps({}, object_server.PICKLE_PROTOCOL))
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        new_dir = df.quarantine()
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined',
                                'objects', os.path.basename(os.path.dirname(
                                                            df.data_file)))
        self.assert_(os.path.isdir(quar_dir))
        self.assertEquals(quar_dir, new_dir)
        # have to remake the datadir and file
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), object_server.METADATA_KEY,
                 pickle.dumps({}, object_server.PICKLE_PROTOCOL))

        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=True)
        double_uuid_path = df.quarantine()
        self.assert_(os.path.isdir(double_uuid_path))
        self.assert_('-' in os.path.basename(double_uuid_path))

    def _get_data_file(self, invalid_type=None, obj_name='o',
                       fsize=1024, csize=8, extension='.data', ts=None,
                       iter_hook=None):
        '''returns a DiskFile'''
        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    obj_name, FakeLogger())
        data = '0' * fsize
        etag = md5()
        if ts:
            timestamp = ts
        else:
            timestamp = str(normalize_timestamp(time()))
        with df.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            df.put(fd, metadata, extension=extension)
            if invalid_type == 'ETag':
                etag = md5()
                etag.update('1' + '0' * (fsize - 1))
                etag = etag.hexdigest()
                metadata['ETag'] = etag
                object_server.write_metadata(fd, metadata)
            if invalid_type == 'Content-Length':
                metadata['Content-Length'] = fsize - 1
                object_server.write_metadata(fd, metadata)

        df = object_server.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    obj_name, FakeLogger(),
                                    keep_data_fp=True, disk_chunk_size=csize,
                                    iter_hook=iter_hook)
        if invalid_type == 'Zero-Byte':
            os.remove(df.data_file)
            fp = open(df.data_file, 'w')
            fp.close()
        df.unit_test_len = fsize
        return df

    def test_quarantine_valids(self):
        df = self._get_data_file(obj_name='1')
        for chunk in df:
            pass
        self.assertFalse(df.quarantined_dir)

        df = self._get_data_file(obj_name='2', csize=1)
        for chunk in df:
            pass
        self.assertFalse(df.quarantined_dir)

        df = self._get_data_file(obj_name='3', csize=100000)
        for chunk in df:
            pass
        self.assertFalse(df.quarantined_dir)

    def run_quarantine_invalids(self, invalid_type):
        df = self._get_data_file(invalid_type=invalid_type, obj_name='1')
        for chunk in df:
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_data_file(invalid_type=invalid_type,
                                 obj_name='2', csize=1)
        for chunk in df:
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_data_file(invalid_type=invalid_type,
                                 obj_name='3', csize=100000)
        for chunk in df:
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_data_file(invalid_type=invalid_type, obj_name='4')
        self.assertFalse(df.quarantined_dir)
        df = self._get_data_file(invalid_type=invalid_type, obj_name='5')
        for chunk in df.app_iter_range(0, df.unit_test_len):
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_data_file(invalid_type=invalid_type, obj_name='6')
        for chunk in df.app_iter_range(0, df.unit_test_len + 100):
            pass
        self.assertTrue(df.quarantined_dir)
        expected_quar = False
        # for the following, Content-Length/Zero-Byte errors will always result
        # in a quarantine, even if the whole file isn't check-summed
        if invalid_type in ('Zero-Byte', 'Content-Length'):
            expected_quar = True
        df = self._get_data_file(invalid_type=invalid_type, obj_name='7')
        for chunk in df.app_iter_range(1, df.unit_test_len):
            pass
        self.assertEquals(bool(df.quarantined_dir), expected_quar)
        df = self._get_data_file(invalid_type=invalid_type, obj_name='8')
        for chunk in df.app_iter_range(0, df.unit_test_len - 1):
            pass
        self.assertEquals(bool(df.quarantined_dir), expected_quar)
        df = self._get_data_file(invalid_type=invalid_type, obj_name='8')
        for chunk in df.app_iter_range(1, df.unit_test_len + 1):
            pass
        self.assertEquals(bool(df.quarantined_dir), expected_quar)

    def test_quarantine_invalids(self):
        self.run_quarantine_invalids('ETag')
        self.run_quarantine_invalids('Content-Length')
        self.run_quarantine_invalids('Zero-Byte')

    def test_quarantine_deleted_files(self):
        df = self._get_data_file(invalid_type='Content-Length',
                                 extension='.data')
        df.close()
        self.assertTrue(df.quarantined_dir)
        df = self._get_data_file(invalid_type='Content-Length',
                                 extension='.ts')
        df.close()
        self.assertFalse(df.quarantined_dir)
        df = self._get_data_file(invalid_type='Content-Length',
                                 extension='.ts')
        self.assertRaises(DiskFileNotExist, df.get_data_file_size)

    def test_put_metadata(self):
        df = self._get_data_file()
        ts = time()
        metadata = { 'X-Timestamp': ts, 'X-Object-Meta-test': 'data' }
        df.put_metadata(metadata)
        exp_name = '%s.meta' % str(normalize_timestamp(ts))
        dl = os.listdir(df.datadir)
        self.assertEquals(len(dl), 2)
        self.assertTrue(exp_name in set(dl))

    def test_put_metadata_ts(self):
        df = self._get_data_file()
        ts = time()
        metadata = { 'X-Timestamp': ts, 'X-Object-Meta-test': 'data' }
        df.put_metadata(metadata, tombstone=True)
        exp_name = '%s.ts' % str(normalize_timestamp(ts))
        dl = os.listdir(df.datadir)
        self.assertEquals(len(dl), 2)
        self.assertTrue(exp_name in set(dl))

    def test_unlinkold(self):
        df1 = self._get_data_file()
        future_time = str(normalize_timestamp(time() + 100))
        df2 = self._get_data_file(ts=future_time)
        self.assertEquals(len(os.listdir(df1.datadir)), 2)
        df1.unlinkold(future_time)
        self.assertEquals(len(os.listdir(df1.datadir)), 1)
        self.assertEquals(os.listdir(df1.datadir)[0], "%s.data" % future_time)

    def test_close_error(self):

        def err():
            raise Exception("bad")

        df = self._get_data_file(fsize=1024 * 1024 * 2)
        df._handle_close_quarantine = err
        for chunk in df:
            pass
        # close is called at the end of the iterator
        self.assertEquals(df.fp, None)
        self.assertEquals(len(df.logger.log_dict['error']), 1)

    def test_quarantine_twice(self):
        df = self._get_data_file(invalid_type='Content-Length',
                                 extension='.data')
        self.assert_(os.path.isfile(df.data_file))
        quar_dir = df.quarantine()
        self.assertFalse(os.path.isfile(df.data_file))
        self.assert_(os.path.isdir(quar_dir))
        self.assertEquals(df.quarantine(), None)


class TestObjectController(unittest.TestCase):
    """ Test swift.obj.server.ObjectController """

    def setUp(self):
        """ Set up for testing swift.object_server.ObjectController """
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = \
            os.path.join(mkdtemp(), 'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.object_controller = object_server.ObjectController(conf)
        self.object_controller.bytes_per_sync = 1

    def tearDown(self):
        """ Tear down for testing swift.object_server.ObjectController """
        rmtree(os.path.dirname(self.testdir))

    def test_POST_update_meta(self):
        """ Test swift.object_server.ObjectController.POST """
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
        resp = self.object_controller.PUT(req)
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
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
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
        resp = self.object_controller.HEAD(req)
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
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
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
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
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
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assert_("X-Object-Meta-1" not in resp.headers and
                     "Foo" not in resp.headers and
                     "Content-Encoding" not in resp.headers and
                     "X-Object-Manifest" not in resp.headers and
                     "Content-Disposition" not in resp.headers and
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

    def test_POST_quarantine_zbyte(self):
        """ Test swift.object_server.ObjectController.GET """
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        file = object_server.DiskFile(self.testdir, 'sda1', 'p', 'a', 'c', 'o',
                                      FakeLogger(), keep_data_fp=True)

        file_name = os.path.basename(file.data_file)
        with open(file.data_file) as fp:
            metadata = object_server.read_metadata(fp)
        os.unlink(file.data_file)
        with open(file.data_file, 'w') as fp:
            object_server.write_metadata(fp, metadata)

        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o',
                        headers={'X-Timestamp': normalize_timestamp(time())})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 404)

        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                       os.path.basename(os.path.dirname(file.data_file)))
        self.assertEquals(os.listdir(quar_dir)[0], file_name)

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

    def test_HEAD_quarantine_zbyte(self):
        """ Test swift.object_server.ObjectController.GET """
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        file = object_server.DiskFile(self.testdir, 'sda1', 'p', 'a', 'c', 'o',
                                      FakeLogger(), keep_data_fp=True)

        file_name = os.path.basename(file.data_file)
        with open(file.data_file) as fp:
            metadata = object_server.read_metadata(fp)
        os.unlink(file.data_file)
        with open(file.data_file, 'w') as fp:
            object_server.write_metadata(fp, metadata)

        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)

        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                       os.path.basename(os.path.dirname(file.data_file)))
        self.assertEquals(os.listdir(quar_dir)[0], file_name)

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

        since = strftime('%a, %d %b %Y %H:%M:%S GMT', gmtime(float(timestamp) + 1))
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

    def test_GET_quarantine(self):
        """ Test swift.object_server.ObjectController.GET """
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        file = object_server.DiskFile(self.testdir, 'sda1', 'p', 'a', 'c', 'o',
                                      FakeLogger(), keep_data_fp=True)
        file_name = os.path.basename(file.data_file)
        etag = md5()
        etag.update('VERIF')
        etag = etag.hexdigest()
        metadata = {'X-Timestamp': timestamp,
                    'Content-Length': 6, 'ETag': etag}
        object_server.write_metadata(file.fp, metadata)
        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                       os.path.basename(os.path.dirname(file.data_file)))
        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        body = resp.body  # actually does quarantining
        self.assertEquals(body, 'VERIFY')
        self.assertEquals(os.listdir(quar_dir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

    def test_GET_quarantine_zbyte(self):
        """ Test swift.object_server.ObjectController.GET """
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        file = object_server.DiskFile(self.testdir, 'sda1', 'p', 'a', 'c', 'o',
                                      FakeLogger(), keep_data_fp=True)
        file_name = os.path.basename(file.data_file)
        with open(file.data_file) as fp:
            metadata = object_server.read_metadata(fp)
        os.unlink(file.data_file)
        with open(file.data_file, 'w') as fp:
            object_server.write_metadata(fp, metadata)

        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                       os.path.basename(os.path.dirname(file.data_file)))
        self.assertEquals(os.listdir(quar_dir)[0], file_name)

    def test_GET_quarantine_range(self):
        """ Test swift.object_server.ObjectController.GET """
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': timestamp,
                                     'Content-Type': 'application/x-test'})
        req.body = 'VERIFY'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        file = object_server.DiskFile(self.testdir, 'sda1', 'p', 'a', 'c', 'o',
                                      FakeLogger(), keep_data_fp=True)
        file_name = os.path.basename(file.data_file)
        etag = md5()
        etag.update('VERIF')
        etag = etag.hexdigest()
        metadata = {'X-Timestamp': timestamp,
                    'Content-Length': 6, 'ETag': etag}
        object_server.write_metadata(file.fp, metadata)
        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=0-4'  # partial
        resp = self.object_controller.GET(req)
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                            os.path.basename(os.path.dirname(file.data_file)))
        body = resp.body
        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        self.assertFalse(os.path.isdir(quar_dir))
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=1-6'  # partial
        resp = self.object_controller.GET(req)
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                            os.path.basename(os.path.dirname(file.data_file)))
        body = resp.body
        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        self.assertFalse(os.path.isdir(quar_dir))

        req = Request.blank('/sda1/p/a/c/o')
        req.range = 'bytes=0-14'  # full
        resp = self.object_controller.GET(req)
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined', 'objects',
                       os.path.basename(os.path.dirname(file.data_file)))
        self.assertEquals(os.listdir(file.datadir)[0], file_name)
        body = resp.body
        self.assertTrue(os.path.isdir(quar_dir))
        req = Request.blank('/sda1/p/a/c/o')
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 404)

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

    def test_invalid_method_doesnt_exist(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        def start_response(*args):
            outbuf.writelines(args)
        self.object_controller.__call__({'REQUEST_METHOD': 'method_doesnt_exist',
                                         'PATH_INFO': '/sda1/p/a/c/o'},
                                        start_response)
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        inbuf = StringIO()
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
        max_name_len = constraints.MAX_OBJECT_NAME_LENGTH
        req = Request.blank('/sda1/p/a/c/' + ('1' * max_name_len),
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/' + ('2' * (max_name_len + 1)),
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
        req.body = 'DATA'
        resp = self.object_controller.PUT(req)
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

    def test_manifest_head_request(self):
        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': timestamp,
                         'Content-Type': 'text/plain',
                         'Content-Length': '0',
                         'X-Object-Manifest': 'c/o/'})
        req.body = 'hi'
        resp = self.object_controller.PUT(req)
        objfile = os.path.join(self.testdir, 'sda1',
            storage_directory(object_server.DATADIR, 'p', hash_path('a', 'c',
            'o')), timestamp + '.data')
        self.assert_(os.path.isfile(objfile))
        req = Request.blank('/sda1/p/a/c/o',
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.body, '')

    def test_async_update_http_connect(self):
        given_args = []

        def fake_http_connect(*args):
            given_args.extend(args)
            raise Exception('test')

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.async_update('PUT', 'a', 'c', 'o',
                '127.0.0.1:1234', 1, 'sdc1',
                {'x-timestamp': '1', 'x-out': 'set'}, 'sda1')
        finally:
            object_server.http_connect = orig_http_connect
        self.assertEquals(given_args, ['127.0.0.1', '1234', 'sdc1', 1, 'PUT',
            '/a/c/o', {'x-timestamp': '1', 'x-out': 'set'}])


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
                dict((k,v) for k,v in captured_args.iteritems()
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
                     'X-Delete-At-Host': "10.1.1.1:6001,10.2.2.2:6002",
                     'X-Delete-At-Partition': '6237',
                     'X-Delete-At-Device': 'sdp,sdq'})

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            resp = self.object_controller.PUT(req)
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
             'headers': {'x-content-type': 'application/burrito',
                         'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                         'x-size': '0',
                         'x-timestamp': '12345',
                         'x-trans-id': '-'}})
        self.assertEquals(
            http_connect_args[1],
            {'ipaddr': '10.1.1.1',
             'port': '6001',
             'path': '/exp/9999999960/9999999999-a/c/o',
             'device': 'sdp',
             'partition': '6237',
             'method': 'PUT',
             'ssl': False,
             'headers': {'x-content-type': 'text/plain',
                         'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                         'x-size': '0',
                         'x-timestamp': '12345',
                         'x-trans-id': '-'}})
        self.assertEquals(
            http_connect_args[2],
            {'ipaddr': '10.2.2.2',
             'port': '6002',
             'path': '/exp/9999999960/9999999999-a/c/o',
             'device': 'sdq',
             'partition': '6237',
             'method': 'PUT',
             'ssl': False,
             'headers': {'x-content-type': 'text/plain',
                         'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                         'x-size': '0',
                         'x-timestamp': '12345',
                         'x-trans-id': '-'}})

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
                dict((k,v) for k,v in captured_args.iteritems()
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
             'headers': {'x-content-type': 'application/burrito',
                         'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                         'x-size': '0',
                         'x-timestamp': '12345',
                         'x-trans-id': '-'}})
        self.assertEquals(
            http_connect_args[1],
            {'ipaddr': '6.7.8.9',
             'port': '10',
             'path': '/a/c/o',
             'device': 'sdf1',
             'partition': '20',
             'method': 'PUT',
             'ssl': False,
             'headers': {'x-content-type': 'application/burrito',
                         'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
                         'x-size': '0',
                         'x-timestamp': '12345',
                         'x-trans-id': '-'}})

    def test_async_update_saves_on_exception(self):

        def fake_http_connect(*args):
            raise Exception('test')

        orig_http_connect = object_server.http_connect
        try:
            object_server.http_connect = fake_http_connect
            self.object_controller.async_update('PUT', 'a', 'c', 'o',
                '127.0.0.1:1234', 1, 'sdc1',
                {'x-timestamp': '1', 'x-out': 'set'}, 'sda1')
        finally:
            object_server.http_connect = orig_http_connect
        self.assertEquals(
            pickle.load(open(os.path.join(self.testdir, 'sda1',
                'async_pending', 'a83',
                '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000'))),
            {'headers': {'x-timestamp': '1', 'x-out': 'set'}, 'account': 'a',
             'container': 'c', 'obj': 'o', 'op': 'PUT'})

    def test_async_update_saves_on_non_2xx(self):

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
                self.object_controller.async_update('PUT', 'a', 'c', 'o',
                    '127.0.0.1:1234', 1, 'sdc1',
                    {'x-timestamp': '1', 'x-out': str(status)}, 'sda1')
                self.assertEquals(
                    pickle.load(open(os.path.join(self.testdir, 'sda1',
                        'async_pending', 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000'))),
                    {'headers': {'x-timestamp': '1', 'x-out': str(status)},
                     'account': 'a', 'container': 'c', 'obj': 'o',
                     'op': 'PUT'})
        finally:
            object_server.http_connect = orig_http_connect

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
                self.object_controller.async_update('PUT', 'a', 'c', 'o',
                    '127.0.0.1:1234', 1, 'sdc1',
                    {'x-timestamp': '1', 'x-out': str(status)}, 'sda1')
                self.assertFalse(
                    os.path.exists(os.path.join(self.testdir, 'sda1',
                        'async_pending', 'a83',
                        '06fbf0b514e5199dfc4e00f42eb5ea83-0000000001.00000')))
        finally:
            object_server.http_connect = orig_http_connect

    def test_delete_at_update_put(self):
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
            {'x-timestamp': '1'}, 'sda1')
        self.assertEquals(given_args, ['PUT', '.expiring_objects', '0',
            '2-a/c/o', None, None, None,
            {'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'x-content-type': 'text/plain', 'x-timestamp': '1',
             'x-trans-id': '-'},
            'sda1'])

    def test_delete_at_negative(self):
        # Test negative is reset to 0
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.delete_at_update(
            'PUT', -2, 'a', 'c', 'o', {'x-timestamp': '1'}, 'sda1')
        self.assertEquals(given_args, [
            'PUT', '.expiring_objects', '0', '0-a/c/o', None, None, None,
            {'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'x-content-type': 'text/plain', 'x-timestamp': '1',
             'x-trans-id': '-'},
            'sda1'])

    def test_delete_at_cap(self):
        # Test past cap is reset to cap
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.delete_at_update(
            'PUT', 12345678901, 'a', 'c', 'o', {'x-timestamp': '1'}, 'sda1')
        self.assertEquals(given_args, [
            'PUT', '.expiring_objects', '9999936000', '9999999999-a/c/o', None,
            None, None,
            {'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'x-content-type': 'text/plain', 'x-timestamp': '1',
             'x-trans-id': '-'},
            'sda1'])

    def test_delete_at_update_put_with_info(self):
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.delete_at_update('PUT', 2, 'a', 'c', 'o',
            {'x-timestamp': '1', 'X-Delete-At-Host': '127.0.0.1:1234',
             'X-Delete-At-Partition': '3', 'X-Delete-At-Device': 'sdc1'},
            'sda1')
        self.assertEquals(given_args, ['PUT', '.expiring_objects', '0',
            '2-a/c/o', '127.0.0.1:1234', '3', 'sdc1',
            {'x-size': '0', 'x-etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'x-content-type': 'text/plain', 'x-timestamp': '1',
             'x-trans-id': '-'},
            'sda1'])

    def test_delete_at_update_delete(self):
        given_args = []

        def fake_async_update(*args):
            given_args.extend(args)

        self.object_controller.async_update = fake_async_update
        self.object_controller.delete_at_update('DELETE', 2, 'a', 'c', 'o',
            {'x-timestamp': '1'}, 'sda1')
        self.assertEquals(given_args, ['DELETE', '.expiring_objects', '0',
            '2-a/c/o', None, None, None,
            {'x-timestamp': '1', 'x-trans-id': '-'}, 'sda1'])

    def test_POST_calls_delete_at(self):
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [])

        sleep(.00001)
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Type': 'application/x-test'})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(given_args, [])

        sleep(.00001)
        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = str(int(time() + 1000))
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Type': 'application/x-test',
                     'X-Delete-At': delete_at_timestamp1})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(given_args, [
            'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
            {'X-Delete-At': delete_at_timestamp1,
             'Content-Type': 'application/x-test',
             'X-Timestamp': timestamp1,
             'Host': 'localhost:80'},
            'sda1'])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        delete_at_timestamp2 = str(int(time() + 2000))
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Type': 'application/x-test',
                     'X-Delete-At': delete_at_timestamp2})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(given_args, [
            'PUT', int(delete_at_timestamp2), 'a', 'c', 'o',
            {'X-Delete-At': delete_at_timestamp2,
             'Content-Type': 'application/x-test',
             'X-Timestamp': timestamp2, 'Host': 'localhost:80'},
            'sda1',
            'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
            # This 2 timestamp is okay because it's ignored since it's just
            # part of the current request headers. The above 1 timestamp is the
            # important one.
            {'X-Delete-At': delete_at_timestamp2,
             'Content-Type': 'application/x-test',
             'X-Timestamp': timestamp2, 'Host': 'localhost:80'},
            'sda1'])

    def test_PUT_calls_delete_at(self):
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
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
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': delete_at_timestamp1})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [
            'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
            {'X-Delete-At': delete_at_timestamp1,
             'Content-Length': '4',
             'Content-Type': 'application/octet-stream',
             'X-Timestamp': timestamp1,
             'Host': 'localhost:80'},
            'sda1'])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        delete_at_timestamp2 = str(int(time() + 2000))
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': delete_at_timestamp2})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [
            'PUT', int(delete_at_timestamp2), 'a', 'c', 'o',
            {'X-Delete-At': delete_at_timestamp2,
             'Content-Length': '4',
             'Content-Type': 'application/octet-stream',
             'X-Timestamp': timestamp2, 'Host': 'localhost:80'},
            'sda1',
            'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
            # This 2 timestamp is okay because it's ignored since it's just
            # part of the current request headers. The above 1 timestamp is the
            # important one.
            {'X-Delete-At': delete_at_timestamp2,
             'Content-Length': '4',
             'Content-Type': 'application/octet-stream',
             'X-Timestamp': timestamp2, 'Host': 'localhost:80'},
            'sda1'])

    def test_GET_but_expired(self):
        test_time = time() + 10000
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(int(test_time + 100)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'GET'},
            headers={'X-Timestamp': normalize_timestamp(test_time)})
        resp = self.object_controller.GET(req)
        self.assertEquals(resp.status_int, 200)

        orig_time = object_server.time.time
        try:
            t = time()
            object_server.time.time = lambda: t
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(test_time - 1000),
                         'X-Delete-At': str(int(t + 1)),
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
            req.body = 'TEST'
            resp = self.object_controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Timestamp': normalize_timestamp(test_time)})
            resp = self.object_controller.GET(req)
            self.assertEquals(resp.status_int, 200)
        finally:
            object_server.time.time = orig_time

        orig_time = object_server.time.time
        try:
            t = time() + 2
            object_server.time.time = lambda: t
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'GET'},
                headers={'X-Timestamp': normalize_timestamp(t)})
            resp = self.object_controller.GET(req)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_HEAD_but_expired(self):
        test_time = time() + 10000
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(int(test_time + 100)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'X-Timestamp': normalize_timestamp(test_time)})
        resp = self.object_controller.HEAD(req)
        self.assertEquals(resp.status_int, 200)

        orig_time = object_server.time.time
        try:
            t = time()
            object_server.time.time = lambda: t
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Timestamp': normalize_timestamp(test_time - 1000),
                         'X-Delete-At': str(int(t + 1)),
                         'Content-Length': '4',
                         'Content-Type': 'application/octet-stream'})
            req.body = 'TEST'
            resp = self.object_controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'HEAD'},
                headers={'X-Timestamp': normalize_timestamp(test_time)})
            resp = self.object_controller.HEAD(req)
            self.assertEquals(resp.status_int, 200)
        finally:
            object_server.time.time = orig_time

        orig_time = object_server.time.time
        try:
            t = time() + 2
            object_server.time.time = lambda: t
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'HEAD'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = self.object_controller.HEAD(req)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_POST_but_expired(self):
        test_time = time() + 10000
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(int(test_time + 100)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 1500)})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 202)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 1000),
                     'X-Delete-At': str(int(time() + 1)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        orig_time = object_server.time.time
        try:
            t = time() + 2
            object_server.time.time = lambda: t
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'POST'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = self.object_controller.POST(req)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_DELETE_but_expired(self):
        test_time = time() + 10000
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 2000),
                     'X-Delete-At': str(int(test_time + 100)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        orig_time = object_server.time.time
        try:
            t = test_time + 100
            object_server.time.time = lambda: float(t)
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'DELETE'},
                headers={'X-Timestamp': normalize_timestamp(time())})
            resp = self.object_controller.DELETE(req)
            self.assertEquals(resp.status_int, 404)
        finally:
            object_server.time.time = orig_time

    def test_DELETE_if_delete_at(self):
        test_time = time() + 10000
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 99),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 98)})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 97),
                     'X-Delete-At': str(int(test_time - 1)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 95),
                     'X-If-Delete-At': str(int(test_time))})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 95)})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

        delete_at_timestamp = str(int(test_time - 1))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 94),
                     'X-Delete-At': delete_at_timestamp,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': str(int(test_time))})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 412)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': normalize_timestamp(test_time - 92),
                     'X-If-Delete-At': delete_at_timestamp})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_calls_delete_at(self):
        given_args = []

        def fake_delete_at_update(*args):
            given_args.extend(args)

        self.object_controller.delete_at_update = fake_delete_at_update

        timestamp1 = normalize_timestamp(time())
        delete_at_timestamp1 = str(int(time() + 1000))
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp1,
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream',
                     'X-Delete-At': delete_at_timestamp1})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(given_args, [
            'PUT', int(delete_at_timestamp1), 'a', 'c', 'o',
            {'X-Delete-At': delete_at_timestamp1,
             'Content-Length': '4',
             'Content-Type': 'application/octet-stream',
             'X-Timestamp': timestamp1,
             'Host': 'localhost:80'},
            'sda1'])

        while given_args:
            given_args.pop()

        sleep(.00001)
        timestamp2 = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Timestamp': timestamp2,
                     'Content-Type': 'application/octet-stream'})
        resp = self.object_controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(given_args, [
            'DELETE', int(delete_at_timestamp1), 'a', 'c', 'o',
            {'Content-Type': 'application/octet-stream',
             'Host': 'localhost:80', 'X-Timestamp': timestamp2},
            'sda1'])

    def test_PUT_delete_at_in_past(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'X-Delete-At': str(int(time() - 1)),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 400)
        self.assertTrue('X-Delete-At in past' in resp.body)

    def test_POST_delete_at_in_past(self):
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        req.body = 'TEST'
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(time() + 1),
                     'X-Delete-At': str(int(time() - 1))})
        resp = self.object_controller.POST(req)
        self.assertEquals(resp.status_int, 400)
        self.assertTrue('X-Delete-At in past' in resp.body)

    def test_REPLICATE_works(self):

        def fake_get_hashes(*args, **kwargs):
            return 0, {1: 2}

        def my_tpool_execute(func, *args, **kwargs):
            return func(*args, **kwargs)

        was_get_hashes = object_server.get_hashes
        object_server.get_hashes = fake_get_hashes
        was_tpool_exe = tpool.execute
        tpool.execute = my_tpool_execute
        try:
            req = Request.blank('/sda1/p/suff',
                environ={'REQUEST_METHOD': 'REPLICATE'},
                headers={})
            resp = self.object_controller.REPLICATE(req)
            self.assertEquals(resp.status_int, 200)
            p_data = pickle.loads(resp.body)
            self.assertEquals(p_data, {1: 2})
        finally:
            tpool.execute = was_tpool_exe
            object_server.get_hashes = was_get_hashes

    def test_REPLICATE_timeout(self):

        def fake_get_hashes(*args, **kwargs):
            raise Timeout()

        def my_tpool_execute(func, *args, **kwargs):
            return func(*args, **kwargs)

        was_get_hashes = object_server.get_hashes
        object_server.get_hashes = fake_get_hashes
        was_tpool_exe = tpool.execute
        tpool.execute = my_tpool_execute
        try:
            req = Request.blank('/sda1/p/suff',
                environ={'REQUEST_METHOD': 'REPLICATE'},
                headers={})
            self.assertRaises(Timeout, self.object_controller.REPLICATE, req)
        finally:
            tpool.execute = was_tpool_exe
            object_server.get_hashes = was_get_hashes

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

        orig_fallocate = object_server.fallocate
        try:
            object_server.fallocate = fake_fallocate
            timestamp = normalize_timestamp(time())
            body_reader = IgnoredBody()
            req = Request.blank('/sda1/p/a/c/o',
                    environ={'REQUEST_METHOD': 'PUT',
                             'wsgi.input': body_reader},
                    headers={'X-Timestamp': timestamp,
                             'Content-Length': '6',
                             'Content-Type': 'application/octet-stream',
                             'Expect': '100-continue'})
            resp = self.object_controller.PUT(req)
            self.assertEquals(resp.status_int, 507)
            self.assertFalse(body_reader.read_called)
        finally:
            object_server.fallocate = orig_fallocate

if __name__ == '__main__':
    unittest.main()
