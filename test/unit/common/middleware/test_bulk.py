# -*- coding: utf-8 -*-
# Copyright (c) 2012 OpenStack Foundation
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

from collections import Counter
import numbers
from six.moves import urllib
import unittest
import os
import tarfile
import zlib
import mock
import six
from six import BytesIO
from shutil import rmtree
from tempfile import mkdtemp
from eventlet import sleep
from mock import patch, call
from test.unit.common.middleware.helpers import FakeSwift
from swift.common import utils, constraints
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware import bulk
from swift.common.swob import Request, Response, HTTPException, \
    HTTPNoContent, HTTPCreated
from swift.common.http import HTTP_NOT_FOUND, HTTP_UNAUTHORIZED


class FakeApp(object):
    def __init__(self):
        self.calls = 0
        self.delete_paths = []
        self.max_pathlen = 100
        self.del_cont_total_calls = 2
        self.del_cont_cur_call = 0

    def __call__(self, env, start_response):
        self.calls += 1
        if env['PATH_INFO'].startswith('/unauth/'):
            if env['PATH_INFO'].endswith('/c/f_ok'):
                return Response(status='204 No Content')(env, start_response)
            return Response(status=401)(env, start_response)
        if env['PATH_INFO'].startswith('/create_cont/'):
            if env['REQUEST_METHOD'] == 'HEAD':
                return Response(status='404 Not Found')(env, start_response)
            return Response(status='201 Created')(env, start_response)
        if env['PATH_INFO'].startswith('/create_cont_fail/'):
            if env['REQUEST_METHOD'] == 'HEAD':
                return Response(status='403 Forbidden')(env, start_response)
            return Response(status='404 Not Found')(env, start_response)
        if env['PATH_INFO'].startswith('/create_obj_unauth/'):
            if env['PATH_INFO'].endswith('/cont'):
                return Response(status='201 Created')(env, start_response)
            return Response(status=401)(env, start_response)
        if env['PATH_INFO'].startswith('/tar_works/'):
            if len(env['PATH_INFO']) > self.max_pathlen:
                return Response(status='400 Bad Request')(env, start_response)
            return Response(status='201 Created')(env, start_response)
        if env['PATH_INFO'].startswith('/tar_works_cont_head_fail/'):
            if env['REQUEST_METHOD'] == 'HEAD':
                return Response(status='404 Not Found')(env, start_response)
            if len(env['PATH_INFO']) > 100:
                return Response(status='400 Bad Request')(env, start_response)
            return Response(status='201 Created')(env, start_response)
        if (env['PATH_INFO'].startswith('/delete_works/')
                and env['REQUEST_METHOD'] == 'DELETE'):
            self.delete_paths.append(env['PATH_INFO'])
            if len(env['PATH_INFO']) > self.max_pathlen:
                return Response(status='400 Bad Request')(env, start_response)
            if env['PATH_INFO'].endswith('404'):
                return Response(status='404 Not Found')(env, start_response)
            if env['PATH_INFO'].endswith('badutf8'):
                return Response(
                    status='412 Precondition Failed')(env, start_response)
            return Response(status='204 No Content')(env, start_response)
        if env['PATH_INFO'].startswith('/delete_cont_fail/'):
            return Response(status='409 Conflict')(env, start_response)
        if env['PATH_INFO'].startswith('/broke/'):
            return Response(status='500 Internal Error')(env, start_response)
        if env['PATH_INFO'].startswith('/delete_cont_success_after_attempts/'):
            if self.del_cont_cur_call < self.del_cont_total_calls:
                self.del_cont_cur_call += 1
                return Response(status='409 Conflict')(env, start_response)
            else:
                return Response(status='204 No Content')(env, start_response)


def build_dir_tree(start_path, tree_obj):
    if isinstance(tree_obj, list):
        for obj in tree_obj:
            build_dir_tree(start_path, obj)
    if isinstance(tree_obj, dict):
        for dir_name, obj in tree_obj.items():
            dir_path = os.path.join(start_path, dir_name)
            os.mkdir(dir_path)
            build_dir_tree(dir_path, obj)
    if isinstance(tree_obj, six.text_type):
        tree_obj = tree_obj.encode('utf8')
    if isinstance(tree_obj, str):
        obj_path = os.path.join(start_path, tree_obj)
        with open(obj_path, 'w+') as tree_file:
            tree_file.write('testing')


def build_tar_tree(tar, start_path, tree_obj, base_path=''):
    if isinstance(tree_obj, list):
        for obj in tree_obj:
            build_tar_tree(tar, start_path, obj, base_path=base_path)
    if isinstance(tree_obj, dict):
        for dir_name, obj in tree_obj.items():
            dir_path = os.path.join(start_path, dir_name)
            tar_info = tarfile.TarInfo(dir_path[len(base_path):])
            tar_info.type = tarfile.DIRTYPE
            tar.addfile(tar_info)
            build_tar_tree(tar, dir_path, obj, base_path=base_path)
    if isinstance(tree_obj, six.text_type):
        tree_obj = tree_obj.encode('utf8')
    if isinstance(tree_obj, str):
        obj_path = os.path.join(start_path, tree_obj)
        tar_info = tarfile.TarInfo('./' + obj_path[len(base_path):])
        tar.addfile(tar_info)


class TestUntarMetadata(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.bulk = bulk.filter_factory({})(self.app)
        self.testdir = mkdtemp(suffix='tmp_test_bulk')

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_extract_metadata(self):
        self.app.register('HEAD', '/v1/a/c?extract-archive=tar',
                          HTTPNoContent, {}, None)
        self.app.register('PUT', '/v1/a/c/obj1?extract-archive=tar',
                          HTTPCreated, {}, None)
        self.app.register('PUT', '/v1/a/c/obj2?extract-archive=tar',
                          HTTPCreated, {}, None)

        # It's a real pain to instantiate TarInfo objects directly; they
        # really want to come from a file on disk or a tarball. So, we write
        # out some files and add pax headers to them as they get placed into
        # the tarball.
        with open(os.path.join(self.testdir, "obj1"), "w") as fh1:
            fh1.write("obj1 contents\n")
        with open(os.path.join(self.testdir, "obj2"), "w") as fh2:
            fh2.write("obj2 contents\n")

        tar_ball = BytesIO()
        tar_file = tarfile.TarFile.open(fileobj=tar_ball, mode="w",
                                        format=tarfile.PAX_FORMAT)

        # With GNU tar 1.27.1 or later (possibly 1.27 as well), a file with
        # extended attribute user.thingy = dingy gets put into the tarfile
        # with pax_headers containing key/value pair
        # (SCHILY.xattr.user.thingy, dingy), both unicode strings (py2: type
        # unicode, not type str).
        #
        # With BSD tar (libarchive), you get key/value pair
        # (LIBARCHIVE.xattr.user.thingy, dingy), which strikes me as
        # gratuitous incompatibility.
        #
        # Still, we'll support uploads with both. Just heap more code on the
        # problem until you can forget it's under there.
        with open(os.path.join(self.testdir, "obj1")) as fh1:
            tar_info1 = tar_file.gettarinfo(fileobj=fh1,
                                            arcname="obj1")
            tar_info1.pax_headers[u'SCHILY.xattr.user.mime_type'] = \
                u'application/food-diary'
            tar_info1.pax_headers[u'SCHILY.xattr.user.meta.lunch'] = \
                u'sopa de albóndigas'
            tar_info1.pax_headers[
                u'SCHILY.xattr.user.meta.afternoon-snack'] = \
                u'gigantic bucket of coffee'
            tar_file.addfile(tar_info1, fh1)

        with open(os.path.join(self.testdir, "obj2")) as fh2:
            tar_info2 = tar_file.gettarinfo(fileobj=fh2,
                                            arcname="obj2")
            tar_info2.pax_headers[
                u'LIBARCHIVE.xattr.user.meta.muppet'] = u'bert'
            tar_info2.pax_headers[
                u'LIBARCHIVE.xattr.user.meta.cat'] = u'fluffy'
            tar_info2.pax_headers[
                u'LIBARCHIVE.xattr.user.notmeta'] = u'skipped'
            tar_file.addfile(tar_info2, fh2)

        tar_ball.seek(0)

        req = Request.blank('/v1/a/c?extract-archive=tar')
        req.environ['REQUEST_METHOD'] = 'PUT'
        req.environ['wsgi.input'] = tar_ball
        req.headers['transfer-encoding'] = 'chunked'
        req.headers['accept'] = 'application/json;q=1.0'

        resp = req.get_response(self.bulk)
        self.assertEqual(resp.status_int, 200)

        # sanity check to make sure the upload worked
        upload_status = utils.json.loads(resp.body)
        self.assertEqual(upload_status['Number Files Created'], 2)

        put1_headers = HeaderKeyDict(self.app.calls_with_headers[1][2])
        self.assertEqual(
            put1_headers.get('Content-Type'),
            'application/food-diary')
        self.assertEqual(
            put1_headers.get('X-Object-Meta-Lunch'),
            'sopa de alb\xc3\xb3ndigas')
        self.assertEqual(
            put1_headers.get('X-Object-Meta-Afternoon-Snack'),
            'gigantic bucket of coffee')

        put2_headers = HeaderKeyDict(self.app.calls_with_headers[2][2])
        self.assertEqual(put2_headers.get('X-Object-Meta-Muppet'), 'bert')
        self.assertEqual(put2_headers.get('X-Object-Meta-Cat'), 'fluffy')
        self.assertIsNone(put2_headers.get('Content-Type'))
        self.assertIsNone(put2_headers.get('X-Object-Meta-Blah'))


class TestUntar(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.bulk = bulk.filter_factory({})(self.app)
        self.testdir = mkdtemp(suffix='tmp_test_bulk')

    def tearDown(self):
        self.app.calls = 0
        rmtree(self.testdir, ignore_errors=1)

    def handle_extract_and_iter(self, req, compress_format,
                                out_content_type='application/json'):
        iter = self.bulk.handle_extract_iter(
            req, compress_format, out_content_type=out_content_type)
        first_chunk = next(iter)
        self.assertEqual(req.environ['eventlet.minimum_write_chunk_size'], 0)
        resp_body = first_chunk + ''.join(iter)
        return resp_body

    def test_create_container_for_path(self):
        req = Request.blank('/')
        self.assertEqual(
            self.bulk.create_container(req, '/create_cont/acc/cont'),
            True)
        self.assertEqual(self.app.calls, 2)
        self.assertRaises(
            bulk.CreateContainerError,
            self.bulk.create_container,
            req, '/create_cont_fail/acc/cont')
        self.assertEqual(self.app.calls, 3)

    def test_extract_tar_works(self):
        # On systems where $TMPDIR is long (like OS X), we need to do this
        # or else every upload will fail due to the path being too long.
        self.app.max_pathlen += len(self.testdir)
        for compress_format in ['', 'gz', 'bz2']:
            base_name = 'base_works_%s' % compress_format
            dir_tree = [
                {base_name: [{'sub_dir1': ['sub1_file1', 'sub1_file2']},
                             {'sub_dir2': ['sub2_file1', u'test obj \u2661']},
                             'sub_file1',
                             {'sub_dir3': [{'sub4_dir1': '../sub4 file1'}]},
                             {'sub_dir4': None},
                             ]}]

            build_dir_tree(self.testdir, dir_tree)
            mode = 'w'
            extension = ''
            if compress_format:
                mode += ':' + compress_format
                extension += '.' + compress_format
            tar = tarfile.open(name=os.path.join(self.testdir,
                                                 'tar_works.tar' + extension),
                               mode=mode)
            tar.add(os.path.join(self.testdir, base_name))
            tar.close()
            req = Request.blank('/tar_works/acc/cont/')
            req.environ['wsgi.input'] = open(
                os.path.join(self.testdir, 'tar_works.tar' + extension))
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, compress_format)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Number Files Created'], 6)

            # test out xml
            req = Request.blank('/tar_works/acc/cont/')
            req.environ['wsgi.input'] = open(
                os.path.join(self.testdir, 'tar_works.tar' + extension))
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(
                req, compress_format, 'application/xml')
            self.assertTrue(
                '<response_status>201 Created</response_status>' in
                resp_body)
            self.assertTrue(
                '<number_files_created>6</number_files_created>' in
                resp_body)

            # test out nonexistent format
            req = Request.blank('/tar_works/acc/cont/?extract-archive=tar',
                                headers={'Accept': 'good_xml'})
            req.environ['REQUEST_METHOD'] = 'PUT'
            req.environ['wsgi.input'] = open(
                os.path.join(self.testdir, 'tar_works.tar' + extension))
            req.headers['transfer-encoding'] = 'chunked'

            def fake_start_response(*args, **kwargs):
                pass

            app_iter = self.bulk(req.environ, fake_start_response)
            resp_body = ''.join([i for i in app_iter])

            self.assertTrue('Response Status: 406' in resp_body)

    def test_extract_call(self):
        base_name = 'base_works_gz'
        dir_tree = [
            {base_name: [{'sub_dir1': ['sub1_file1', 'sub1_file2']},
                         {'sub_dir2': ['sub2_file1', 'sub2_file2']},
                         'sub_file1',
                         {'sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]}]
        build_dir_tree(self.testdir, dir_tree)
        tar = tarfile.open(name=os.path.join(self.testdir,
                                             'tar_works.tar.gz'),
                           mode='w:gz')
        tar.add(os.path.join(self.testdir, base_name))
        tar.close()

        def fake_start_response(*args, **kwargs):
            pass

        req = Request.blank('/tar_works/acc/cont/?extract-archive=tar.gz')
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar.gz'))
        self.bulk(req.environ, fake_start_response)
        self.assertEqual(self.app.calls, 1)

        self.app.calls = 0
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar.gz'))
        req.headers['transfer-encoding'] = 'Chunked'
        req.method = 'PUT'
        app_iter = self.bulk(req.environ, fake_start_response)
        list(app_iter)  # iter over resp
        self.assertEqual(self.app.calls, 7)

        self.app.calls = 0
        req = Request.blank('/tar_works/acc/cont/?extract-archive=bad')
        req.method = 'PUT'
        req.headers['transfer-encoding'] = 'Chunked'
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar.gz'))
        t = self.bulk(req.environ, fake_start_response)
        self.assertEqual(t[0], "Unsupported archive format")

        tar = tarfile.open(name=os.path.join(self.testdir,
                                             'tar_works.tar'),
                           mode='w')
        tar.add(os.path.join(self.testdir, base_name))
        tar.close()
        self.app.calls = 0
        req = Request.blank('/tar_works/acc/cont/?extract-archive=tar')
        req.method = 'PUT'
        req.headers['transfer-encoding'] = 'Chunked'
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar'))
        app_iter = self.bulk(req.environ, fake_start_response)
        list(app_iter)  # iter over resp
        self.assertEqual(self.app.calls, 7)

    def test_bad_container(self):
        req = Request.blank('/invalid/', body='')
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertTrue('404 Not Found' in resp_body)

    def test_content_length_required(self):
        req = Request.blank('/create_cont_fail/acc/cont')
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertTrue('411 Length Required' in resp_body)

    def test_bad_tar(self):
        req = Request.blank('/create_cont_fail/acc/cont', body='')

        def bad_open(*args, **kwargs):
            raise zlib.error('bad tar')

        with patch.object(tarfile, 'open', bad_open):
            resp_body = self.handle_extract_and_iter(req, '')
            self.assertTrue('400 Bad Request' in resp_body)

    def build_tar(self, dir_tree=None):
        if not dir_tree:
            dir_tree = [
                {'base_fails1': [{'sub_dir1': ['sub1_file1']},
                                 {'sub_dir2': ['sub2_file1', 'sub2_file2']},
                                 'f' * 101,
                                 {'sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]}]
        tar = tarfile.open(name=os.path.join(self.testdir, 'tar_fails.tar'),
                           mode='w')
        build_tar_tree(tar, self.testdir, dir_tree,
                       base_path=self.testdir + '/')
        tar.close()
        return tar

    def test_extract_tar_with_basefile(self):
        dir_tree = [
            'base_lvl_file', 'another_base_file',
            {'base_fails1': [{'sub_dir1': ['sub1_file1']},
                             {'sub_dir2': ['sub2_file1', 'sub2_file2']},
                             {'sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]}]
        self.build_tar(dir_tree)
        req = Request.blank('/tar_works/acc/')
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Files Created'], 4)

    def test_extract_tar_fail_cont_401(self):
        self.build_tar()
        req = Request.blank('/unauth/acc/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertEqual(self.app.calls, 1)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Response Status'], '401 Unauthorized')
        self.assertEqual(resp_data['Errors'], [])

    def test_extract_tar_fail_obj_401(self):
        self.build_tar()
        req = Request.blank('/create_obj_unauth/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertEqual(self.app.calls, 2)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Response Status'], '401 Unauthorized')
        self.assertEqual(
            resp_data['Errors'],
            [['cont/base_fails1/sub_dir1/sub1_file1', '401 Unauthorized']])

    def test_extract_tar_fail_obj_name_len(self):
        self.build_tar()
        req = Request.blank('/tar_works/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertEqual(self.app.calls, 6)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Files Created'], 4)
        self.assertEqual(
            resp_data['Errors'],
            [['cont/base_fails1/' + ('f' * 101), '400 Bad Request']])

    def test_extract_tar_fail_compress_type(self):
        self.build_tar()
        req = Request.blank('/tar_works/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, 'gz')
        self.assertEqual(self.app.calls, 0)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(
            resp_data['Response Body'].lower(),
            'invalid tar file: not a gzip file')

    def test_extract_tar_fail_max_failed_extractions(self):
        self.build_tar()
        with patch.object(self.bulk, 'max_failed_extractions', 1):
            self.app.calls = 0
            req = Request.blank('/tar_works/acc/cont/',
                                headers={'Accept': 'application/json'})
            req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                          'tar_fails.tar'))
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, '')
            self.assertEqual(self.app.calls, 5)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Number Files Created'], 3)
            self.assertEqual(
                resp_data['Errors'],
                [['cont/base_fails1/' + ('f' * 101), '400 Bad Request']])

    @patch.object(constraints, 'MAX_FILE_SIZE', 4)
    def test_extract_tar_fail_max_file_size(self):
        tar = self.build_tar()
        dir_tree = [{'test': [{'sub_dir1': ['sub1_file1']}]}]
        build_dir_tree(self.testdir, dir_tree)
        tar = tarfile.open(name=os.path.join(self.testdir,
                                             'tar_works.tar'),
                           mode='w')
        tar.add(os.path.join(self.testdir, 'test'))
        tar.close()
        self.app.calls = 0
        req = Request.blank('/tar_works/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(
            resp_data['Errors'],
            [['cont' + self.testdir + '/test/sub_dir1/sub1_file1',
              '413 Request Entity Too Large']])

    def test_extract_tar_fail_max_cont(self):
        dir_tree = [{'sub_dir1': ['sub1_file1']},
                    {'sub_dir2': ['sub2_file1', 'sub2_file2']},
                    'f' * 101,
                    {'sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]
        self.build_tar(dir_tree)
        with patch.object(self.bulk, 'max_containers', 1):
            self.app.calls = 0
            body = open(os.path.join(self.testdir, 'tar_fails.tar')).read()
            req = Request.blank('/tar_works_cont_head_fail/acc/', body=body,
                                headers={'Accept': 'application/json'})
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, '')
            self.assertEqual(self.app.calls, 5)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Response Status'], '400 Bad Request')
            self.assertEqual(
                resp_data['Response Body'],
                'More than 1 containers to create from tar.')

    def test_extract_tar_fail_create_cont(self):
        dir_tree = [{'base_fails1': [
            {'sub_dir1': ['sub1_file1']},
            {'sub_dir2': ['sub2_file1', 'sub2_file2']},
            {'./sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]}]
        self.build_tar(dir_tree)
        req = Request.blank('/create_cont_fail/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(self.app.calls, 5)
        self.assertEqual(len(resp_data['Errors']), 5)

    def test_extract_tar_fail_create_cont_value_err(self):
        self.build_tar()
        req = Request.blank('/create_cont_fail/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'

        def bad_create(req, path):
            raise ValueError('Test')

        with patch.object(self.bulk, 'create_container', bad_create):
            resp_body = self.handle_extract_and_iter(req, '')
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(self.app.calls, 0)
            self.assertEqual(len(resp_data['Errors']), 5)
            self.assertEqual(
                resp_data['Errors'][0],
                ['cont/base_fails1/sub_dir1/sub1_file1', '400 Bad Request'])

    def test_extract_tar_fail_unicode(self):
        dir_tree = [{'sub_dir1': ['sub1_file1']},
                    {'sub_dir2': ['sub2\xdefile1', 'sub2_file2']},
                    {'sub_\xdedir3': [{'sub4_dir1': 'sub4_file1'}]}]
        self.build_tar(dir_tree)
        req = Request.blank('/tar_works/acc/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(self.app.calls, 4)
        self.assertEqual(resp_data['Number Files Created'], 2)
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(
            resp_data['Errors'],
            [['sub_dir2/sub2%DEfile1', '412 Precondition Failed'],
             ['sub_%DEdir3/sub4_dir1/sub4_file1', '412 Precondition Failed']])

    def test_get_response_body(self):
        txt_body = bulk.get_response_body(
            'bad_formay', {'hey': 'there'}, [['json > xml', '202 Accepted']],
            "doesn't matter for text")
        self.assertTrue('hey: there' in txt_body)
        xml_body = bulk.get_response_body(
            'text/xml', {'hey': 'there'}, [['json > xml', '202 Accepted']],
            'root_tag')
        self.assertTrue('&gt' in xml_body)
        self.assertTrue(xml_body.startswith('<root_tag>\n'))
        self.assertTrue(xml_body.endswith('\n</root_tag>\n'))


class TestDelete(unittest.TestCase):
    conf = {'delete_concurrency': 1}  # default to old single-threaded behavior

    def setUp(self):
        self.app = FakeApp()
        self.bulk = bulk.filter_factory(self.conf)(self.app)

    def tearDown(self):
        self.app.calls = 0
        self.app.delete_paths = []

    def handle_delete_and_iter(self, req, out_content_type='application/json'):
        iter = self.bulk.handle_delete_iter(
            req, out_content_type=out_content_type)
        first_chunk = next(iter)
        self.assertEqual(req.environ['eventlet.minimum_write_chunk_size'], 0)
        resp_body = first_chunk + ''.join(iter)
        return resp_body

    def test_bulk_delete_uses_predefined_object_errors(self):
        req = Request.blank('/delete_works/AUTH_Acc')
        objs_to_delete = [
            {'name': '/c/file_a'},
            {'name': '/c/file_b', 'error': {'code': HTTP_NOT_FOUND,
                                            'message': 'not found'}},
            {'name': '/c/file_c', 'error': {'code': HTTP_UNAUTHORIZED,
                                            'message': 'unauthorized'}},
            {'name': '/c/file_d'}]
        resp_body = ''.join(self.bulk.handle_delete_iter(
            req, objs_to_delete=objs_to_delete,
            out_content_type='application/json'))
        self.assertEqual(set(self.app.delete_paths),
                         set(['/delete_works/AUTH_Acc/c/file_a',
                              '/delete_works/AUTH_Acc/c/file_d']))
        self.assertEqual(self.app.calls, 2)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Number Deleted'], 2)
        self.assertEqual(resp_data['Number Not Found'], 1)
        self.assertEqual(resp_data['Errors'],
                         [['/c/file_c', 'unauthorized']])

    def test_bulk_delete_works_with_POST_verb(self):
        req = Request.blank('/delete_works/AUTH_Acc', body='/c/f\n/c/f404',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEqual(set(self.app.delete_paths),
                         set(['/delete_works/AUTH_Acc/c/f',
                              '/delete_works/AUTH_Acc/c/f404']))
        self.assertEqual(self.app.calls, 2)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Deleted'], 1)
        self.assertEqual(resp_data['Number Not Found'], 1)

    def test_bulk_delete_works_with_DELETE_verb(self):
        req = Request.blank('/delete_works/AUTH_Acc', body='/c/f\n/c/f404',
                            headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEqual(set(self.app.delete_paths),
                         set(['/delete_works/AUTH_Acc/c/f',
                              '/delete_works/AUTH_Acc/c/f404']))
        self.assertEqual(self.app.calls, 2)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Deleted'], 1)
        self.assertEqual(resp_data['Number Not Found'], 1)

    def test_bulk_delete_bad_content_type(self):
        req = Request.blank('/delete_works/AUTH_Acc',
                            headers={'Accept': 'badformat'})

        req = Request.blank('/delete_works/AUTH_Acc',
                            headers={'Accept': 'application/json',
                                     'Content-Type': 'text/xml'})
        req.method = 'POST'
        req.environ['wsgi.input'] = BytesIO(b'/c/f\n/c/f404')
        resp_body = self.handle_delete_and_iter(req)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Response Status'], '406 Not Acceptable')

    def test_bulk_delete_call_and_content_type(self):
        def fake_start_response(*args, **kwargs):
            self.assertEqual(args[1][0], ('Content-Type', 'application/json'))

        req = Request.blank('/delete_works/AUTH_Acc?bulk-delete')
        req.method = 'POST'
        req.headers['Transfer-Encoding'] = 'chunked'
        req.headers['Accept'] = 'application/json'
        req.environ['wsgi.input'] = BytesIO(b'/c/f%20')
        list(self.bulk(req.environ, fake_start_response))  # iterate over resp
        self.assertEqual(
            self.app.delete_paths, ['/delete_works/AUTH_Acc/c/f '])
        self.assertEqual(self.app.calls, 1)

    def test_bulk_delete_get_objs(self):
        req = Request.blank('/delete_works/AUTH_Acc', body='1%20\r\n2\r\n')
        req.method = 'POST'
        with patch.object(self.bulk, 'max_deletes_per_request', 2):
            results = self.bulk.get_objs_to_delete(req)
            self.assertEqual(results, [{'name': '1 '}, {'name': '2'}])

        with patch.object(self.bulk, 'max_path_length', 2):
            results = []
            req.environ['wsgi.input'] = BytesIO(b'1\n2\n3')
            results = self.bulk.get_objs_to_delete(req)
            self.assertEqual(results,
                             [{'name': '1'}, {'name': '2'}, {'name': '3'}])

        with patch.object(self.bulk, 'max_deletes_per_request', 9):
            with patch.object(self.bulk, 'max_path_length', 1):
                req_body = '\n'.join([str(i) for i in range(10)])
                req = Request.blank('/delete_works/AUTH_Acc', body=req_body)
                self.assertRaises(
                    HTTPException, self.bulk.get_objs_to_delete, req)

    def test_bulk_delete_works_extra_newlines_extra_quoting(self):
        req = Request.blank('/delete_works/AUTH_Acc',
                            body='/c/f\n\n\n/c/f404\n\n\n/c/%2525',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEqual(
            Counter(self.app.delete_paths),
            Counter(['/delete_works/AUTH_Acc/c/f',
                     '/delete_works/AUTH_Acc/c/f404',
                     '/delete_works/AUTH_Acc/c/%25']))
        self.assertEqual(self.app.calls, 3)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Deleted'], 2)
        self.assertEqual(resp_data['Number Not Found'], 1)

    def test_bulk_delete_too_many_newlines(self):
        req = Request.blank('/delete_works/AUTH_Acc')
        req.method = 'POST'
        data = b'\n\n' * self.bulk.max_deletes_per_request
        req.environ['wsgi.input'] = BytesIO(data)
        req.content_length = len(data)
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('413 Request Entity Too Large' in resp_body)

    def test_bulk_delete_works_unicode(self):
        body = (u'/c/ obj \u2661\r\n'.encode('utf8') +
                'c/ objbadutf8\r\n' +
                '/c/f\xdebadutf8\n')
        req = Request.blank('/delete_works/AUTH_Acc', body=body,
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEqual(
            Counter(self.app.delete_paths),
            Counter(['/delete_works/AUTH_Acc/c/ obj \xe2\x99\xa1',
                     '/delete_works/AUTH_Acc/c/ objbadutf8']))

        self.assertEqual(self.app.calls, 2)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Deleted'], 1)
        self.assertEqual(len(resp_data['Errors']), 2)
        self.assertEqual(
            Counter(map(tuple, resp_data['Errors'])),
            Counter([(urllib.parse.quote('c/ objbadutf8'),
                      '412 Precondition Failed'),
                     (urllib.parse.quote('/c/f\xdebadutf8'),
                      '412 Precondition Failed')]))

    def test_bulk_delete_no_body(self):
        req = Request.blank('/unauth/AUTH_acc/')
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('411 Length Required' in resp_body)

    def test_bulk_delete_no_files_in_body(self):
        req = Request.blank('/unauth/AUTH_acc/', body=' ')
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('400 Bad Request' in resp_body)

    def test_bulk_delete_unauth(self):
        req = Request.blank('/unauth/AUTH_acc/', body='/c/f\n/c/f_ok\n',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEqual(self.app.calls, 2)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Errors'], [['/c/f', '401 Unauthorized']])
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Number Deleted'], 1)

    def test_bulk_delete_500_resp(self):
        req = Request.blank('/broke/AUTH_acc/', body='/c/f\nc/f2\n',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        resp_body = self.handle_delete_and_iter(req)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(
            Counter(map(tuple, resp_data['Errors'])),
            Counter([('/c/f', '500 Internal Error'),
                     ('c/f2', '500 Internal Error')]))
        self.assertEqual(resp_data['Response Status'], '502 Bad Gateway')

    def test_bulk_delete_bad_path(self):
        req = Request.blank('/delete_cont_fail/')
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('404 Not Found' in resp_body)

    def test_bulk_delete_container_delete(self):
        req = Request.blank('/delete_cont_fail/AUTH_Acc', body='c\n',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        with patch('swift.common.middleware.bulk.sleep',
                   new=mock.MagicMock(wraps=sleep,
                                      return_value=None)) as mock_sleep:
            resp_body = self.handle_delete_and_iter(req)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Number Deleted'], 0)
            self.assertEqual(resp_data['Errors'], [['c', '409 Conflict']])
            self.assertEqual(resp_data['Response Status'], '400 Bad Request')
            self.assertEqual([], mock_sleep.call_args_list)

    def test_bulk_delete_container_delete_retry_and_fails(self):
        self.bulk.retry_count = 3
        req = Request.blank('/delete_cont_fail/AUTH_Acc', body='c\n',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        with patch('swift.common.middleware.bulk.sleep',
                   new=mock.MagicMock(wraps=sleep,
                                      return_value=None)) as mock_sleep:
            resp_body = self.handle_delete_and_iter(req)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Number Deleted'], 0)
            self.assertEqual(resp_data['Errors'], [['c', '409 Conflict']])
            self.assertEqual(resp_data['Response Status'], '400 Bad Request')
            self.assertEqual([call(self.bulk.retry_interval),
                              call(self.bulk.retry_interval ** 2),
                              call(self.bulk.retry_interval ** 3)],
                             mock_sleep.call_args_list)

    def test_bulk_delete_container_delete_retry_and_success(self):
        self.bulk.retry_count = 3
        self.app.del_container_total = 2
        req = Request.blank('/delete_cont_success_after_attempts/AUTH_Acc',
                            body='c\n', headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        with patch('swift.common.middleware.bulk.sleep',
                   new=mock.MagicMock(wraps=sleep,
                                      return_value=None)) as mock_sleep:
            resp_body = self.handle_delete_and_iter(req)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Number Deleted'], 1)
            self.assertEqual(resp_data['Errors'], [])
            self.assertEqual(resp_data['Response Status'], '200 OK')
            self.assertEqual([call(self.bulk.retry_interval),
                              call(self.bulk.retry_interval ** 2)],
                             mock_sleep.call_args_list)

    def test_bulk_delete_bad_file_too_long(self):
        req = Request.blank('/delete_works/AUTH_Acc',
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        bad_file = 'c/' + ('1' * self.bulk.max_path_length)
        data = b'/c/f\n' + bad_file.encode('ascii') + b'\n/c/f'
        req.environ['wsgi.input'] = BytesIO(data)
        req.headers['Transfer-Encoding'] = 'chunked'
        resp_body = self.handle_delete_and_iter(req)
        resp_data = utils.json.loads(resp_body)
        self.assertEqual(resp_data['Number Deleted'], 2)
        self.assertEqual(resp_data['Errors'], [[bad_file, '400 Bad Request']])
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')

    def test_bulk_delete_bad_file_over_twice_max_length(self):
        body = '/c/f\nc/' + ('123456' * self.bulk.max_path_length) + '\n'
        req = Request.blank('/delete_works/AUTH_Acc', body=body)
        req.method = 'POST'
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('400 Bad Request' in resp_body)

    def test_bulk_delete_max_failures(self):
        body = '\n'.join([
            '/c/f1', '/c/f2', '/c/f3', '/c/f4', '/c/f5', '/c/f6',
        ])
        req = Request.blank('/unauth/AUTH_Acc', body=body,
                            headers={'Accept': 'application/json'})
        req.method = 'POST'
        with patch.object(self.bulk, 'max_failed_deletes', 2):
            resp_body = self.handle_delete_and_iter(req)
            # We know there should be at least max_failed_deletes, but there
            # may be more as we clean up in-progress requests.
            self.assertGreaterEqual(self.app.calls,
                                    self.bulk.max_failed_deletes)
            # As we're pulling things off the pile, we:
            #   - get delete result,
            #   - process the result,
            #   - check max_failed_deletes,
            #   - spawn another delete, repeat.
            # As a result, we know our app calls should be *strictly* less.
            # Note this means that when delete_concurrency is one,
            # self.app.calls will exactly equal self.bulk.max_failed_deletes.
            self.assertLess(self.app.calls,
                            self.bulk.max_failed_deletes +
                            self.bulk.delete_concurrency)
            resp_data = utils.json.loads(resp_body)
            self.assertEqual(resp_data['Response Status'], '400 Bad Request')
            self.assertEqual(resp_data['Response Body'],
                             'Max delete failures exceeded')
            self.assertIn(['/c/f1', '401 Unauthorized'], resp_data['Errors'])
            self.assertIn(['/c/f2', '401 Unauthorized'], resp_data['Errors'])


class TestConcurrentDelete(TestDelete):
    conf = {'delete_concurrency': 3}

    def test_concurrency_set(self):
        self.assertEqual(self.bulk.delete_concurrency, 3)


class TestConfig(unittest.TestCase):
    def test_defaults(self):
        expected_defaults = {
            'delete_concurrency': 2,
            'max_containers': 10000,
            'max_deletes_per_request': 10000,
            'max_failed_deletes': 1000,
            'max_failed_extractions': 1000,
            'retry_count': 0,
            'retry_interval': 1.5,
            'yield_frequency': 10,
        }

        filter_app = bulk.filter_factory({})(FakeApp())
        self.assertEqual(expected_defaults, {k: getattr(filter_app, k)
                                             for k in expected_defaults})

        filter_app = bulk.Bulk(FakeApp(), None)
        self.assertEqual(expected_defaults, {k: getattr(filter_app, k)
                                             for k in expected_defaults})

    def test_delete_concurrency(self):
        # Must be an integer
        conf = {'delete_concurrency': '1.5'}
        self.assertRaises(ValueError, bulk.filter_factory, conf)

        conf = {'delete_concurrency': 'asdf'}
        self.assertRaises(ValueError, bulk.filter_factory, conf)

        # Will be at least one
        conf = {'delete_concurrency': '-1'}
        filter_app = bulk.filter_factory(conf)(FakeApp())
        self.assertEqual(1, filter_app.delete_concurrency)

        conf = {'delete_concurrency': '0'}
        filter_app = bulk.filter_factory(conf)(FakeApp())
        self.assertEqual(1, filter_app.delete_concurrency)

        # But if you want to set it stupid-high, we won't stop you
        conf = {'delete_concurrency': '1000'}
        filter_app = bulk.filter_factory(conf)(FakeApp())
        self.assertEqual(1000, filter_app.delete_concurrency)

        # ...unless it's extra-stupid-high, in which case we cap it
        conf = {'delete_concurrency': '1001'}
        filter_app = bulk.filter_factory(conf)(FakeApp())
        self.assertEqual(1000, filter_app.delete_concurrency)


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        bulk.filter_factory({})
        swift_info = utils.get_swift_info()
        self.assertTrue('bulk_upload' in swift_info)
        self.assertTrue(isinstance(
            swift_info['bulk_upload'].get('max_containers_per_extraction'),
            numbers.Integral))
        self.assertTrue(isinstance(
            swift_info['bulk_upload'].get('max_failed_extractions'),
            numbers.Integral))

        self.assertTrue('bulk_delete' in swift_info)
        self.assertTrue(isinstance(
            swift_info['bulk_delete'].get('max_deletes_per_request'),
            numbers.Integral))
        self.assertTrue(isinstance(
            swift_info['bulk_delete'].get('max_failed_deletes'),
            numbers.Integral))

if __name__ == '__main__':
    unittest.main()
