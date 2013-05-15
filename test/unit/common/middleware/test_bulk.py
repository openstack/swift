# Copyright (c) 2012 OpenStack, LLC.
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

import unittest
import os
import tarfile
import urllib
from shutil import rmtree
from tempfile import mkdtemp
from StringIO import StringIO
from mock import patch
from swift.common.middleware import bulk
from swift.common.swob import Request, Response, HTTPException
from swift.common.utils import json


class FakeApp(object):
    def __init__(self):
        self.calls = 0
        self.delete_paths = []

    def __call__(self, env, start_response):
        self.calls += 1
        if env['PATH_INFO'].startswith('/unauth/'):
            return Response(status=401)(env, start_response)
        if env['PATH_INFO'].startswith('/create_cont/'):
            return Response(status='201 Created')(env, start_response)
        if env['PATH_INFO'].startswith('/create_cont_fail/'):
            return Response(status='404 Not Found')(env, start_response)
        if env['PATH_INFO'].startswith('/create_obj_unauth/'):
            if env['PATH_INFO'].endswith('/cont'):
                return Response(status='201 Created')(env, start_response)
            return Response(status=401)(env, start_response)
        if env['PATH_INFO'].startswith('/tar_works/'):
            if len(env['PATH_INFO']) > 100:
                return Response(status='400 Bad Request')(env, start_response)
            return Response(status='201 Created')(env, start_response)
        if env['PATH_INFO'].startswith('/delete_works/'):
            self.delete_paths.append(env['PATH_INFO'])
            if len(env['PATH_INFO']) > 100:
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


def build_dir_tree(start_path, tree_obj):
    if isinstance(tree_obj, list):
        for obj in tree_obj:
            build_dir_tree(start_path, obj)
    if isinstance(tree_obj, dict):
        for dir_name, obj in tree_obj.iteritems():
            dir_path = os.path.join(start_path, dir_name)
            os.mkdir(dir_path)
            build_dir_tree(dir_path, obj)
    if isinstance(tree_obj, unicode):
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
        for dir_name, obj in tree_obj.iteritems():
            dir_path = os.path.join(start_path, dir_name)
            tar_info = tarfile.TarInfo(dir_path[len(base_path):])
            tar_info.type = tarfile.DIRTYPE
            tar.addfile(tar_info)
            build_tar_tree(tar, dir_path, obj, base_path=base_path)
    if isinstance(tree_obj, unicode):
        tree_obj = tree_obj.encode('utf8')
    if isinstance(tree_obj, str):
        obj_path = os.path.join(start_path, tree_obj)
        tar_info = tarfile.TarInfo('./' + obj_path[len(base_path):])
        tar.addfile(tar_info)


class TestUntar(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.bulk = bulk.filter_factory({})(self.app)
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_bulk')
        os.mkdir(self.testdir)

    def tearDown(self):
        self.app.calls = 0
        rmtree(self.testdir)

    def handle_extract_and_iter(self, req, compress_format):
        resp_body = ''.join(
            self.bulk.handle_extract_iter(req, compress_format))
        return resp_body

    def test_create_container_for_path(self):
        req = Request.blank('/')
        self.assertEquals(
            self.bulk.create_container(req, '/create_cont/acc/cont'),
            None)
        self.assertRaises(
            bulk.CreateContainerError,
            self.bulk.create_container,
            req, '/create_cont_fail/acc/cont')

    def test_extract_tar_works(self):
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
            req = Request.blank('/tar_works/acc/cont/',
                                headers={'Accept': 'application/json'})
            req.environ['wsgi.input'] = open(
                os.path.join(self.testdir, 'tar_works.tar' + extension))
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, compress_format)
            resp_data = json.loads(resp_body)
            self.assertEquals(resp_data['Number Files Created'], 6)

            # test out xml
            req = Request.blank('/tar_works/acc/cont/',
                                headers={'Accept': 'application/xml'})
            req.environ['wsgi.input'] = open(
                os.path.join(self.testdir, 'tar_works.tar' + extension))
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, compress_format)
            self.assert_('<response_status>201 Created</response_status>' in
                         resp_body)
            self.assert_('<number_files_created>6</number_files_created>' in
                         resp_body)

            # test out nonexistent format
            req = Request.blank('/tar_works/acc/cont/',
                                headers={'Accept': 'good_xml'})
            req.environ['wsgi.input'] = open(
                os.path.join(self.testdir, 'tar_works.tar' + extension))
            resp_body = self.handle_extract_and_iter(req, compress_format)
            self.assert_('Response Status: 406' in resp_body)

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
        self.assertEquals(self.app.calls, 1)

        self.app.calls = 0
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar.gz'))
        req.headers['transfer-encoding'] = 'Chunked'
        req.method = 'PUT'
        app_iter = self.bulk(req.environ, fake_start_response)
        resp_body = ''.join([i for i in app_iter])
        self.assertEquals(self.app.calls, 7)

        self.app.calls = 0
        req = Request.blank('/tar_works/acc/cont/?extract-archive=bad')
        req.method = 'PUT'
        req.headers['transfer-encoding'] = 'Chunked'
        req.environ['wsgi.input'] = open(
            os.path.join(self.testdir, 'tar_works.tar.gz'))
        t = self.bulk(req.environ, fake_start_response)
        self.assertEquals(t[0], "Unsupported archive format")

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
        resp_body = ''.join([i for i in app_iter])
        self.assertEquals(self.app.calls, 7)

    def test_bad_container(self):
        req = Request.blank('/invalid/', body='')
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertTrue('404 Not Found' in resp_body)

    def test_content_length_required(self):
        req = Request.blank('/create_cont_fail/acc/cont')
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertTrue('411 Length Required' in resp_body)

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
        req = Request.blank('/tar_works/acc/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Files Created'], 4)

    def test_extract_tar_fail_cont_401(self):
        self.build_tar()
        req = Request.blank('/unauth/acc/')
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertTrue('Response Status: 401 Unauthorized' in resp_body)

    def test_extract_tar_fail_obj_401(self):
        self.build_tar()
        req = Request.blank('/create_obj_unauth/acc/cont/')
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        self.assertTrue('Response Status: 401 Unauthorized' in resp_body)

    def test_extract_tar_fail_obj_name_len(self):
        self.build_tar()
        req = Request.blank('/tar_works/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Files Created'], 4)
        self.assertEquals(resp_data['Errors'][0][0],
                          '/tar_works/acc/cont/base_fails1/' + ('f' * 101))

    def test_extract_tar_fail_compress_type(self):
        self.build_tar()
        req = Request.blank('/tar_works/acc/cont/')
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, 'gz')
        self.assert_('400 Bad Request' in resp_body)
        self.assertEquals(self.app.calls, 0)

    def test_extract_tar_fail_max_file_name_length(self):
        self.build_tar()
        with patch.object(self.bulk, 'max_failed_extractions', 1):
            self.app.calls = 0
            req = Request.blank('/tar_works/acc/cont/',
                                headers={'Accept': 'application/json'})
            req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                          'tar_fails.tar'))
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, '')
            resp_data = json.loads(resp_body)
            self.assertEquals(self.app.calls, 5)
            self.assertEquals(resp_data['Errors'][0][0],
                              '/tar_works/acc/cont/base_fails1/' + ('f' * 101))

    @patch.object(bulk, 'MAX_FILE_SIZE', 4)
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
        resp_data = json.loads(resp_body)
        self.assert_(resp_data['Errors'][0][1].startswith('413'))

    def test_extract_tar_fail_max_cont(self):
        dir_tree = [{'sub_dir1': ['sub1_file1']},
                    {'sub_dir2': ['sub2_file1', 'sub2_file2']},
                    'f' * 101,
                    {'sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]
        self.build_tar(dir_tree)
        with patch.object(self.bulk, 'max_containers', 1):
            self.app.calls = 0
            body = open(os.path.join(self.testdir, 'tar_fails.tar')).read()
            req = Request.blank('/tar_works/acc/', body=body)
            req.headers['transfer-encoding'] = 'chunked'
            resp_body = self.handle_extract_and_iter(req, '')
            self.assertEquals(self.app.calls, 3)
            self.assert_('400 Bad Request' in resp_body)

    def test_extract_tar_fail_create_cont(self):
        dir_tree = [{'base_fails1': [
            {'sub_dir1': ['sub1_file1']},
            {'sub_dir2': ['sub2_file1', 'sub2_file2']},
            'f\xde',
            {'./sub_dir3': [{'sub4_dir1': 'sub4_file1'}]}]}]
        self.build_tar(dir_tree)
        req = Request.blank('/create_cont_fail/acc/cont/',
                            headers={'Accept': 'application/json'})
        req.environ['wsgi.input'] = open(os.path.join(self.testdir,
                                                      'tar_fails.tar'))
        req.headers['transfer-encoding'] = 'chunked'
        resp_body = self.handle_extract_and_iter(req, '')
        resp_data = json.loads(resp_body)
        self.assertEquals(self.app.calls, 4)
        self.assertEquals(len(resp_data['Errors']), 5)

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
            resp_data = json.loads(resp_body)
            self.assertEquals(self.app.calls, 0)
            self.assertEquals(len(resp_data['Errors']), 5)

    def test_get_response_body(self):
        txt_body = bulk.get_response_body(
            'bad_formay', {'hey': 'there'}, [['json > xml', '202 Accepted']])
        self.assert_('hey: there' in txt_body)
        xml_body = bulk.get_response_body(
            'text/xml', {'hey': 'there'}, [['json > xml', '202 Accepted']])
        self.assert_('&gt' in xml_body)


class TestDelete(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.bulk = bulk.filter_factory({})(self.app)

    def tearDown(self):
        self.app.calls = 0
        self.app.delete_paths = []

    def handle_delete_and_iter(self, req):
        resp_body = ''.join(self.bulk.handle_delete_iter(req))
        return resp_body

    def test_bulk_delete_works(self):
        req = Request.blank('/delete_works/AUTH_Acc', body='/c/f\n/c/f404',
                            headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEquals(
            self.app.delete_paths,
            ['/delete_works/AUTH_Acc/c/f', '/delete_works/AUTH_Acc/c/f404'])
        self.assertEquals(self.app.calls, 2)
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Deleted'], 1)
        self.assertEquals(resp_data['Number Not Found'], 1)

    def test_bulk_delete_bad_content_type(self):
        req = Request.blank('/delete_works/AUTH_Acc',
                            headers={'Accept': 'badformat'})

        req = Request.blank('/delete_works/AUTH_Acc',
                            headers={'Accept': 'application/json',
                                     'Content-Type': 'text/xml'})
        req.method = 'DELETE'
        req.environ['wsgi.input'] = StringIO('/c/f\n/c/f404')
        resp_body = self.handle_delete_and_iter(req)
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Response Status'], '406 Not Acceptable')

    def test_bulk_delete_call(self):
        def fake_start_response(*args, **kwargs):
            pass
        req = Request.blank('/delete_works/AUTH_Acc?bulk-delete')
        req.method = 'DELETE'
        req.headers['Transfer-Encoding'] = 'chunked'
        req.environ['wsgi.input'] = StringIO('/c/f')
        list(self.bulk(req.environ,
                       fake_start_response))  # iterate over whole resp
        self.assertEquals(
            self.app.delete_paths, ['/delete_works/AUTH_Acc/c/f'])
        self.assertEquals(self.app.calls, 1)

    def test_bulk_delete_get_objs(self):
        req = Request.blank('/delete_works/AUTH_Acc', body='1\r\n2\r\n')
        req.method = 'DELETE'
        with patch.object(self.bulk, 'max_deletes_per_request', 2):
            results = self.bulk.get_objs_to_delete(req)
            self.assertEquals(results, ['1\r', '2\r'])

        with patch.object(bulk, 'MAX_PATH_LENGTH', 2):
            results = []
            req.environ['wsgi.input'] = StringIO('1\n2\n3')
            results = self.bulk.get_objs_to_delete(req)
            self.assertEquals(results, ['1', '2', '3'])

        with patch.object(self.bulk, 'max_deletes_per_request', 9):
            with patch.object(bulk, 'MAX_PATH_LENGTH', 1):
                req_body = '\n'.join([str(i) for i in xrange(10)])
                req = Request.blank('/delete_works/AUTH_Acc', body=req_body)
                self.assertRaises(
                    HTTPException, self.bulk.get_objs_to_delete, req)

    def test_bulk_delete_works_extra_newlines_extra_quoting(self):
        req = Request.blank('/delete_works/AUTH_Acc',
                            body='/c/f\n\n\n/c/f404\n\n\n/c/%2525',
                            headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEquals(
            self.app.delete_paths,
            ['/delete_works/AUTH_Acc/c/f',
             '/delete_works/AUTH_Acc/c/f404',
             '/delete_works/AUTH_Acc/c/%25'])
        self.assertEquals(self.app.calls, 3)
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Deleted'], 2)
        self.assertEquals(resp_data['Number Not Found'], 1)

    def test_bulk_delete_too_many_newlines(self):
        req = Request.blank('/delete_works/AUTH_Acc')
        req.method = 'DELETE'
        data = '\n\n' * self.bulk.max_deletes_per_request
        req.environ['wsgi.input'] = StringIO(data)
        req.content_length = len(data)
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('413 Request Entity Too Large' in resp_body)

    def test_bulk_delete_works_unicode(self):
        body = (u'/c/ obj \u2661\r\n'.encode('utf8') +
                'c/ objbadutf8\r\n' +
                '/c/f\xdebadutf8\n')
        req = Request.blank('/delete_works/AUTH_Acc', body=body,
                            headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertEquals(
            self.app.delete_paths,
            ['/delete_works/AUTH_Acc/c/ obj \xe2\x99\xa1',
             '/delete_works/AUTH_Acc/c/ objbadutf8'])

        self.assertEquals(self.app.calls, 2)
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Deleted'], 1)
        self.assertEquals(len(resp_data['Errors']), 2)
        self.assertEquals(
            resp_data['Errors'],
            [[urllib.quote('/delete_works/AUTH_Acc/c/ objbadutf8'),
              '412 Precondition Failed'],
             [urllib.quote('/delete_works/AUTH_Acc/c/f\xdebadutf8'),
              '412 Precondition Failed']])

    def test_bulk_delete_no_body(self):
        req = Request.blank('/unauth/AUTH_acc/')
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('411 Length Required' in resp_body)

    def test_bulk_delete_no_files_in_body(self):
        req = Request.blank('/unauth/AUTH_acc/', body=' ')
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('400 Bad Request' in resp_body)

    def test_bulk_delete_unauth(self):
        req = Request.blank('/unauth/AUTH_acc/', body='/c/f\n')
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('401 Unauthorized' in resp_body)

    def test_bulk_delete_500_resp(self):
        req = Request.blank('/broke/AUTH_acc/', body='/c/f\n')
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('502 Bad Gateway' in resp_body)

    def test_bulk_delete_bad_path(self):
        req = Request.blank('/delete_cont_fail/')
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('404 Not Found' in resp_body)

    def test_bulk_delete_container_delete(self):
        req = Request.blank('/delete_cont_fail/AUTH_Acc', body='c\n',
                            headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Errors'][0][1], '409 Conflict')

    def test_bulk_delete_bad_file_too_long(self):
        req = Request.blank('/delete_works/AUTH_Acc',
                            headers={'Accept': 'application/json'})
        req.method = 'DELETE'
        data = '/c/f\nc/' + ('1' * bulk.MAX_PATH_LENGTH) + '\n/c/f'
        req.environ['wsgi.input'] = StringIO(data)
        req.headers['Transfer-Encoding'] = 'chunked'
        resp_body = self.handle_delete_and_iter(req)
        resp_data = json.loads(resp_body)
        self.assertEquals(resp_data['Number Deleted'], 2)
        self.assertEquals(resp_data['Errors'][0][1], '400 Bad Request')

    def test_bulk_delete_bad_file_over_twice_max_length(self):
        body = '/c/f\nc/' + ('123456' * bulk.MAX_PATH_LENGTH) + '\n'
        req = Request.blank('/delete_works/AUTH_Acc', body=body)
        req.method = 'DELETE'
        resp_body = self.handle_delete_and_iter(req)
        self.assertTrue('400 Bad Request' in resp_body)

if __name__ == '__main__':
    unittest.main()
