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
import json
import shutil
import tempfile
import unittest

from six import BytesIO

from swift import gettext_ as _
from swift.common.swob import Request, Response

try:
    from swift.common.middleware import xprofile
    from swift.common.middleware.xprofile import ProfileMiddleware
    from swift.common.middleware.x_profile.exceptions import (
        MethodNotAllowed, NotFoundException, ODFLIBNotInstalled,
        PLOTLIBNotInstalled)
    from swift.common.middleware.x_profile.html_viewer import (
        HTMLViewer, PLOTLIB_INSTALLED)
    from swift.common.middleware.x_profile.profile_model import (
        ODFLIB_INSTALLED, ProfileLog, Stats2)
except ImportError:
    xprofile = None


class FakeApp(object):

    def __call__(self, env, start_response):
        req = Request(env)
        return Response(request=req, body='FAKE APP')(
            env, start_response)


class TestXProfile(unittest.TestCase):

    @unittest.skipIf(xprofile is None, "can't import xprofile")
    def test_get_profiler(self):
        self.assertTrue(xprofile.get_profiler('cProfile') is not None)
        self.assertTrue(xprofile.get_profiler('eventlet.green.profile')
                        is not None)


class TestProfilers(unittest.TestCase):

    @unittest.skipIf(xprofile is None, "can't import xprofile")
    def setUp(self):
        self.profilers = [xprofile.get_profiler('cProfile'),
                          xprofile.get_profiler('eventlet.green.profile')]

    def fake_func(self, *args, **kw):
        return len(args) + len(kw)

    def test_runcall(self):
        for p in self.profilers:
            v = p.runcall(self.fake_func, 'one', 'two', {'key1': 'value1'})
            self.assertEqual(v, 3)

    def test_runctx(self):
        for p in self.profilers:
            p.runctx('import os;os.getcwd();', globals(), locals())
            p.snapshot_stats()
            self.assertTrue(p.stats is not None)
            self.assertGreater(len(p.stats.keys()), 0)


class TestProfileMiddleware(unittest.TestCase):

    @unittest.skipIf(xprofile is None, "can't import xprofile")
    def setUp(self):
        self.got_statuses = []
        self.app = ProfileMiddleware(FakeApp, {})
        self.tempdir = os.path.dirname(self.app.log_filename_prefix)
        self.pids = ['123', '456', str(os.getpid())]
        profiler = xprofile.get_profiler('eventlet.green.profile')
        for pid in self.pids:
            path = self.app.log_filename_prefix + pid
            profiler.runctx('import os;os.getcwd();', globals(), locals())
            profiler.dump_stats(path)
            profiler.runctx('import os;os.getcwd();', globals(), locals())
            profiler.dump_stats(path + '.tmp')

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def get_app(self, app, global_conf, **local_conf):
        factory = xprofile.filter_factory(global_conf, **local_conf)
        return factory(app)

    def start_response(self, status, headers):
        self.got_statuses = [status]
        self.headers = headers

    def test_combine_body_qs(self):
        body = (b"profile=all&sort=time&limit=-1&fulldirs=1"
                b"&nfl_filter=__call__&query=query&metric=nc&format=default")
        wsgi_input = BytesIO(body)
        environ = {'REQUEST_METHOD': 'GET',
                   'QUERY_STRING': 'profile=all&format=json',
                   'wsgi.input': wsgi_input}
        req = Request.blank('/__profile__/', environ=environ)
        query_dict = self.app._combine_body_qs(req)
        self.assertEqual(query_dict['profile'], ['all'])
        self.assertEqual(query_dict['sort'], ['time'])
        self.assertEqual(query_dict['limit'], ['-1'])
        self.assertEqual(query_dict['fulldirs'], ['1'])
        self.assertEqual(query_dict['nfl_filter'], ['__call__'])
        self.assertEqual(query_dict['query'], ['query'])
        self.assertEqual(query_dict['metric'], ['nc'])
        self.assertEqual(query_dict['format'], ['default'])

    def test_call(self):
        body = b"sort=time&limit=-1&fulldirs=1&nfl_filter=&metric=nc"
        wsgi_input = BytesIO(body + b'&query=query')
        environ = {'HTTP_HOST': 'localhost:8080',
                   'PATH_INFO': '/__profile__',
                   'REQUEST_METHOD': 'GET',
                   'QUERY_STRING': 'profile=all&format=json',
                   'wsgi.input': wsgi_input}
        resp = self.app(environ, self.start_response)
        self.assertTrue(resp[0].find(b'<html>') > 0, resp)
        self.assertEqual(self.got_statuses, ['200 OK'])
        self.assertEqual(self.headers, [('content-type', 'text/html')])
        wsgi_input = BytesIO(body + b'&plot=plot')
        environ['wsgi.input'] = wsgi_input
        if PLOTLIB_INSTALLED:
            resp = self.app(environ, self.start_response)
            self.assertEqual(self.got_statuses, ['200 OK'])
            self.assertEqual(self.headers, [('content-type', 'image/jpg')])
        else:
            resp = self.app(environ, self.start_response)
            self.assertEqual(self.got_statuses, ['500 Internal Server Error'])
        wsgi_input = BytesIO(body + b'&download=download&format=default')
        environ['wsgi.input'] = wsgi_input
        resp = self.app(environ, self.start_response)
        self.assertEqual(self.headers, [('content-type',
                                         HTMLViewer.format_dict['default'])])
        wsgi_input = BytesIO(body + b'&download=download&format=json')
        environ['wsgi.input'] = wsgi_input
        resp = self.app(environ, self.start_response)
        self.assertTrue(self.headers == [('content-type',
                                          HTMLViewer.format_dict['json'])])
        env2 = environ.copy()
        env2['REQUEST_METHOD'] = 'DELETE'
        resp = self.app(env2, self.start_response)
        self.assertEqual(self.got_statuses, ['405 Method Not Allowed'], resp)

        # use a totally bogus profile identifier
        wsgi_input = BytesIO(body + b'&profile=ABC&download=download')
        environ['wsgi.input'] = wsgi_input
        resp = self.app(environ, self.start_response)
        self.assertEqual(self.got_statuses, ['404 Not Found'], resp)

        wsgi_input = BytesIO(body + b'&download=download&format=ods')
        environ['wsgi.input'] = wsgi_input
        resp = self.app(environ, self.start_response)
        if ODFLIB_INSTALLED:
            self.assertEqual(self.headers, [('content-type',
                                             HTMLViewer.format_dict['ods'])])
        else:
            self.assertEqual(self.got_statuses, ['500 Internal Server Error'])

    def test_dump_checkpoint(self):
        self.app.dump_checkpoint()
        self.assertTrue(self.app.last_dump_at is not None)

    def test_renew_profile(self):
        old_profiler = self.app.profiler
        self.app.renew_profile()
        new_profiler = self.app.profiler
        self.assertTrue(old_profiler != new_profiler)


class Test_profile_log(unittest.TestCase):

    @unittest.skipIf(xprofile is None, "can't import xprofile")
    def setUp(self):
        self.dir1 = tempfile.mkdtemp()
        self.log_filename_prefix1 = self.dir1 + '/unittest.profile'
        self.profile_log1 = ProfileLog(self.log_filename_prefix1, False)
        self.pids1 = ['123', '456', str(os.getpid())]
        profiler1 = xprofile.get_profiler('eventlet.green.profile')
        for pid in self.pids1:
            profiler1.runctx('import os;os.getcwd();', globals(), locals())
            self.profile_log1.dump_profile(profiler1, pid)

        self.dir2 = tempfile.mkdtemp()
        self.log_filename_prefix2 = self.dir2 + '/unittest.profile'
        self.profile_log2 = ProfileLog(self.log_filename_prefix2, True)
        self.pids2 = ['321', '654', str(os.getpid())]
        profiler2 = xprofile.get_profiler('eventlet.green.profile')
        for pid in self.pids2:
            profiler2.runctx('import os;os.getcwd();', globals(), locals())
            self.profile_log2.dump_profile(profiler2, pid)

    def tearDown(self):
        self.profile_log1.clear('all')
        self.profile_log2.clear('all')
        shutil.rmtree(self.dir1, ignore_errors=True)
        shutil.rmtree(self.dir2, ignore_errors=True)

    def test_get_all_pids(self):
        self.assertEqual(self.profile_log1.get_all_pids(),
                         sorted(self.pids1, reverse=True))
        for pid in self.profile_log2.get_all_pids():
            self.assertTrue(pid.split('-')[0] in self.pids2)

    def test_clear(self):
        self.profile_log1.clear('123')
        self.assertFalse(os.path.exists(self.log_filename_prefix1 + '123'))
        self.profile_log1.clear('current')
        self.assertFalse(os.path.exists(self.log_filename_prefix1 +
                                        str(os.getpid())))
        self.profile_log1.clear('all')
        for pid in self.pids1:
            self.assertFalse(os.path.exists(self.log_filename_prefix1 + pid))

        self.profile_log2.clear('321')
        self.assertFalse(os.path.exists(self.log_filename_prefix2 + '321'))
        self.profile_log2.clear('current')
        self.assertFalse(os.path.exists(self.log_filename_prefix2 +
                                        str(os.getpid())))
        self.profile_log2.clear('all')
        for pid in self.pids2:
            self.assertFalse(os.path.exists(self.log_filename_prefix2 + pid))

    def test_get_logfiles(self):
        log_files = self.profile_log1.get_logfiles('all')
        self.assertEqual(len(log_files), 3)
        self.assertEqual(len(log_files), len(self.pids1))
        log_files = self.profile_log1.get_logfiles('current')
        self.assertEqual(len(log_files), 1)
        self.assertEqual(log_files, [self.log_filename_prefix1
                         + str(os.getpid())])
        log_files = self.profile_log1.get_logfiles(self.pids1[0])
        self.assertEqual(len(log_files), 1)
        self.assertEqual(log_files, [self.log_filename_prefix1
                         + self.pids1[0]])
        log_files = self.profile_log2.get_logfiles('all')
        self.assertEqual(len(log_files), 3)
        self.assertEqual(len(log_files), len(self.pids2))
        log_files = self.profile_log2.get_logfiles('current')
        self.assertEqual(len(log_files), 1)
        self.assertTrue(log_files[0].find(self.log_filename_prefix2 +
                                          str(os.getpid())) > -1)
        log_files = self.profile_log2.get_logfiles(self.pids2[0])
        self.assertEqual(len(log_files), 1)
        self.assertTrue(log_files[0].find(self.log_filename_prefix2 +
                                          self.pids2[0]) > -1)

    def test_dump_profile(self):
        prof = xprofile.get_profiler('eventlet.green.profile')
        prof.runctx('import os;os.getcwd();', globals(), locals())
        prof.create_stats()
        pfn = self.profile_log1.dump_profile(prof, os.getpid())
        self.assertTrue(os.path.exists(pfn))
        os.remove(pfn)
        pfn = self.profile_log2.dump_profile(prof, os.getpid())
        self.assertTrue(os.path.exists(pfn))
        os.remove(pfn)


class Test_html_viewer(unittest.TestCase):

    @unittest.skipIf(xprofile is None, "can't import xprofile")
    def setUp(self):
        self.app = ProfileMiddleware(FakeApp, {})
        self.log_files = []
        self.tempdir = tempfile.mkdtemp()
        self.log_filename_prefix = self.tempdir + '/unittest.profile'
        self.profile_log = ProfileLog(self.log_filename_prefix, False)
        self.pids = ['123', '456', str(os.getpid())]
        profiler = xprofile.get_profiler('eventlet.green.profile')
        for pid in self.pids:
            profiler.runctx('import os;os.getcwd();', globals(), locals())
            self.log_files.append(self.profile_log.dump_profile(profiler, pid))
        self.viewer = HTMLViewer('__profile__', 'eventlet.green.profile',
                                 self.profile_log)
        body = (b"profile=123&profile=456&sort=time&sort=nc&limit=10"
                b"&fulldirs=1&nfl_filter=getcwd&query=query&metric=nc")
        wsgi_input = BytesIO(body)
        environ = {'REQUEST_METHOD': 'GET',
                   'QUERY_STRING': 'profile=all',
                   'wsgi.input': wsgi_input}
        req = Request.blank('/__profile__/', environ=environ)
        self.query_dict = self.app._combine_body_qs(req)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def fake_call_back(self):
        pass

    def test_get_param(self):
        query_dict = self.query_dict
        get_param = self.viewer._get_param
        self.assertEqual(get_param(query_dict, 'profile', 'current', True),
                         ['123', '456'])
        self.assertEqual(get_param(query_dict, 'profile', 'current'), '123')
        self.assertEqual(get_param(query_dict, 'sort', 'time'), 'time')
        self.assertEqual(get_param(query_dict, 'sort', 'time', True),
                         ['time', 'nc'])
        self.assertEqual(get_param(query_dict, 'limit', -1), 10)
        self.assertEqual(get_param(query_dict, 'fulldirs', '0'), '1')
        self.assertEqual(get_param(query_dict, 'nfl_filter', ''), 'getcwd')
        self.assertEqual(get_param(query_dict, 'query', ''), 'query')
        self.assertEqual(get_param(query_dict, 'metric', 'time'), 'nc')
        self.assertEqual(get_param(query_dict, 'format', 'default'), 'default')

    def test_render(self):
        url = 'http://localhost:8080/__profile__'
        path_entries = ['/__profile__'.split('/'),
                        '/__profile__/'.split('/'),
                        '/__profile__/123'.split('/'),
                        '/__profile__/123/'.split('/'),
                        '/__profile__/123/:0(getcwd)'.split('/'),
                        '/__profile__/all'.split('/'),
                        '/__profile__/all/'.split('/'),
                        '/__profile__/all/:0(getcwd)'.split('/'),
                        '/__profile__/current'.split('/'),
                        '/__profile__/current/'.split('/'),
                        '/__profile__/current/:0(getcwd)'.split('/')]

        content, headers = self.viewer.render(url, 'GET', path_entries[0],
                                              self.query_dict, None)
        self.assertTrue(content is not None)
        self.assertEqual(headers, [('content-type', 'text/html')])

        content, headers = self.viewer.render(url, 'POST', path_entries[0],
                                              self.query_dict, None)
        self.assertTrue(content is not None)
        self.assertEqual(headers, [('content-type', 'text/html')])

        plot_dict = self.query_dict.copy()
        plot_dict['plot'] = ['plot']
        if PLOTLIB_INSTALLED:
            content, headers = self.viewer.render(url, 'POST', path_entries[0],
                                                  plot_dict, None)
            self.assertEqual(headers, [('content-type', 'image/jpg')])
        else:
            self.assertRaises(PLOTLIBNotInstalled, self.viewer.render,
                              url, 'POST', path_entries[0], plot_dict, None)

        clear_dict = self.query_dict.copy()
        clear_dict['clear'] = ['clear']
        del clear_dict['query']
        clear_dict['profile'] = ['xxx']
        content, headers = self.viewer.render(url, 'POST', path_entries[0],
                                              clear_dict, None)
        self.assertEqual(headers, [('content-type', 'text/html')])

        download_dict = self.query_dict.copy()
        download_dict['download'] = ['download']
        content, headers = self.viewer.render(url, 'POST', path_entries[0],
                                              download_dict, None)
        self.assertTrue(headers == [('content-type',
                                    self.viewer.format_dict['default'])])

        content, headers = self.viewer.render(url, 'GET', path_entries[1],
                                              self.query_dict, None)
        self.assertTrue(isinstance(json.loads(content), dict))

        for method in ['HEAD', 'PUT', 'DELETE', 'XYZMethod']:
            self.assertRaises(MethodNotAllowed, self.viewer.render, url,
                              method, path_entries[10], self.query_dict, None)

        for entry in path_entries[2:]:
            download_dict['format'] = 'default'
            content, headers = self.viewer.render(url, 'GET', entry,
                                                  download_dict, None)
            self.assertTrue(
                ('content-type', self.viewer.format_dict['default'])
                in headers, entry)
            download_dict['format'] = 'json'
            content, headers = self.viewer.render(url, 'GET', entry,
                                                  download_dict, None)
            self.assertTrue(isinstance(json.loads(content), dict))

    def test_index(self):
        content, headers = self.viewer.index_page(self.log_files[0:1],
                                                  profile_id='current')
        self.assertTrue(content.find('<html>') > -1)
        self.assertTrue(headers == [('content-type', 'text/html')])

    def test_index_all(self):
        content, headers = self.viewer.index_page(self.log_files,
                                                  profile_id='all')
        for f in self.log_files:
            self.assertTrue(content.find(f) > 0, content)
            self.assertTrue(headers == [('content-type', 'text/html')])

    def test_download(self):
        content, headers = self.viewer.download(self.log_files)
        self.assertTrue(content is not None)
        self.assertEqual(headers, [('content-type',
                                    self.viewer.format_dict['default'])])
        content, headers = self.viewer.download(self.log_files, sort='calls',
                                                limit=10, nfl_filter='os')
        self.assertTrue(content is not None)
        self.assertEqual(headers, [('content-type',
                                    self.viewer.format_dict['default'])])
        content, headers = self.viewer.download(self.log_files,
                                                output_format='default')
        self.assertEqual(headers, [('content-type',
                                    self.viewer.format_dict['default'])])
        content, headers = self.viewer.download(self.log_files,
                                                output_format='json')
        self.assertTrue(isinstance(json.loads(content), dict))
        self.assertEqual(headers, [('content-type',
                                    self.viewer.format_dict['json'])])
        content, headers = self.viewer.download(self.log_files,
                                                output_format='csv')
        self.assertEqual(headers, [('content-type',
                                    self.viewer.format_dict['csv'])])
        if ODFLIB_INSTALLED:
            content, headers = self.viewer.download(self.log_files,
                                                    output_format='ods')
            self.assertEqual(headers, [('content-type',
                                        self.viewer.format_dict['ods'])])
        else:
            self.assertRaises(ODFLIBNotInstalled, self.viewer.download,
                              self.log_files, output_format='ods')
        content, headers = self.viewer.download(self.log_files,
                                                nfl_filter=__file__,
                                                output_format='python')
        self.assertEqual(headers, [('content-type',
                                    self.viewer.format_dict['python'])])

    def test_plot(self):
        if PLOTLIB_INSTALLED:
            content, headers = self.viewer.plot(self.log_files)
            self.assertTrue(content is not None)
            self.assertEqual(headers, [('content-type', 'image/jpg')])
            self.assertRaises(NotFoundException, self.viewer.plot, [])
        else:
            self.assertRaises(PLOTLIBNotInstalled, self.viewer.plot,
                              self.log_files)

    def test_format_source_code(self):
        osfile = os.__file__.rstrip('c')
        nfl_os = '%s:%d(%s)' % (osfile, 136, 'makedirs')
        self.assertIn('makedirs', self.viewer.format_source_code(nfl_os))
        self.assertNotIn('makedirsXYZ', self.viewer.format_source_code(nfl_os))
        nfl_illegal = '%sc:136(makedirs)' % osfile
        self.assertIn(_('The file type are forbidden to access!'),
                      self.viewer.format_source_code(nfl_illegal))
        nfl_not_exist = '%s.py:136(makedirs)' % osfile
        expected_msg = _('Can not access the file %s.py.') % osfile
        self.assertIn(expected_msg,
                      self.viewer.format_source_code(nfl_not_exist))


class TestStats2(unittest.TestCase):

    @unittest.skipIf(xprofile is None, "can't import xprofile")
    def setUp(self):
        self.profile_file = tempfile.mktemp('profile', 'unittest')
        self.profilers = [xprofile.get_profiler('cProfile'),
                          xprofile.get_profiler('eventlet.green.profile')]
        for p in self.profilers:
            p.runctx('import os;os.getcwd();', globals(), locals())
            p.dump_stats(self.profile_file)
            self.stats2 = Stats2(self.profile_file)
            self.selections = [['getcwd'], ['getcwd', -1],
                               ['getcwd', -10], ['getcwd', 0.1]]

    def tearDown(self):
        os.remove(self.profile_file)

    def test_func_to_dict(self):
        func = ['profile.py', 100, '__call__']
        self.assertEqual({'module': 'profile.py', 'line': 100, 'function':
                          '__call__'}, self.stats2.func_to_dict(func))
        func = ['', 0, '__call__']
        self.assertEqual({'module': '', 'line': 0, 'function':
                          '__call__'}, self.stats2.func_to_dict(func))

    def test_to_json(self):
        for selection in self.selections:
            js = self.stats2.to_json(selection)
            self.assertTrue(isinstance(json.loads(js), dict))
            self.assertTrue(json.loads(js)['stats'] is not None)
            self.assertTrue(json.loads(js)['stats'][0] is not None)

    def test_to_ods(self):
        if ODFLIB_INSTALLED:
            for selection in self.selections:
                self.assertTrue(self.stats2.to_ods(selection) is not None)

    def test_to_csv(self):
        for selection in self.selections:
            self.assertTrue(self.stats2.to_csv(selection) is not None)
            self.assertTrue('function calls' in self.stats2.to_csv(selection))


if __name__ == '__main__':
    unittest.main()
