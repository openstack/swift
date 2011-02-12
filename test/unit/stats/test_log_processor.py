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

import unittest
from test.unit import tmpfile

from swift.common import internal_proxy
from swift.stats import log_processor


class FakeUploadApp(object):
    def __init__(self, *args, **kwargs):
        pass


class DumbLogger(object):
    def __getattr__(self, n):
        return self.foo

    def foo(self, *a, **kw):
        pass

class DumbInternalProxy(object):
    def get_container_list(self, account, container, marker=None,
                           end_marker=None):
        n = '2010/03/14/13/obj1'
        if marker is None or n > marker:
            if end_marker:
                if n <= end_marker:
                    return [{'name': n}]
                else:
                    return []
            return [{'name': n}]
        return []

    def get_object(self, account, container, object_name):
        code = 200
        if object_name.endswith('.gz'):
            # same data as below, compressed with gzip -9
            def data():
                yield '\x1f\x8b\x08'
                yield '\x08"\xd79L'
                yield '\x02\x03te'
                yield 'st\x00\xcbO'
                yield '\xca\xe2JI,I'
                yield '\xe4\x02\x00O\xff'
                yield '\xa3Y\t\x00\x00\x00'
        else:
            def data():
                yield 'obj\n'
                yield 'data'
        return code, data()

class TestLogProcessor(unittest.TestCase):
    
    access_test_line = 'Jul  9 04:14:30 saio proxy-server 1.2.3.4 4.5.6.7 '\
                    '09/Jul/2010/04/14/30 GET '\
                    '/v1/acct/foo/bar?format=json&foo HTTP/1.0 200 - '\
                    'curl tk4e350daf-9338-4cc6-aabb-090e49babfbd '\
                    '6 95 - txfa431231-7f07-42fd-8fc7-7da9d8cc1f90 - 0.0262'
    stats_test_line = 'account,1,2,3'
    proxy_config = {'log-processor': {
                      
                    }
                   }

    def test_lazy_load_internal_proxy(self):
        # stub out internal_proxy's upload_app
        internal_proxy.BaseApplication = FakeUploadApp
        dummy_proxy_config = """[app:proxy-server]
use = egg:swift#proxy
"""
        with tmpfile(dummy_proxy_config) as proxy_config_file:
            conf = {'log-processor': {
                    'proxy_server_conf': proxy_config_file,
                }
            }
            p = log_processor.LogProcessor(conf, DumbLogger())
            self.assert_(isinstance(p._internal_proxy,
                                    None.__class__))
            self.assert_(isinstance(p.internal_proxy,
                                    log_processor.InternalProxy))
            self.assertEquals(p.internal_proxy, p._internal_proxy)

        # reset FakeUploadApp
        reload(internal_proxy)

    def test_access_log_line_parser(self):
        access_proxy_config = self.proxy_config.copy()
        access_proxy_config.update({
                        'log-processor-access': {
                            'source_filename_format':'%Y%m%d%H*',
                            'class_path':
                                'swift.stats.access_processor.AccessLogProcessor'
                        }})
        p = log_processor.LogProcessor(access_proxy_config, DumbLogger())
        result = p.plugins['access']['instance'].log_line_parser(self.access_test_line)
        self.assertEquals(result, {'code': 200,
           'processing_time': '0.0262',
           'auth_token': 'tk4e350daf-9338-4cc6-aabb-090e49babfbd',
           'month': '07',
           'second': '30',
           'year': '2010',
           'query': 'format=json&foo',
           'tz': '+0000',
           'http_version': 'HTTP/1.0',
           'object_name': 'bar',
           'etag': '-',
           'foo': 1,
           'method': 'GET',
           'trans_id': 'txfa431231-7f07-42fd-8fc7-7da9d8cc1f90',
           'client_ip': '1.2.3.4',
           'format': 1,
           'bytes_out': 95,
           'container_name': 'foo',
           'day': '09',
           'minute': '14',
           'account': 'acct',
           'hour': '04',
           'referrer': '-',
           'request': '/v1/acct/foo/bar',
           'user_agent': 'curl',
           'bytes_in': 6,
           'lb_ip': '4.5.6.7'})

    def test_process_one_access_file(self):
        access_proxy_config = self.proxy_config.copy()
        access_proxy_config.update({
                        'log-processor-access': {
                            'source_filename_format':'%Y%m%d%H*',
                            'class_path':
                                'swift.stats.access_processor.AccessLogProcessor'
                        }})
        p = log_processor.LogProcessor(access_proxy_config, DumbLogger())
        def get_object_data(*a, **kw):
            return [self.access_test_line]
        p.get_object_data = get_object_data
        result = p.process_one_file('access', 'a', 'c', 'o')
        expected = {('acct', '2010', '07', '09', '04'):
                    {('public', 'object', 'GET', '2xx'): 1,
                    ('public', 'bytes_out'): 95,
                    'marker_query': 0,
                    'format_query': 1,
                    'delimiter_query': 0,
                    'path_query': 0,
                    ('public', 'bytes_in'): 6,
                    'prefix_query': 0}}
        self.assertEquals(result, expected)

    def test_get_container_listing(self):
        p = log_processor.LogProcessor(self.proxy_config, DumbLogger())
        p._internal_proxy = DumbInternalProxy()
        result = p.get_container_listing('a', 'foo')
        expected = ['2010/03/14/13/obj1']
        self.assertEquals(result, expected)
        result = p.get_container_listing('a', 'foo', listing_filter=expected)
        expected = []
        self.assertEquals(result, expected)
        result = p.get_container_listing('a', 'foo', start_date='2010031412',
                                            end_date='2010031414')
        expected = ['2010/03/14/13/obj1']
        self.assertEquals(result, expected)
        result = p.get_container_listing('a', 'foo', start_date='2010031414')
        expected = []
        self.assertEquals(result, expected)
        result = p.get_container_listing('a', 'foo', start_date='2010031410',
                                            end_date='2010031412')
        expected = []
        self.assertEquals(result, expected)
        result = p.get_container_listing('a', 'foo', start_date='2010031412',
                                            end_date='2010031413')
        expected = ['2010/03/14/13/obj1']
        self.assertEquals(result, expected)

    def test_get_object_data(self):
        p = log_processor.LogProcessor(self.proxy_config, DumbLogger())
        p._internal_proxy = DumbInternalProxy()
        result = list(p.get_object_data('a', 'c', 'o', False))
        expected = ['obj','data']
        self.assertEquals(result, expected)
        result = list(p.get_object_data('a', 'c', 'o.gz', True))
        self.assertEquals(result, expected)

    def test_get_stat_totals(self):
        stats_proxy_config = self.proxy_config.copy()
        stats_proxy_config.update({
                        'log-processor-stats': {
                            'class_path':
                                'swift.stats.stats_processor.StatsLogProcessor'
                        }})
        p = log_processor.LogProcessor(stats_proxy_config, DumbLogger())
        p._internal_proxy = DumbInternalProxy()
        def get_object_data(*a,**kw):
            return [self.stats_test_line]
        p.get_object_data = get_object_data
        result = p.process_one_file('stats', 'a', 'c', 'y/m/d/h/o')
        expected = {('account', 'y', 'm', 'd', 'h'):
                    {'replica_count': 1,
                    'object_count': 2,
                    'container_count': 1,
                    'bytes_used': 3}}
        self.assertEquals(result, expected)

    def test_generate_keylist_mapping(self):
        p = log_processor.LogProcessor(self.proxy_config, DumbLogger())
        result = p.generate_keylist_mapping()
        expected = {}
        self.assertEquals(result, expected)

    def test_generate_keylist_mapping_with_dummy_plugins(self):
        class Plugin1(object):
            def keylist_mapping(self):
                return {'a': 'b', 'c': 'd', 'e': ['f', 'g']}
        class Plugin2(object):
            def keylist_mapping(self):
                return {'a': '1', 'e': '2', 'h': '3'}
        p = log_processor.LogProcessor(self.proxy_config, DumbLogger())
        p.plugins['plugin1'] = {'instance': Plugin1()}
        p.plugins['plugin2'] = {'instance': Plugin2()}
        result = p.generate_keylist_mapping()
        expected = {'a': set(['b', '1']), 'c': 'd', 'e': set(['2', 'f', 'g']),
                    'h': '3'}
        self.assertEquals(result, expected)

    def test_access_keylist_mapping_format(self):
        proxy_config = self.proxy_config.copy()
        proxy_config.update({
                        'log-processor-access': {
                            'source_filename_format':'%Y%m%d%H*',
                            'class_path':
                                'swift.stats.access_processor.AccessLogProcessor'
                        }})
        p = log_processor.LogProcessor(proxy_config, DumbLogger())
        mapping = p.generate_keylist_mapping()
        for k, v in mapping.items():
            # these only work for Py2.7+
            #self.assertIsInstance(k, str)
            self.assertTrue(isinstance(k, str), type(k))

    def test_stats_keylist_mapping_format(self):
        proxy_config = self.proxy_config.copy()
        proxy_config.update({
                        'log-processor-stats': {
                            'class_path':
                                'swift.stats.stats_processor.StatsLogProcessor'
                        }})
        p = log_processor.LogProcessor(proxy_config, DumbLogger())
        mapping = p.generate_keylist_mapping()
        for k, v in mapping.items():
            # these only work for Py2.7+
            #self.assertIsInstance(k, str)
            self.assertTrue(isinstance(k, str), type(k))
