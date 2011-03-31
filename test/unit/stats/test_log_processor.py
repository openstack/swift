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
import Queue
import datetime
import hashlib
import pickle
import time

from swift.common import internal_proxy
from swift.stats import log_processor
from swift.common.exceptions import ChunkReadTimeout


class FakeUploadApp(object):
    def __init__(self, *args, **kwargs):
        pass

class DumbLogger(object):
    def __getattr__(self, n):
        return self.foo

    def foo(self, *a, **kw):
        pass

class DumbInternalProxy(object):
    def __init__(self, code=200, timeout=False, bad_compressed=False):
        self.code = code
        self.timeout = timeout
        self.bad_compressed = bad_compressed

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
        if object_name.endswith('.gz'):
            if self.bad_compressed:
                # invalid compressed data
                def data():
                    yield '\xff\xff\xff\xff\xff\xff\xff'
            else:
                # 'obj\ndata', compressed with gzip -9
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
                if self.timeout:
                    raise ChunkReadTimeout
                yield 'data'
        return self.code, data()

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

    def test_process_one_access_file_error(self):
        access_proxy_config = self.proxy_config.copy()
        access_proxy_config.update({
                        'log-processor-access': {
                            'source_filename_format':'%Y%m%d%H*',
                            'class_path':
                                'swift.stats.access_processor.AccessLogProcessor'
                        }})
        p = log_processor.LogProcessor(access_proxy_config, DumbLogger())
        p._internal_proxy = DumbInternalProxy(code=500)
        self.assertRaises(log_processor.BadFileDownload, p.process_one_file,
                          'access', 'a', 'c', 'o')

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

    def test_get_object_data_errors(self):
        p = log_processor.LogProcessor(self.proxy_config, DumbLogger())
        p._internal_proxy = DumbInternalProxy(code=500)
        result = p.get_object_data('a', 'c', 'o')
        self.assertRaises(log_processor.BadFileDownload, list, result)
        p._internal_proxy = DumbInternalProxy(bad_compressed=True)
        result = p.get_object_data('a', 'c', 'o.gz', True)
        self.assertRaises(log_processor.BadFileDownload, list, result)
        p._internal_proxy = DumbInternalProxy(timeout=True)
        result = p.get_object_data('a', 'c', 'o')
        self.assertRaises(log_processor.BadFileDownload, list, result)

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

    def test_collate_worker(self):
        try:
            log_processor.LogProcessor._internal_proxy = DumbInternalProxy()
            def get_object_data(*a,**kw):
                return [self.access_test_line]
            orig_get_object_data = log_processor.LogProcessor.get_object_data
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format':'%Y%m%d%H*',
                        'class_path':
                            'swift.stats.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            q_in = Queue.Queue()
            q_out = Queue.Queue()
            work_request = ('access', 'a','c','o')
            q_in.put(work_request)
            q_in.put(None)
            log_processor.collate_worker(processor_args, q_in, q_out)
            item, ret = q_out.get()
            self.assertEquals(item, work_request)
            expected = {('acct', '2010', '07', '09', '04'):
                        {('public', 'object', 'GET', '2xx'): 1,
                        ('public', 'bytes_out'): 95,
                        'marker_query': 0,
                        'format_query': 1,
                        'delimiter_query': 0,
                        'path_query': 0,
                        ('public', 'bytes_in'): 6,
                        'prefix_query': 0}}
            self.assertEquals(ret, expected)
        finally:
            log_processor.LogProcessor._internal_proxy = None
            log_processor.LogProcessor.get_object_data = orig_get_object_data

    def test_collate_worker_error(self):
        def get_object_data(*a,**kw):
            raise log_processor.BadFileDownload()
        orig_get_object_data = log_processor.LogProcessor.get_object_data
        try:
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format':'%Y%m%d%H*',
                        'class_path':
                            'swift.stats.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            q_in = Queue.Queue()
            q_out = Queue.Queue()
            work_request = ('access', 'a','c','o')
            q_in.put(work_request)
            q_in.put(None)
            log_processor.collate_worker(processor_args, q_in, q_out)
            item, ret = q_out.get()
            self.assertEquals(item, work_request)
            # these only work for Py2.7+
            #self.assertIsInstance(ret, log_processor.BadFileDownload)
            self.assertTrue(isinstance(ret, log_processor.BadFileDownload),
                            type(ret))
        finally:
            log_processor.LogProcessor.get_object_data = orig_get_object_data

    def test_multiprocess_collate(self):
        try:
            log_processor.LogProcessor._internal_proxy = DumbInternalProxy()
            def get_object_data(*a,**kw):
                return [self.access_test_line]
            orig_get_object_data = log_processor.LogProcessor.get_object_data
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format':'%Y%m%d%H*',
                        'class_path':
                            'swift.stats.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            item = ('access', 'a','c','o')
            logs_to_process = [item]
            results = log_processor.multiprocess_collate(processor_args,
                                                         logs_to_process,
                                                         1)
            results = list(results)
            expected = [(item, {('acct', '2010', '07', '09', '04'):
                        {('public', 'object', 'GET', '2xx'): 1,
                        ('public', 'bytes_out'): 95,
                        'marker_query': 0,
                        'format_query': 1,
                        'delimiter_query': 0,
                        'path_query': 0,
                        ('public', 'bytes_in'): 6,
                        'prefix_query': 0}})]
            self.assertEquals(results, expected)
        finally:
            log_processor.LogProcessor._internal_proxy = None
            log_processor.LogProcessor.get_object_data = orig_get_object_data

    def test_multiprocess_collate_errors(self):
        def get_object_data(*a,**kw):
            raise log_processor.BadFileDownload()
        orig_get_object_data = log_processor.LogProcessor.get_object_data
        try:
            log_processor.LogProcessor.get_object_data = get_object_data
            proxy_config = self.proxy_config.copy()
            proxy_config.update({
                    'log-processor-access': {
                        'source_filename_format':'%Y%m%d%H*',
                        'class_path':
                            'swift.stats.access_processor.AccessLogProcessor'
                    }})
            processor_args = (proxy_config, DumbLogger())
            item = ('access', 'a','c','o')
            logs_to_process = [item]
            results = log_processor.multiprocess_collate(processor_args,
                                                         logs_to_process,
                                                         1)
            results = list(results)
            expected = []
            self.assertEquals(results, expected)
        finally:
            log_processor.LogProcessor._internal_proxy = None
            log_processor.LogProcessor.get_object_data = orig_get_object_data

class TestLogProcessorDaemon(unittest.TestCase):

    def test_get_lookback_interval(self):
        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self, lookback_hours, lookback_window):
                self.lookback_hours = lookback_hours
                self.lookback_window = lookback_window

        try:
            d = datetime.datetime

            for x in [
                    [d(2011, 1, 1), 0, 0, None, None],
                    [d(2011, 1, 1), 120, 0, '2010122700', None],
                    [d(2011, 1, 1), 120, 24, '2010122700', '2010122800'],
                    [d(2010, 1, 2, 3, 4), 120, 48, '2009122803', '2009123003'],
                    [d(2009, 5, 6, 7, 8), 1200, 100, '2009031707', '2009032111'],
                    [d(2008, 9, 10, 11, 12), 3000, 1000, '2008050811', '2008061903'],
                ]:

                log_processor.now = lambda: x[0]

                d = MockLogProcessorDaemon(x[1], x[2])
                self.assertEquals((x[3], x[4]), d.get_lookback_interval())
        finally:
            log_processor.now = datetime.datetime.now

    def test_get_processed_files_list(self):
        class MockLogProcessor():
            def __init__(self, stream):
                self.stream = stream

            def get_object_data(self, *args, **kwargs):
                return self.stream

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self, stream):
                self.log_processor = MockLogProcessor(stream)
                self.log_processor_account = 'account'
                self.log_processor_container = 'container'
                self.processed_files_filename = 'filename'

        file_list = set(['a', 'b', 'c'])

        for s, l in [['', None],
                [pickle.dumps(set()).split('\n'), set()],
                [pickle.dumps(file_list).split('\n'), file_list],
            ]:

            self.assertEquals(l,
                MockLogProcessorDaemon(s).get_processed_files_list())

    def test_get_processed_files_list_bad_file_downloads(self):
        class MockLogProcessor():
            def __init__(self, status_code):
                self.err = log_processor.BadFileDownload(status_code)

            def get_object_data(self, *a, **k):
                raise self.err

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self, status_code):
                self.log_processor = MockLogProcessor(status_code)
                self.log_processor_account = 'account'
                self.log_processor_container = 'container'
                self.processed_files_filename = 'filename'

        for c, l in [[404, set()], [503, None], [None, None]]:
            self.assertEquals(l,
                MockLogProcessorDaemon(c).get_processed_files_list())

    def test_get_aggregate_data(self):
        # when run "for real"
        # the various keys/values in the input and output
        # dictionaries are often not simple strings
        # for testing we can use keys that are easier to work with

        processed_files = set()

        data_in = [
            ['file1', {
                'acct1_time1': {'field1': 1, 'field2': 2, 'field3': 3},
                'acct1_time2': {'field1': 4, 'field2': 5},
                'acct2_time1': {'field1': 6, 'field2': 7},
                'acct3_time3': {'field1': 8, 'field2': 9},
                }
            ],
            ['file2', {'acct1_time1': {'field1': 10}}],
        ]

        expected_data_out = {
            'acct1_time1': {'field1': 11, 'field2': 2, 'field3': 3},
            'acct1_time2': {'field1': 4, 'field2': 5},
            'acct2_time1': {'field1': 6, 'field2': 7},
            'acct3_time3': {'field1': 8, 'field2': 9},
        }

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self):
                pass

        d = MockLogProcessorDaemon()
        data_out = d.get_aggregate_data(processed_files, data_in)

        for k, v in expected_data_out.items():
            self.assertEquals(v, data_out[k])

        self.assertEquals(set(['file1', 'file2']), processed_files)

    def test_get_final_info(self):
        # when run "for real"
        # the various keys/values in the input and output
        # dictionaries are often not simple strings
        # for testing we can use keys/values that are easier to work with

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self):
                self._keylist_mapping = {
                    'out_field1':['field1', 'field2', 'field3'],
                    'out_field2':['field2', 'field3'],
                    'out_field3':['field3'],
                    'out_field4':'field4',
                    'out_field5':['field6', 'field7', 'field8'],
                    'out_field6':['field6'],
                    'out_field7':'field7',
                }

        data_in = {
            'acct1_time1': {'field1': 11, 'field2': 2, 'field3': 3,
                'field4': 8, 'field5': 11},
            'acct1_time2': {'field1': 4, 'field2': 5},
            'acct2_time1': {'field1': 6, 'field2': 7},
            'acct3_time3': {'field1': 8, 'field2': 9},
        }

        expected_data_out = {
            'acct1_time1': {'out_field1': 16, 'out_field2': 5,
                'out_field3': 3, 'out_field4': 8, 'out_field5': 0,
                'out_field6': 0, 'out_field7': 0,},
            'acct1_time2': {'out_field1': 9, 'out_field2': 5,
                'out_field3': 0, 'out_field4': 0, 'out_field5': 0,
                'out_field6': 0, 'out_field7': 0,},
            'acct2_time1': {'out_field1': 13, 'out_field2': 7,
                'out_field3': 0, 'out_field4': 0, 'out_field5': 0,
                'out_field6': 0, 'out_field7': 0,},
            'acct3_time3': {'out_field1': 17, 'out_field2': 9,
                'out_field3': 0, 'out_field4': 0, 'out_field5': 0,
                'out_field6': 0, 'out_field7': 0,},
        }

        self.assertEquals(expected_data_out,
            MockLogProcessorDaemon().get_final_info(data_in))

    def test_store_processed_files_list(self):
        class MockInternalProxy:
            def __init__(self, test, daemon, processed_files):
                self.test = test
                self.daemon = daemon
                self.processed_files = processed_files

            def upload_file(self, f, account, container, filename):
                self.test.assertEquals(self.processed_files,
                    pickle.loads(f.getvalue()))
                self.test.assertEquals(self.daemon.log_processor_account,
                    account)
                self.test.assertEquals(self.daemon.log_processor_container,
                    container)
                self.test.assertEquals(self.daemon.processed_files_filename,
                    filename)

        class MockLogProcessor:
            def __init__(self, test, daemon, processed_files):
                self.internal_proxy = MockInternalProxy(test, daemon,
                    processed_files)

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self, test, processed_files):
                self.log_processor = \
                    MockLogProcessor(test, self, processed_files)
                self.log_processor_account = 'account'
                self.log_processor_container = 'container'
                self.processed_files_filename = 'filename'

        processed_files = set(['a', 'b', 'c'])
        MockLogProcessorDaemon(self, processed_files).\
            store_processed_files_list(processed_files)

    def test_get_output(self):
        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self):
                self._keylist_mapping = {'a':None, 'b':None, 'c':None}

        data_in = {
            ('acct1', 2010, 1, 1, 0): {'a':1, 'b':2, 'c':3},
            ('acct1', 2010, 10, 10, 10): {'a':10, 'b':20, 'c':30},
            ('acct2', 2008, 3, 6, 9): {'a':8, 'b':9, 'c':12},
            ('acct3', 2005, 4, 8, 16): {'a':1, 'b':5, 'c':25},
        }

        expected_data_out = [
            ['data_ts', 'account', 'a', 'b', 'c'],
            ['2010/01/01 00:00:00', 'acct1', '1', '2', '3'],
            ['2010/10/10 10:00:00', 'acct1', '10', '20', '30'],
            ['2008/03/06 09:00:00', 'acct2', '8', '9', '12'],
            ['2005/04/08 16:00:00', 'acct3', '1', '5', '25'],
        ]

        data_out = MockLogProcessorDaemon().get_output(data_in)
        self.assertEquals(expected_data_out[0], data_out[0])

        for row in data_out[1:]:
            self.assert_(row in expected_data_out)

        for row in expected_data_out[1:]:
            self.assert_(row in data_out)

    def test_store_output(self):
        try:
            real_strftime = time.strftime
            mock_strftime_return = '2010/03/02/01/'
            def mock_strftime(format):
                self.assertEquals('%Y/%m/%d/%H/', format)
                return mock_strftime_return
            log_processor.time.strftime = mock_strftime

            data_in = [
                ['data_ts', 'account', 'a', 'b', 'c'],
                ['2010/10/10 10:00:00', 'acct1', '1', '2', '3'],
                ['2010/10/10 10:00:00', 'acct1', '10', '20', '30'],
                ['2008/03/06 09:00:00', 'acct2', '8', '9', '12'],
                ['2005/04/08 16:00:00', 'acct3', '1', '5', '25'],
            ]

            expected_output = '\n'.join([','.join(row) for row in data_in])
            h = hashlib.md5(expected_output).hexdigest()
            expected_filename = '%s%s.csv.gz' % (mock_strftime_return, h)

            class MockInternalProxy:
                def __init__(self, test, daemon, expected_filename,
                    expected_output):
                    self.test = test
                    self.daemon = daemon
                    self.expected_filename = expected_filename
                    self.expected_output = expected_output

                def upload_file(self, f, account, container, filename):
                    self.test.assertEquals(self.daemon.log_processor_account,
                        account)
                    self.test.assertEquals(self.daemon.log_processor_container,
                        container)
                    self.test.assertEquals(self.expected_filename, filename)
                    self.test.assertEquals(self.expected_output, f.getvalue())

            class MockLogProcessor:
                def __init__(self, test, daemon, expected_filename,
                    expected_output):
                    self.internal_proxy = MockInternalProxy(test, daemon,
                        expected_filename, expected_output)

            class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
                def __init__(self, test, expected_filename, expected_output):
                    self.log_processor = MockLogProcessor(test, self,
                        expected_filename, expected_output)
                    self.log_processor_account = 'account'
                    self.log_processor_container = 'container'
                    self.processed_files_filename = 'filename'

            MockLogProcessorDaemon(self, expected_filename, expected_output).\
                store_output(data_in)
        finally:
            log_processor.time.strftime = real_strftime

    def test_keylist_mapping(self):
        # Kind of lame test to see if the propery is both
        # generated by a particular method and cached properly.
        # The method that actually generates the mapping is
        # tested elsewhere.

        value_return = 'keylist_mapping'
        class MockLogProcessor:
            def __init__(self):
                self.call_count = 0

            def generate_keylist_mapping(self):
                self.call_count += 1
                return value_return

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self):
                self.log_processor = MockLogProcessor()
                self._keylist_mapping = None

        d = MockLogProcessorDaemon()
        self.assertEquals(value_return, d.keylist_mapping)
        self.assertEquals(value_return, d.keylist_mapping)
        self.assertEquals(1, d.log_processor.call_count)

    def test_process_logs(self):
        try:
            mock_logs_to_process = 'logs_to_process'
            mock_processed_files = 'processed_files'

            real_multiprocess_collate = log_processor.multiprocess_collate
            multiprocess_collate_return = 'multiprocess_collate_return'

            get_aggregate_data_return = 'get_aggregate_data_return'
            get_final_info_return = 'get_final_info_return'
            get_output_return = 'get_output_return'

            class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
                def __init__(self, test):
                    self.test = test
                    self.total_conf = 'total_conf'
                    self.logger = 'logger'
                    self.worker_count = 'worker_count'

                def get_aggregate_data(self, processed_files, results):
                    self.test.assertEquals(mock_processed_files, processed_files)
                    self.test.assertEquals(multiprocess_collate_return, results)
                    return get_aggregate_data_return

                def get_final_info(self, aggr_data):
                    self.test.assertEquals(get_aggregate_data_return, aggr_data)
                    return get_final_info_return

                def get_output(self, final_info):
                    self.test.assertEquals(get_final_info_return, final_info)
                    return get_output_return

            d = MockLogProcessorDaemon(self)

            def mock_multiprocess_collate(processor_args, logs_to_process,
                worker_count):
                self.assertEquals(d.total_conf, processor_args[0])
                self.assertEquals(d.logger, processor_args[1])

                self.assertEquals(mock_logs_to_process, logs_to_process)
                self.assertEquals(d.worker_count, worker_count)

                return multiprocess_collate_return

            log_processor.multiprocess_collate = mock_multiprocess_collate

            output = d.process_logs(mock_logs_to_process, mock_processed_files)
            self.assertEquals(get_output_return, output)
        finally:
            log_processor.multiprocess_collate = real_multiprocess_collate

    def test_run_once_get_processed_files_list_returns_none(self):
        class MockLogProcessor:
            def get_data_list(self, lookback_start, lookback_end,
                processed_files):
                raise unittest.TestCase.failureException, \
                    'Method should not be called'

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self):
                self.logger = DumbLogger()
                self.log_processor = MockLogProcessor()

            def get_lookback_interval(self):
                return None, None

            def get_processed_files_list(self):
                return None

        MockLogProcessorDaemon().run_once()

    def test_run_once_no_logs_to_process(self):
        class MockLogProcessor():
            def __init__(self, daemon, test):
                self.daemon = daemon
                self.test = test

            def get_data_list(self, lookback_start, lookback_end,
                processed_files):
                self.test.assertEquals(self.daemon.lookback_start,
                    lookback_start)
                self.test.assertEquals(self.daemon.lookback_end,
                    lookback_end)
                self.test.assertEquals(self.daemon.processed_files,
                    processed_files)
                return []

        class MockLogProcessorDaemon(log_processor.LogProcessorDaemon):
            def __init__(self, test):
                self.logger = DumbLogger()
                self.log_processor = MockLogProcessor(self, test)
                self.lookback_start = 'lookback_start'
                self.lookback_end = 'lookback_end'
                self.processed_files = ['a', 'b', 'c']

            def get_lookback_interval(self):
                return self.lookback_start, self.lookback_end

            def get_processed_files_list(self):
                return self.processed_files

            def process_logs(logs_to_process, processed_files):
                raise unittest.TestCase.failureException, \
                    'Method should not be called'

        MockLogProcessorDaemon(self).run_once()
