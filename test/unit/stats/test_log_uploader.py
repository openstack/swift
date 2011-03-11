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

# TODO: More tests

import unittest
import os
from datetime import datetime
from tempfile import mkdtemp
from shutil import rmtree
from functools import partial
from collections import defaultdict
import random
import string

from test.unit import temptree
from swift.stats import log_uploader

import logging
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger()

DEFAULT_GLOB = '%Y%m%d%H'

COMPRESSED_DATA = '\x1f\x8b\x08\x08\x87\xa5zM\x02\xffdata\x00KI,I\x04\x00c' \
        '\xf3\xf3\xad\x04\x00\x00\x00'


def mock_appconfig(*args, **kwargs):
    pass


class MockInternalProxy():

    def __init__(self, *args, **kwargs):
        pass

    def create_container(self, *args, **kwargs):
        return True

    def upload_file(self, *args, **kwargs):
        return True


_orig_LogUploader = log_uploader.LogUploader


class MockLogUploader(_orig_LogUploader):

    def __init__(self, conf, logger=LOGGER):
        conf['swift_account'] = conf.get('swift_account', '')
        conf['container_name'] = conf.get('container_name', '')
        conf['new_log_cutoff'] = conf.get('new_log_cutoff', '0')
        conf['source_filename_format'] = conf.get(
            'source_filename_format', conf.get('filename_format'))
        log_uploader.LogUploader.__init__(self, conf, 'plugin')
        self.logger = logger
        self.uploaded_files = []

    def upload_one_log(self, filename, year, month, day, hour):
        d = {'year': year, 'month': month, 'day': day, 'hour': hour}
        self.uploaded_files.append((filename, d))
        _orig_LogUploader.upload_one_log(self, filename, year, month,
                                         day, hour)


class TestLogUploader(unittest.TestCase):

    def setUp(self):
        # mock internal proxy
        self._orig_InternalProxy = log_uploader.InternalProxy
        self._orig_appconfig = log_uploader.appconfig
        log_uploader.InternalProxy = MockInternalProxy
        log_uploader.appconfig = mock_appconfig

    def tearDown(self):
        log_uploader.appconfig = self._orig_appconfig
        log_uploader.InternalProxy = self._orig_InternalProxy

    def test_deprecated_glob_style_upload_all_logs(self):
        tmpdir = mkdtemp()
        try:
            today = datetime.now()
            year = today.year
            month = today.month
            day = today.day

            today_str = today.strftime('%Y%m%d')
            time_strs = []
            for i in range(24):
                time_strs.append('%s%0.2d' % (today_str, i))
            for ts in time_strs:
                open(os.path.join(tmpdir, ts), 'w').close()

            conf = {'log_dir': tmpdir}
            uploader = MockLogUploader(conf)
            uploader.upload_all_logs()
            self.assertEquals(len(uploader.uploaded_files), 24)
            for i, file_date in enumerate(sorted(uploader.uploaded_files)):
                d = {'year': year, 'month': month, 'day': day, 'hour': i}
                for k, v in d.items():
                    d[k] = '%0.2d' % v
                expected = (os.path.join(tmpdir, '%s%0.2d' %
                                         (today_str, i)), d)
                self.assertEquals(file_date, expected)
        finally:
            rmtree(tmpdir)

        tmpdir = mkdtemp()
        try:
            today = datetime.now()
            year = today.year
            month = today.month
            day = today.day

            today_str = today.strftime('%Y%m%d')
            time_strs = []
            for i in range(24):
                time_strs.append('%s-%0.2d00' % (today_str, i))
            for ts in time_strs:
                open(os.path.join(tmpdir, 'swift-blah_98764.%s-2400.tar.gz' %
                                  ts), 'w').close()

            open(os.path.join(tmpdir, 'swift.blah_98764.%s-2400.tar.gz' % ts),
                 'w').close()
            open(os.path.join(tmpdir, 'swift-blah_98764.%s-2400.tar.g' % ts),
                 'w').close()
            open(os.path.join(tmpdir,
                              'swift-blah_201102160100.%s-2400.tar.gz' %
                              '201102160100'), 'w').close()

            conf = {
                'log_dir': '%s/' % tmpdir,
                'filename_format': 'swift-blah_98764.%Y%m%d-%H*.tar.gz',
            }
            uploader = MockLogUploader(conf)
            uploader.upload_all_logs()
            self.assertEquals(len(uploader.uploaded_files), 24)
            for i, file_date in enumerate(sorted(uploader.uploaded_files)):
                filename, date_dict = file_date
                filename = os.path.basename(filename)
                self.assert_(today_str in filename, filename)
                self.assert_(filename.startswith('swift'), filename)
                self.assert_(filename.endswith('tar.gz'), filename)
                d = {'year': year, 'month': month, 'day': day, 'hour': i}
                for k, v in d.items():
                    d[k] = '%0.2d' % v
                self.assertEquals(d, date_dict)
        finally:
            rmtree(tmpdir)

        tmpdir = mkdtemp()
        try:
            today = datetime.now()
            year = today.year
            month = today.month
            day = today.day

            today_str = today.strftime('%Y%m%d')
            time_strs = []
            for i in range(24):
                time_strs.append('%s%0.2d' % (today_str, i))
            for i, ts in enumerate(time_strs):
                open(os.path.join(tmpdir, '%s.%s.log' % (i, ts)), 'w').close()

            conf = {
                'log_dir': tmpdir,
                'filename_format': '*.%Y%m%d%H.log',
            }
            uploader = MockLogUploader(conf)
            uploader.upload_all_logs()
            self.assertEquals(len(uploader.uploaded_files), 24)
            fname_to_int = lambda x: int(os.path.basename(x[0]).split('.')[0])
            numerically = lambda x, y: cmp(fname_to_int(x),
                                           fname_to_int(y))
            for i, file_date in enumerate(sorted(uploader.uploaded_files,
                                                 cmp=numerically)):
                d = {'year': year, 'month': month, 'day': day, 'hour': i}
                for k, v in d.items():
                    d[k] = '%0.2d' % v
                expected = (os.path.join(tmpdir, '%s.%s%0.2d.log' %
                                         (i, today_str, i)), d)
                self.assertEquals(file_date, expected)
        finally:
            rmtree(tmpdir)

    def test_bad_pattern_in_config(self):
        files = [datetime.now().strftime('%Y%m%d%H')]
        with temptree(files, contents=[COMPRESSED_DATA] * len(files)) as t:
            # invalid pattern
            conf = {'log_dir': t, 'source_filename_pattern': '%Y%m%d%h'}  # should be %H
            uploader = MockLogUploader(conf)
            self.assertFalse(uploader.validate_filename_pattern())
            uploader.upload_all_logs()
            self.assertEquals(uploader.uploaded_files, [])

            conf = {'log_dir': t, 'source_filename_pattern': '%Y%m%d%H'}
            uploader = MockLogUploader(conf)
            self.assert_(uploader.validate_filename_pattern())
            uploader.upload_all_logs()
            self.assertEquals(len(uploader.uploaded_files), 1)


        # deprecated warning on source_filename_format
        class MockLogger():

            def __init__(self):
                self.msgs = defaultdict(list)

            def log(self, level, msg):
                self.msgs[level].append(msg)

            def __getattr__(self, attr):
                return partial(self.log, attr)

        logger = MockLogger.logger = MockLogger()

        def mock_get_logger(*args, **kwargs):
            return MockLogger.logger

        _orig_get_logger = log_uploader.utils.get_logger
        try:
            log_uploader.utils.get_logger = mock_get_logger
            conf = {'source_filename_format': '%Y%m%d%H'}
            uploader = MockLogUploader(conf, logger=logger)
            self.assert_([m for m in logger.msgs['warning']
                          if 'deprecated' in m])
        finally:
            log_uploader.utils.get_logger = _orig_get_logger

        # convert source_filename_format to regex
        conf = {'source_filename_format': 'pattern-*.%Y%m%d%H.*.gz'}
        uploader = MockLogUploader(conf)
        expected = r'pattern-.*\.%Y%m%d%H\..*\.gz'
        self.assertEquals(uploader.pattern, expected)

        # use source_filename_pattern if we have the choice!
        conf = {
            'source_filename_format': 'bad',
            'source_filename_pattern': 'good',
        }
        uploader = MockLogUploader(conf)
        self.assertEquals(uploader.pattern, 'good')

    def test_pattern_upload_all_logs(self):

        # test empty dir
        with temptree([]) as t:
            conf = {'log_dir': t}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 0)

        def get_random_length_str(max_len=10, chars=string.ascii_letters):
            return ''.join(random.choice(chars) for x in
                           range(random.randint(1, max_len)))

        template = 'prefix_%(random)s_%(digits)s.blah.' \
                '%(datestr)s%(hour)0.2d00-%(next_hour)0.2d00-%(number)s.gz'
        pattern = r'prefix_.*_[0-9]+\.blah\.%Y%m%d%H00-[0-9]{2}00' \
                '-[0-9]?[0-9]\.gz'
        files_that_should_match = []
        # add some files that match
        for i in range(24):
            fname = template % {
                'random': get_random_length_str(),
                'digits': get_random_length_str(16, string.digits),
                'datestr': datetime.now().strftime('%Y%m%d'),
                'hour': i,
                'next_hour': i + 1,
                'number': random.randint(0, 20),
            }
            files_that_should_match.append(fname)

        # add some files that don't match
        files = list(files_that_should_match)
        for i in range(24):
            fname = template % {
                'random': get_random_length_str(),
                'digits': get_random_length_str(16, string.digits),
                'datestr': datetime.now().strftime('%Y%m'),
                'hour': i,
                'next_hour': i + 1,
                'number': random.randint(0, 20),
            }
            files.append(fname)

        for fname in files:
            print fname

        with temptree(files, contents=[COMPRESSED_DATA] * len(files)) as t:
            self.assertEquals(len(os.listdir(t)), 48)
            conf = {'source_filename_pattern': pattern, 'log_dir': t}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(os.listdir(t)), 24)
            self.assertEquals(len(uploader.uploaded_files), 24)
            files_that_were_uploaded = set(x[0] for x in
                                           uploader.uploaded_files)
            for f in files_that_should_match:
                self.assert_(os.path.join(t, f) in files_that_were_uploaded)

    def test_log_cutoff(self):
        files = [datetime.now().strftime('%Y%m%d%H')]
        with temptree(files) as t:
            conf = {'log_dir': t, 'new_log_cutoff': '7200'}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 0)
            conf = {'log_dir': t, 'new_log_cutoff': '0'}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 1)

    def test_create_container_fail(self):
        files = [datetime.now().strftime('%Y%m%d%H')]
        with temptree(files) as t:
            conf = {'log_dir': t}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 1)

        with temptree(files) as t:
            conf = {'log_dir': t}
            uploader = MockLogUploader(conf)
            # mock create_container to fail
            uploader.internal_proxy.create_container = lambda *args: False
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 0)

    def test_unlink_log(self):
        files = [datetime.now().strftime('%Y%m%d%H')]
        with temptree(files, contents=[COMPRESSED_DATA]) as t:
            conf = {'log_dir': t, 'unlink_log': 'false'}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 1)
            # file still there
            self.assertEquals(len(os.listdir(t)), 1)

            conf = {'log_dir': t, 'unlink_log': 'true'}
            uploader = MockLogUploader(conf)
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 1)
            # file gone
            self.assertEquals(len(os.listdir(t)), 0)

    def test_upload_file_failed(self):
        files = [datetime.now().strftime('%Y%m%d%H')]
        with temptree(files, contents=[COMPRESSED_DATA]) as t:
            conf = {'log_dir': t, 'unlink_log': 'true'}
            uploader = MockLogUploader(conf)

            # mock upload_file to fail, and clean up mock
            def mock_upload_file(self, *args, **kwargs):
                uploader.uploaded_files.pop()
                return False
            uploader.internal_proxy.upload_file = mock_upload_file
            uploader.run_once()
            self.assertEquals(len(uploader.uploaded_files), 0)
            # file still there
            self.assertEquals(len(os.listdir(t)), 1)



if __name__ == '__main__':
    unittest.main()
