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

from swift.stats import log_uploader

import logging
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger()

DEFAULT_GLOB = '%Y%m%d%H'


class TestLogUploader(unittest.TestCase):

    def test_upload_all_logs(self):

        class MockInternalProxy():

            def create_container(self, *args, **kwargs):
                pass

        class MonkeyLogUploader(log_uploader.LogUploader):

            def __init__(self, conf, logger=LOGGER):
                self.log_dir = conf['log_dir']
                self.filename_format = conf.get('filename_format',
                                                DEFAULT_GLOB)
                self.new_log_cutoff = 0
                self.logger = logger
                self.internal_proxy = MockInternalProxy()
                self.swift_account = ''
                self.container_name = ''

                self.uploaded_files = []

            def upload_one_log(self, filename, year, month, day, hour):
                d = {'year': year, 'month': month, 'day': day, 'hour': hour}
                self.uploaded_files.append((filename, d))

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
            uploader = MonkeyLogUploader(conf)
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
            uploader = MonkeyLogUploader(conf)
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
            uploader = MonkeyLogUploader(conf)
            uploader.upload_all_logs()
            self.assertEquals(len(uploader.uploaded_files), 24)
            for i, file_date in enumerate(sorted(uploader.uploaded_files)):
                d = {'year': year, 'month': month, 'day': day, 'hour': i}
                for k, v in d.items():
                    d[k] = '%0.2d' % v
                expected = (os.path.join(tmpdir, '%s.%s%0.2d.log' %
                                         (i, today_str, i)), d)
                # TODO: support wildcards before the date pattern
                # (i.e. relative offsets)
                #print file_date
                #self.assertEquals(file_date, expected)
        finally:
            rmtree(tmpdir)


if __name__ == '__main__':
    unittest.main()
