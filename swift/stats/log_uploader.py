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

from __future__ import with_statement
import os
import hashlib
import time
import gzip
import glob
from paste.deploy import appconfig

from swift.common.internal_proxy import InternalProxy
from swift.common.daemon import Daemon
from swift.common import utils


class LogUploader(Daemon):
    '''
    Given a local directory, a swift account, and a container name, LogParser
    will upload all files in the local directory to the given account/
    container.  All but the newest files will be uploaded, and the files' md5
    sum will be computed. The hash is used to prevent duplicate data from
    being uploaded multiple times in different files (ex: log lines). Since
    the hash is computed, it is also used as the uploaded object's etag to
    ensure data integrity.

    Note that after the file is successfully uploaded, it will be unlinked.

    The given proxy server config is used to instantiate a proxy server for
    the object uploads.
    '''

    def __init__(self, uploader_conf, plugin_name):
        super(LogUploader, self).__init__(uploader_conf)
        log_dir = uploader_conf.get('log_dir', '/var/log/swift/')
        swift_account = uploader_conf['swift_account']
        container_name = uploader_conf['container_name']
        source_filename_format = uploader_conf['source_filename_format']
        proxy_server_conf_loc = uploader_conf.get('proxy_server_conf',
                                            '/etc/swift/proxy-server.conf')
        proxy_server_conf = appconfig('config:%s' % proxy_server_conf_loc,
                                      name='proxy-server')
        new_log_cutoff = int(uploader_conf.get('new_log_cutoff', '7200'))
        unlink_log = uploader_conf.get('unlink_log', 'True').lower() in \
                                                    ('true', 'on', '1', 'yes')
        self.unlink_log = unlink_log
        self.new_log_cutoff = new_log_cutoff
        if not log_dir.endswith('/'):
            log_dir = log_dir + '/'
        self.log_dir = log_dir
        self.swift_account = swift_account
        self.container_name = container_name
        self.filename_format = source_filename_format
        self.internal_proxy = InternalProxy(proxy_server_conf)
        log_name = 'swift-log-uploader-%s' % plugin_name
        self.logger = utils.get_logger(uploader_conf, plugin_name)

    def run_once(self):
        self.logger.info(_("Uploading logs"))
        start = time.time()
        self.upload_all_logs()
        self.logger.info(_("Uploading logs complete (%0.2f minutes)") %
            ((time.time() - start) / 60))

    def upload_all_logs(self):
        i = [(self.filename_format.index(c), c) for c in '%Y %m %d %H'.split()]
        i.sort()
        year_offset = month_offset = day_offset = hour_offset = None
        base_offset = len(self.log_dir)
        for start, c in i:
            offset = base_offset + start
            if c == '%Y':
                year_offset = offset, offset + 4
                # Add in the difference between len(%Y) and the expanded
                # version of %Y (????). This makes sure the codes after this
                # one will align properly in the final filename.
                base_offset += 2
            elif c == '%m':
                month_offset = offset, offset + 2
            elif c == '%d':
                day_offset = offset, offset + 2
            elif c == '%H':
                hour_offset = offset, offset + 2
        if not (year_offset and month_offset and day_offset and hour_offset):
            # don't have all the parts, can't upload anything
            return
        glob_pattern = self.filename_format
        glob_pattern = glob_pattern.replace('%Y', '????', 1)
        glob_pattern = glob_pattern.replace('%m', '??', 1)
        glob_pattern = glob_pattern.replace('%d', '??', 1)
        glob_pattern = glob_pattern.replace('%H', '??', 1)
        filelist = glob.iglob(os.path.join(self.log_dir, glob_pattern))
        current_hour = int(time.strftime('%H'))
        today = int(time.strftime('%Y%m%d'))
        self.internal_proxy.create_container(self.swift_account,
                                            self.container_name)
        for filename in filelist:
            try:
                # From the filename, we need to derive the year, month, day,
                # and hour for the file. These values are used in the uploaded
                # object's name, so they should be a reasonably accurate
                # representation of the time for which the data in the file was
                # collected. The file's last modified time is not a reliable
                # representation of the data in the file. For example, an old
                # log file (from hour A) may be uploaded or moved into the
                # log_dir in hour Z. The file's modified time will be for hour
                # Z, and therefore the object's name in the system will not
                # represent the data in it.
                # If the filename doesn't match the format, it shouldn't be
                # uploaded.
                year = filename[slice(*year_offset)]
                month = filename[slice(*month_offset)]
                day = filename[slice(*day_offset)]
                hour = filename[slice(*hour_offset)]
            except IndexError:
                # unexpected filename format, move on
                self.logger.error(_("Unexpected log: %s") % filename)
                continue
            if ((time.time() - os.stat(filename).st_mtime) <
                                                        self.new_log_cutoff):
                # don't process very new logs
                self.logger.debug(
                    _("Skipping log: %(file)s (< %(cutoff)d seconds old)") %
                    {'file': filename, 'cutoff': self.new_log_cutoff})
                continue
            self.upload_one_log(filename, year, month, day, hour)

    def upload_one_log(self, filename, year, month, day, hour):
        if os.path.getsize(filename) == 0:
            self.logger.debug(_("Log %s is 0 length, skipping") % filename)
            return
        self.logger.debug(_("Processing log: %s") % filename)
        filehash = hashlib.md5()
        already_compressed = True if filename.endswith('.gz') else False
        opener = gzip.open if already_compressed else open
        f = opener(filename, 'rb')
        try:
            for line in f:
                # filter out bad lines here?
                filehash.update(line)
        finally:
            f.close()
        filehash = filehash.hexdigest()
        # By adding a hash to the filename, we ensure that uploaded files
        # have unique filenames and protect against uploading one file
        # more than one time. By using md5, we get an etag for free.
        target_filename = '/'.join([year, month, day, hour, filehash + '.gz'])
        if self.internal_proxy.upload_file(filename,
                                          self.swift_account,
                                          self.container_name,
                                          target_filename,
                                          compress=(not already_compressed)):
            self.logger.debug(_("Uploaded log %(file)s to %(target)s") %
                {'file': filename, 'target': target_filename})
            if self.unlink_log:
                os.unlink(filename)
        else:
            self.logger.error(_("ERROR: Upload of log %s failed!") % filename)
