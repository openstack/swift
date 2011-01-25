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

import os
import time
from paste.deploy import appconfig
import shutil
import hashlib

from swift.account.server import DATADIR as account_server_data_dir
from swift.common.db import AccountBroker
from swift.common.internal_proxy import InternalProxy
from swift.common.utils import renamer, get_logger, readconf, mkdirs
from swift.common.constraints import check_mount
from swift.common.daemon import Daemon


class AccountStat(Daemon):
    """
    Extract storage stats from account databases on the account
    storage nodes
    """

    def __init__(self, stats_conf):
        super(AccountStat, self).__init__(stats_conf)
        target_dir = stats_conf.get('log_dir', '/var/log/swift')
        account_server_conf_loc = stats_conf.get('account_server_conf',
                                             '/etc/swift/account-server.conf')
        server_conf = appconfig('config:%s' % account_server_conf_loc,
                                name='account-server')
        filename_format = stats_conf['source_filename_format']
        if filename_format.count('*') > 1:
            raise Exception('source filename format should have at max one *')
        self.filename_format = filename_format
        self.target_dir = target_dir
        mkdirs(self.target_dir)
        self.devices = server_conf.get('devices', '/srv/node')
        self.mount_check = server_conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.logger = get_logger(stats_conf, 'swift-account-stats-logger')

    def run_once(self):
        self.logger.info(_("Gathering account stats"))
        start = time.time()
        self.find_and_process()
        self.logger.info(
            _("Gathering account stats complete (%0.2f minutes)") %
            ((time.time() - start) / 60))

    def find_and_process(self):
        src_filename = time.strftime(self.filename_format)
        working_dir = os.path.join(self.target_dir, '.stats_tmp')
        shutil.rmtree(working_dir, ignore_errors=True)
        mkdirs(working_dir)
        tmp_filename = os.path.join(working_dir, src_filename)
        hasher = hashlib.md5()
        with open(tmp_filename, 'wb') as statfile:
            # csv has the following columns:
            # Account Name, Container Count, Object Count, Bytes Used
            for device in os.listdir(self.devices):
                if self.mount_check and not check_mount(self.devices, device):
                    self.logger.error(
                        _("Device %s is not mounted, skipping.") % device)
                    continue
                accounts = os.path.join(self.devices,
                                        device,
                                        account_server_data_dir)
                if not os.path.exists(accounts):
                    self.logger.debug(_("Path %s does not exist, skipping.") %
                        accounts)
                    continue
                for root, dirs, files in os.walk(accounts, topdown=False):
                    for filename in files:
                        if filename.endswith('.db'):
                            db_path = os.path.join(root, filename)
                            broker = AccountBroker(db_path)
                            if not broker.is_deleted():
                                (account_name,
                                _junk, _junk, _junk,
                                container_count,
                                object_count,
                                bytes_used,
                                _junk, _junk) = broker.get_info()
                                line_data = '"%s",%d,%d,%d\n' % (
                                    account_name, container_count,
                                    object_count, bytes_used)
                                statfile.write(line_data)
                                hasher.update(line_data)
        file_hash = hasher.hexdigest()
        hash_index = src_filename.find('*')
        if hash_index < 0:
            # if there is no * in the target filename, the uploader probably
            # won't work because we are crafting a filename that doesn't
            # fit the pattern
            src_filename = '_'.join([src_filename, file_hash])
        else:
            parts = src_filename[:hash_index], src_filename[hash_index + 1:]
            src_filename = ''.join([parts[0], file_hash, parts[1]])
        renamer(tmp_filename, os.path.join(self.target_dir, src_filename))
        shutil.rmtree(working_dir, ignore_errors=True)
