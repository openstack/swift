# Copyright (c) 2010 OpenStack, LLC.
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

from swift.account.server import DATADIR as account_server_data_dir
from swift.common.db import AccountBroker
from swift.common.internal_proxy import InternalProxy
from swift.common.utils import renamer, get_logger
from swift.common.daemon import Daemon

class AccountStat(Daemon):
    def __init__(self, stats_conf):
        super(self, AccountStat).__init__(stats_conf)
        target_dir = stats_conf.get('log_dir', '/var/log/swift')
        account_server_conf_loc = stats_conf.get('account_server_conf',
                                             '/etc/swift/account-server.conf')
        server_conf = utils.readconf(account_server_conf_loc, 'account-server')
        filename_format = stats_conf['source_filename_format']
        self.filename_format = filename_format
        self.target_dir = target_dir
        self.devices = server_conf.get('devices', '/srv/node')
        self.mount_check = server_conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.logger = get_logger(stats_conf, 'swift-account-stats-logger')

    def run_once(self):
        self.logger.info("Gathering account stats")
        start = time.time()
        self.find_and_process()
        self.logger.info("Gathering account stats complete (%0.2f minutes)" %
            ((time.time()-start)/60))

    def find_and_process(self):
        src_filename = time.strftime(self.filename_format)
        tmp_filename = os.path.join('/tmp', src_filename)
        with open(tmp_filename, 'wb') as statfile:
            #statfile.write('Account Name, Container Count, Object Count, Bytes Used, Created At\n')
            for device in os.listdir(self.devices):
                if self.mount_check and \
                        not os.path.ismount(os.path.join(self.devices, device)):
                    self.logger.error("Device %s is not mounted, skipping." %
                        device)
                    continue
                accounts = os.path.join(self.devices,
                                        device,
                                        account_server_data_dir)
                if not os.path.exists(accounts):
                    self.logger.debug("Path %s does not exist, skipping." %
                        accounts)
                    continue
                for root, dirs, files in os.walk(accounts, topdown=False):
                    for filename in files:
                        if filename.endswith('.db'):
                            broker = AccountBroker(os.path.join(root, filename))
                            if not broker.is_deleted():
                                account_name,
                                created_at,
                                _, _,
                                container_count,
                                object_count,
                                bytes_used,
                                _, _ = broker.get_info()
                                line_data = '"%s",%d,%d,%d,%s\n' % (account_name,
                                                                    container_count,
                                                                    object_count,
                                                                    bytes_used,
                                                                    created_at)
                                statfile.write(line_data)
        renamer(tmp_filename, os.path.join(self.target_dir, src_filename))
