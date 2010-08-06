#!/usr/bin/python
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
import signal
import sys
import time
from ConfigParser import ConfigParser

from swift.account_stats import AccountStat
from swift.common import utils

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: swift-account-stats-logger CONFIG_FILE"
        sys.exit()

    c = ConfigParser()
    if not c.read(sys.argv[1]):
        print "Unable to read config file."
        sys.exit(1)

    if c.has_section('log-processor-stats'):
        stats_conf = dict(c.items('log-processor-stats'))
    else:
        print "Unable to find log-processor-stats config section in %s." % \
                sys.argv[1]
        sys.exit(1)

    # reference this from the account stats conf

    target_dir = stats.conf.get('log_dir', '/var/log/swift')
    account_server_conf_loc = stats_conf.get('account_server_conf',
                                             '/etc/swift/account-server.conf')
    filename_format = stats.conf['source_filename_format']
    try:
        c = ConfigParser()
        c.read(account_server_conf_loc)
        account_server_conf = dict(c.items('account-server'))
    except:
        print "Unable to load account server conf from %s" % account_server_conf_loc
        sys.exit(1)

    utils.drop_privileges(account_server_conf.get('user', 'swift'))

    try:
        os.setsid()
    except OSError:
        pass

    logger = utils.get_logger(stats_conf, 'swift-account-stats-logger')

    def kill_children(*args):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.killpg(0, signal.SIGTERM)
        sys.exit()

    signal.signal(signal.SIGTERM, kill_children)

    stats = AccountStat(filename_format,
                        target_dir,
                        account_server_conf,
                        logger)
    logger.info("Gathering account stats")
    start = time.time()
    stats.find_and_process()
    logger.info("Gathering account stats complete (%0.2f minutes)" %
        ((time.time()-start)/60))
