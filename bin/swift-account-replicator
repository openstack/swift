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

import sys
from ConfigParser import ConfigParser
import getopt

from swift.account import server as account_server
from swift.common import db, db_replicator, utils

class AccountReplicator(db_replicator.Replicator):
    server_type = 'account'
    ring_file = 'account.ring.gz'
    brokerclass = db.AccountBroker
    datadir = account_server.DATADIR
    default_port = 6002

if __name__ == '__main__':
    optlist, args = getopt.getopt(sys.argv[1:], '', ['once'])

    if not args:
        print "Usage: account-replicator <--once> CONFIG_FILE [once]"
        sys.exit()

    c = ConfigParser()
    if not c.read(args[0]):
        print "Unable to read config file."
        sys.exit(1)
    once = len(args) > 1 and args[1] == 'once'

    server_conf = dict(c.items('account-server'))
    if c.has_section('account-replicator'):
        replicator_conf = dict(c.items('account-replicator'))
    else:
        print "Unable to find account-replicator config section in %s." % \
                args[0]
        sys.exit(1)

    utils.drop_privileges(server_conf.get('user', 'swift'))
    if once or '--once' in [opt[0] for opt in optlist]:
        AccountReplicator(server_conf, replicator_conf).replicate_once()
    else:
        AccountReplicator(server_conf, replicator_conf).replicate_forever()

