#!/usr/bin/python -u
# Copyright (c) 2010-2012 OpenStack Foundation
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

from swiftclient import client
from unittest import main

from swift.common.exceptions import LockTimeout
from swift.common.manager import Manager
from swift.common.utils import hash_path, readconf, Timestamp
from swift.container.backend import ContainerBroker

from test.probe.common import (
    kill_nonprimary_server, kill_server, start_server, ReplProbeTest)

# Why is this not called test_container_orphan? Because the crash
# happens in the account server, so both account and container
# services are involved.
#
# The common way users do this is to use TripleO to deploy an overcloud
# and add Gnocchi. Gnocchi is hammering Swift, its container has updates
# all the time. Then, users crash the overcloud and re-deploy it,
# using the new suffix in swift.conf. Thereafter, container service
# inherits old container with outstanding updates, container updater
# tries to send updates to the account server, while the account cannot
# be found anymore. In this situation, in Swift 2.25.0, account server
# tracebacks, and the cycle continues without end.


class TestOrphanContainer(ReplProbeTest):

    def get_account_db_files(self, account):

        # This is "more correct" than (port_num%100)//10, but is it worth it?
        # We have the assumption about port_num vs node_id embedded all over.
        account_configs = {}
        for _, cname in self.configs['account-server'].items():
            conf = readconf(cname)
            # config parser cannot know if it's a number or not, so int()
            port = int(conf['app:account-server']['bind_port'])
            account_configs[port] = conf

        part, nodes = self.account_ring.get_nodes(account)
        hash_str = hash_path(account)

        ret = []
        for node in nodes:

            data_dir = 'accounts'
            device = node['device']
            conf = account_configs[node['port']]
            devices = conf['app:account-server']['devices']

            # os.path.join is for the weak
            db_file = '%s/%s/%s/%s/%s/%s/%s.db' % (
                devices, device, data_dir, part,
                hash_str[-3:], hash_str, hash_str)
            ret.append(db_file)
        return ret

    def test_update_pending(self):

        # Create container
        container = 'contx'
        client.put_container(self.url, self.token, container)

        part, nodes = self.account_ring.get_nodes(self.account)
        anode = nodes[0]

        # Stop a quorum of account servers
        # This allows the put to continue later.
        kill_nonprimary_server(nodes, self.ipport2server)
        kill_server((anode['ip'], anode['port']), self.ipport2server)

        # Put object
        # This creates an outstanding update.
        client.put_object(self.url, self.token, container, 'object1', b'123')

        cont_db_files = self.get_container_db_files(container)
        self.assertEqual(len(cont_db_files), 3)

        # Collect the observable state from containers
        outstanding_files = []
        for cfile in cont_db_files:
            broker = ContainerBroker(cfile)
            try:
                info = broker.get_info()
            except LockTimeout:
                self.fail('LockTimeout at %s' % (cfile,))
            if Timestamp(info['put_timestamp']) <= 0:
                self.fail('No put_timestamp at %s' % (cfile,))
            # Correct even if reported_put_timestamp is zero.
            if info['put_timestamp'] > info['reported_put_timestamp']:
                outstanding_files.append(cfile)
        self.assertGreater(len(outstanding_files), 0)

        # At this point the users shut everything down and screw up the
        # hash in swift.conf. But we destroy the account DB instead.
        files = self.get_account_db_files(self.account)
        for afile in files:
            os.unlink(afile)

        # Restart the stopped primary server
        start_server((anode['ip'], anode['port']), self.ipport2server)

        # Make sure updaters run
        Manager(['container-updater']).once()

        # Collect the observable state from containers again and examine it
        outstanding_files_new = []
        for cfile in cont_db_files:

            # We aren't catching DatabaseConnectionError, because
            # we only want to approve of DBs that were quarantined,
            # and not otherwise damaged. So if the code below throws
            # an exception for other reason, we want the test to fail.
            if not os.path.exists(cfile):
                continue

            broker = ContainerBroker(cfile)
            try:
                info = broker.get_info()
            except LockTimeout:
                self.fail('LockTimeout at %s' % (cfile,))
            if Timestamp(info['put_timestamp']) <= 0:
                self.fail('No put_timestamp at %s' % (cfile,))
            # Correct even if reported_put_timestamp is zero.
            if info['put_timestamp'] > info['reported_put_timestamp']:
                outstanding_files_new.append(cfile)
        self.assertLengthEqual(outstanding_files_new, 0)

        self.get_to_final_state()


if __name__ == '__main__':
    main()
