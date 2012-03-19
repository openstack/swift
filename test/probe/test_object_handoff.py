#!/usr/bin/python -u
# Copyright (c) 2010-2012 OpenStack, LLC.
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
from os import kill
from signal import SIGTERM
from subprocess import call, Popen
from time import sleep
from uuid import uuid4

from swift.common import client, direct_client

from test.probe.common import kill_pids, reset_environment


class TestObjectHandoff(unittest.TestCase):

    def setUp(self):
        self.pids, self.port2server, self.account_ring, self.container_ring, \
            self.object_ring, self.url, self.token, self.account = \
                reset_environment()

    def tearDown(self):
        kill_pids(self.pids)

    def test_main(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        apart, anodes = self.account_ring.get_nodes(self.account)

        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes[0]
        obj = 'object-%s' % uuid4()
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        onode = onodes[0]
        kill(self.pids[self.port2server[onode['port']]], SIGTERM)
        client.put_object(self.url, self.token, container, obj, 'VERIFY')
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))
        # Kill all primaries to ensure GET handoff works
        for node in onodes[1:]:
            kill(self.pids[self.port2server[node['port']]], SIGTERM)
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))
        for node in onodes[1:]:
            self.pids[self.port2server[node['port']]] = Popen([
                'swift-object-server',
                '/etc/swift/object-server/%d.conf' %
                ((node['port'] - 6000) / 10)]).pid
        sleep(2)
        # We've indirectly verified the handoff node has the object, but let's
        # directly verify it.
        another_onode = self.object_ring.get_more_nodes(opart).next()
        odata = direct_client.direct_get_object(another_onode, opart,
                    self.account, container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Direct object GET did not return VERIFY, instead '
                            'it returned: %s' % repr(odata))
        objs = [o['name'] for o in
                client.get_container(self.url, self.token, container)[1]]
        if obj not in objs:
            raise Exception('Container listing did not know about object')
        for cnode in cnodes:
            objs = [o['name'] for o in
                    direct_client.direct_get_container(cnode, cpart,
                        self.account, container)[1]]
            if obj not in objs:
                raise Exception(
                    'Container server %s:%s did not know about object' %
                    (cnode['ip'], cnode['port']))
        self.pids[self.port2server[onode['port']]] = Popen([
            'swift-object-server',
            '/etc/swift/object-server/%d.conf' %
            ((onode['port'] - 6000) / 10)]).pid
        sleep(2)
        exc = False
        try:
            direct_client.direct_get_object(onode, opart, self.account,
                                            container, obj)
        except Exception:
            exc = True
        if not exc:
            raise Exception('Previously downed object server had test object')
        # Run the extra server last so it'll remove it's extra partition
        ps = []
        for n in onodes:
            ps.append(Popen(['swift-object-replicator',
                             '/etc/swift/object-server/%d.conf' %
                             ((n['port'] - 6000) / 10), 'once']))
        for p in ps:
            p.wait()
        call(['swift-object-replicator',
              '/etc/swift/object-server/%d.conf' %
              ((another_onode['port'] - 6000) / 10), 'once'])
        odata = direct_client.direct_get_object(onode, opart, self.account,
                                                container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Direct object GET did not return VERIFY, instead '
                            'it returned: %s' % repr(odata))
        exc = False
        try:
            direct_client.direct_get_object(another_onode, opart, self.account,
                                            container, obj)
        except Exception:
            exc = True
        if not exc:
            raise Exception('Handoff object server still had test object')

# Because POST has changed to a COPY by default, POSTs will succeed on all up
# nodes now if at least one up node has the object.
#       kill(self.pids[self.port2server[onode['port']]], SIGTERM)
#       client.post_object(self.url, self.token, container, obj,
#                          headers={'x-object-meta-probe': 'value'})
#       oheaders = client.head_object(self.url, self.token, container, obj)
#       if oheaders.get('x-object-meta-probe') != 'value':
#           raise Exception('Metadata incorrect, was %s' % repr(oheaders))
#       exc = False
#       try:
#           direct_client.direct_get_object(another_onode, opart, self.account,
#                                           container, obj)
#       except Exception:
#           exc = True
#       if not exc:
#           raise Exception('Handoff server claimed it had the object when '
#                           'it should not have it')
#       self.pids[self.port2server[onode['port']]] = Popen([
#           'swift-object-server',
#           '/etc/swift/object-server/%d.conf' %
#           ((onode['port'] - 6000) / 10)]).pid
#       sleep(2)
#       oheaders = direct_client.direct_get_object(onode, opart, self.account,
#                                                   container, obj)[0]
#       if oheaders.get('x-object-meta-probe') == 'value':
#           raise Exception('Previously downed object server had the new '
#                           'metadata when it should not have it')
#       # Run the extra server last so it'll remove it's extra partition
#       ps = []
#       for n in onodes:
#           ps.append(Popen(['swift-object-replicator',
#                            '/etc/swift/object-server/%d.conf' %
#                            ((n['port'] - 6000) / 10), 'once']))
#       for p in ps:
#           p.wait()
#       call(['swift-object-replicator',
#             '/etc/swift/object-server/%d.conf' %
#             ((another_onode['port'] - 6000) / 10), 'once'])
#       oheaders = direct_client.direct_get_object(onode, opart, self.account,
#                                                   container, obj)[0]
#       if oheaders.get('x-object-meta-probe') != 'value':
#           raise Exception(
#               'Previously downed object server did not have the new metadata')

        kill(self.pids[self.port2server[onode['port']]], SIGTERM)
        client.delete_object(self.url, self.token, container, obj)
        exc = False
        try:
            client.head_object(self.url, self.token, container, obj)
        except Exception:
            exc = True
        if not exc:
            raise Exception('Regular object HEAD was still successful')
        objs = [o['name'] for o in
                client.get_container(self.url, self.token, container)[1]]
        if obj in objs:
            raise Exception('Container listing still knew about object')
        for cnode in cnodes:
            objs = [o['name'] for o in
                    direct_client.direct_get_container(
                        cnode, cpart, self.account, container)[1]]
            if obj in objs:
                raise Exception(
                    'Container server %s:%s still knew about object' %
                    (cnode['ip'], cnode['port']))
        self.pids[self.port2server[onode['port']]] = Popen([
            'swift-object-server',
            '/etc/swift/object-server/%d.conf' %
            ((onode['port'] - 6000) / 10)]).pid
        sleep(2)
        direct_client.direct_get_object(onode, opart, self.account, container,
                                        obj)
        # Run the extra server last so it'll remove it's extra partition
        ps = []
        for n in onodes:
            ps.append(Popen(['swift-object-replicator',
                             '/etc/swift/object-server/%d.conf' %
                             ((n['port'] - 6000) / 10), 'once']))
        for p in ps:
            p.wait()
        call(['swift-object-replicator',
              '/etc/swift/object-server/%d.conf' %
              ((another_onode['port'] - 6000) / 10), 'once'])
        exc = False
        try:
            direct_client.direct_get_object(another_onode, opart, self.account,
                                            container, obj)
        except Exception:
            exc = True
        if not exc:
            raise Exception('Handoff object server still had the object')


if __name__ == '__main__':
    unittest.main()
