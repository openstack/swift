#!/usr/bin/python -u
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

import uuid
from urlparse import urlparse
import random
from nose import SkipTest
import unittest

from swiftclient import client

from swift.common.manager import Manager
from test.probe.common import ReplProbeTest, ENABLED_POLICIES


def get_current_realm_cluster(url):
    parts = urlparse(url)
    url = parts.scheme + '://' + parts.netloc + '/info'
    http_conn = client.http_connection(url)
    try:
        info = client.get_capabilities(http_conn)
    except client.ClientException:
        raise SkipTest('Unable to retrieve cluster info')
    try:
        realms = info['container_sync']['realms']
    except KeyError:
        raise SkipTest('Unable to find container sync realms')
    for realm, realm_info in realms.items():
        for cluster, options in realm_info['clusters'].items():
            if options.get('current', False):
                return realm, cluster
    raise SkipTest('Unable find current realm cluster')


class TestContainerSync(ReplProbeTest):

    def setUp(self):
        super(TestContainerSync, self).setUp()
        self.realm, self.cluster = get_current_realm_cluster(self.url)

    def test_sync(self):
        base_headers = {'X-Container-Sync-Key': 'secret'}

        # setup dest container
        dest_container = 'dest-container-%s' % uuid.uuid4()
        dest_headers = base_headers.copy()
        dest_policy = None
        if len(ENABLED_POLICIES) > 1:
            dest_policy = random.choice(ENABLED_POLICIES)
            dest_headers['X-Storage-Policy'] = dest_policy.name
        client.put_container(self.url, self.token, dest_container,
                             headers=dest_headers)

        # setup source container
        source_container = 'source-container-%s' % uuid.uuid4()
        source_headers = base_headers.copy()
        sync_to = '//%s/%s/%s/%s' % (self.realm, self.cluster, self.account,
                                     dest_container)
        source_headers['X-Container-Sync-To'] = sync_to
        if dest_policy:
            source_policy = random.choice([p for p in ENABLED_POLICIES
                                           if p is not dest_policy])
            source_headers['X-Storage-Policy'] = source_policy.name
        client.put_container(self.url, self.token, source_container,
                             headers=source_headers)

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body')

        # cycle container-sync
        Manager(['container-sync']).once()

        # retrieve from sync'd container
        headers, body = client.get_object(self.url, self.token,
                                          dest_container, object_name)
        self.assertEqual(body, 'test-body')


if __name__ == "__main__":
    unittest.main()
