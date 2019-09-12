# Copyright (c) 2017 OpenStack Foundation
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

import uuid

from swift.common.manager import Manager
from swiftclient import client

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest


def chunker(body):
    '''Helper to ensure swiftclient sends a chunked request.'''
    yield body


class TestPutIfNoneMatchRepl(ReplProbeTest):
    def setUp(self):
        super(TestPutIfNoneMatchRepl, self).setUp()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name, 'object',
                                   policy=self.policy)

    def _do_test(self, overwrite_contents):
        self.brain.put_container()
        self.brain.stop_primary_half()
        # put object to only 1 of 3 primaries
        self.brain.put_object(contents=b'VERIFY')
        self.brain.start_primary_half()

        # Restart services and attempt to overwrite
        with self.assertRaises(client.ClientException) as exc_mgr:
            self.brain.put_object(headers={'If-None-Match': '*'},
                                  contents=overwrite_contents)
        self.assertEqual(exc_mgr.exception.http_status, 412)

        # make sure we're GETting from the servers that missed the original PUT
        self.brain.stop_handoff_half()

        # verify the PUT did not complete
        with self.assertRaises(client.ClientException) as exc_mgr:
            client.get_object(
                self.url, self.token, self.container_name, self.object_name)
        self.assertEqual(exc_mgr.exception.http_status, 404)

        # for completeness, run replicators...
        Manager(['object-replicator']).once()

        # ...and verify the object was not overwritten
        _headers, body = client.get_object(
            self.url, self.token, self.container_name, self.object_name)
        self.assertEqual(body, b'VERIFY')

    def test_content_length_nonzero(self):
        self._do_test(b'OVERWRITE')

    def test_content_length_zero(self):
        self._do_test(b'')

    def test_chunked(self):
        self._do_test(chunker(b'OVERWRITE'))

    def test_chunked_empty(self):
        self._do_test(chunker(b''))
