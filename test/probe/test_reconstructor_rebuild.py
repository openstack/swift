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
import itertools
import unittest
import uuid
import random
import time

from swift.common.direct_client import DirectClientException
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.internal_client import UnexpectedResponse
from swift.common.manager import Manager
from swift.common.swob import wsgi_to_str, str_to_wsgi
from swift.common.utils import md5
from swift.obj.reconstructor import ObjectReconstructor
from test.probe.common import Body as ProbeBody, ECProbeTest

from swift.common import direct_client

from swiftclient import client, ClientException


class Body(object):

    def __init__(self, total=3.5 * 2 ** 20):
        self.total = int(total)
        self.hasher = md5(usedforsecurity=False)
        self.size = 0
        self.chunk = b'test' * 16 * 2 ** 10

    @property
    def etag(self):
        return self.hasher.hexdigest()

    def __iter__(self):
        return self

    def __next__(self):
        if self.size > self.total:
            raise StopIteration()
        self.size += len(self.chunk)
        self.hasher.update(self.chunk)
        return self.chunk


class TestReconstructorRebuild(ECProbeTest):

    def setUp(self):
        super(TestReconstructorRebuild, self).setUp()
        self.int_client = self.make_internal_client()
        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # PUT object and POST some metadata
        self.proxy_put()
        self.headers_post = {
            self._make_name('x-object-meta-').decode('utf8'):
                self._make_name('meta-bar-').decode('utf8')}
        client.post_object(self.url, self.token, self.container_name,
                           self.object_name, headers=dict(self.headers_post))

        self.opart, self.onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)

        # stash frag etags and metadata for later comparison
        self.frag_headers, self.frag_etags = self._assert_all_nodes_have_frag()
        self.assertTrue(self.frag_headers)
        for node_index, hdrs in self.frag_headers.items():
            # sanity check
            self.assertIn(
                'X-Backend-Durable-Timestamp', hdrs,
                'Missing durable timestamp in %r' % self.frag_headers)

    def proxy_get(self):
        # Use internal-client instead of python-swiftclient, since we can't
        # handle UTF-8 headers properly w/ swiftclient.
        # Still a proxy-server tho!
        status, headers, body = self.int_client.get_object(
            self.account,
            self.container_name.decode('utf-8'),
            self.object_name.decode('utf-8'))
        resp_checksum = md5(usedforsecurity=False)
        for chunk in body:
            resp_checksum.update(chunk)
        return HeaderKeyDict(headers), resp_checksum.hexdigest()

    def _format_node(self, node):
        return '%s#%s' % (node['device'], node['index'])

    def _assert_all_nodes_have_frag(self, extra_headers=None):
        # check all frags are in place
        failures = []
        frag_etags = {}
        frag_headers = {}
        for node in self.onodes:
            try:
                headers, etag = self.direct_get(node, self.opart,
                                                extra_headers=extra_headers)
                frag_etags[node['index']] = etag
                del headers['Date']  # Date header will vary so remove it
                frag_headers[node['index']] = headers
            except direct_client.DirectClientException as err:
                failures.append((node, err))
        if failures:
            self.fail('\n'.join(['    Node %r raised %r' %
                                 (self._format_node(node), exc)
                                 for (node, exc) in failures]))
        return frag_headers, frag_etags

    def _test_rebuild_scenario(self, failed, non_durable,
                               reconstructor_cycles):
        # helper method to test a scenario with some nodes missing their
        # fragment and some nodes having non-durable fragments
        with self.subTest(
                failed=[self._format_node(self.onodes[n]) for n in failed],
                non_durable=[self._format_node(self.onodes[n])
                             for n in non_durable]):
            self.break_nodes(self.onodes, self.opart, failed, non_durable)

        # make sure we can still GET the object and it is correct; the
        # proxy is doing decode on remaining fragments to get the obj
        with self.subTest(
                failed=[self._format_node(self.onodes[n]) for n in failed],
                non_durable=[self._format_node(self.onodes[n])
                             for n in non_durable]):
            headers, etag = self.proxy_get()
            self.assertEqual(self.etag, etag)
            for key in self.headers_post:
                # Since we use internal_client for the GET, headers come back
                # as WSGI strings
                wsgi_key = str_to_wsgi(key)
                self.assertIn(wsgi_key, headers)
                self.assertEqual(self.headers_post[key],
                                 wsgi_to_str(headers[wsgi_key]))

        # fire up reconstructor
        for i in range(reconstructor_cycles):
            self.reconstructor.once()

        # check GET via proxy returns expected data and metadata
        with self.subTest(
                failed=[self._format_node(self.onodes[n]) for n in failed],
                non_durable=[self._format_node(self.onodes[n])
                             for n in non_durable]):
            headers, etag = self.proxy_get()
            self.assertEqual(self.etag, etag)
            for key in self.headers_post:
                wsgi_key = str_to_wsgi(key)
                self.assertIn(wsgi_key, headers)
                self.assertEqual(self.headers_post[key],
                                 wsgi_to_str(headers[wsgi_key]))
        # check all frags are intact, durable and have expected metadata
        with self.subTest(
                failed=[self._format_node(self.onodes[n]) for n in failed],
                non_durable=[self._format_node(self.onodes[n])
                             for n in non_durable]):
            frag_headers, frag_etags = self._assert_all_nodes_have_frag()
            self.assertEqual(self.frag_etags, frag_etags)
            # self._frag_headers include X-Backend-Durable-Timestamp so this
            # assertion confirms that the rebuilt frags are all durable
            self.assertEqual(self.frag_headers, frag_headers)

    def test_rebuild_missing_frags(self):
        # build up a list of node lists to kill data from,
        # first try a single node
        # then adjacent nodes and then nodes >1 node apart
        single_node = (random.randint(0, 5),)
        adj_nodes = (0, 5)
        far_nodes = (0, 4)

        for failed_nodes in [single_node, adj_nodes, far_nodes]:
            self._test_rebuild_scenario(failed_nodes, [], 1)

    def test_rebuild_non_durable_frags(self):
        # build up a list of node lists to make non-durable,
        # first try a single node
        # then adjacent nodes and then nodes >1 node apart
        single_node = (random.randint(0, 5),)
        adj_nodes = (0, 5)
        far_nodes = (0, 4)

        for non_durable_nodes in [single_node, adj_nodes, far_nodes]:
            self._test_rebuild_scenario([], non_durable_nodes, 1)

    def test_rebuild_with_missing_frags_and_non_durable_frags(self):
        # pick some nodes with parts deleted, some with non-durable fragments
        scenarios = [
            # failed, non-durable
            ((0, 2), (4,)),
            ((0, 4), (2,)),
        ]
        for failed, non_durable in scenarios:
            self._test_rebuild_scenario(failed, non_durable, 3)
        scenarios = [
            # failed, non-durable
            ((0, 1), (2,)),
            ((0, 2), (1,)),
        ]
        for failed, non_durable in scenarios:
            # why 2 repeats? consider missing fragment on nodes 0, 1  and
            # missing durable on node 2: first reconstructor cycle on node 3
            # will make node 2 durable, first cycle on node 5 will rebuild on
            # node 0; second cycle on node 0 or 2 will rebuild on node 1. Note
            # that it is possible, that reconstructor processes on each node
            # run in order such that all rebuild complete in once cycle, but
            # that is not guaranteed, we allow 2 cycles to be sure.
            self._test_rebuild_scenario(failed, non_durable, 2)
        scenarios = [
            # failed, non-durable
            ((0, 2), (1, 3, 5)),
            ((0,), (1, 2, 4, 5)),
        ]
        for failed, non_durable in scenarios:
            # why 3 repeats? consider missing fragment on node 0 and single
            # durable on node 3: first reconstructor cycle on node 3 will make
            # nodes 2 and 4 durable, second cycle on nodes 2 and 4 will make
            # node 1 and 5 durable, third cycle on nodes 1 or 5 will
            # reconstruct the missing fragment on node 0.
            self._test_rebuild_scenario(failed, non_durable, 3)

    def test_rebuild_partner_down(self):
        # we have to pick a lower index because we have few handoffs
        nodes = self.onodes[:2]
        random.shuffle(nodes)  # left or right is fine
        primary_node, partner_node = nodes

        # capture fragment etag from partner
        failed_partner_meta, failed_partner_etag = self.direct_get(
            partner_node, self.opart)

        # and 507 the failed partner device
        device_path = self.device_dir(partner_node)
        self.kill_drive(device_path)

        # reconstruct from the primary, while one of it's partners is 507'd
        self.reconstructor.once(number=self.config_number(primary_node))

        # a handoff will pickup the rebuild
        hnodes = list(self.object_ring.get_more_nodes(self.opart))
        for node in hnodes:
            try:
                found_meta, found_etag = self.direct_get(
                    node, self.opart)
            except DirectClientException as e:
                if e.http_status != 404:
                    raise
            else:
                break
        else:
            self.fail('Unable to fetch rebuilt frag from handoffs %r '
                      'given primary nodes %r with %s unmounted '
                      'trying to rebuild from %s' % (
                          [h['device'] for h in hnodes],
                          [n['device'] for n in self.onodes],
                          partner_node['device'],
                          primary_node['device'],
                      ))
        self.assertEqual(failed_partner_etag, found_etag)
        del failed_partner_meta['Date']
        del found_meta['Date']
        self.assertEqual(failed_partner_meta, found_meta)

        # just to be nice
        self.revive_drive(device_path)

    def test_sync_expired_object(self):
        # verify that missing frag can be rebuilt for an expired object
        delete_after = 2
        self.proxy_put(extra_headers={'x-delete-after': delete_after})
        self.proxy_get()  # sanity check
        orig_frag_headers, orig_frag_etags = self._assert_all_nodes_have_frag(
            extra_headers={'X-Backend-Replication': 'True'})

        # wait for object to expire
        timeout = time.time() + delete_after + 1
        while time.time() < timeout:
            try:
                self.proxy_get()
            except UnexpectedResponse as e:
                if e.resp.status_int == 404:
                    break
                else:
                    raise
        else:
            self.fail('Timed out waiting for %s/%s to expire after %ss' % (
                self.container_name, self.object_name, delete_after))

        # sanity check - X-Backend-Replication let's us get expired frag...
        fail_node = random.choice(self.onodes)
        self.assert_direct_get_succeeds(
            fail_node, self.opart,
            extra_headers={'X-Backend-Replication': 'True'})
        # ...until we remove the frag from fail_node
        self.break_nodes(
            self.onodes, self.opart, [self.onodes.index(fail_node)], [])
        # ...now it's really gone
        with self.assertRaises(DirectClientException) as cm:
            self.direct_get(fail_node, self.opart,
                            extra_headers={'X-Backend-Replication': 'True'})
        self.assertEqual(404, cm.exception.http_status)
        self.assertNotIn('X-Backend-Timestamp', cm.exception.http_headers)

        # run the reconstructor
        self.reconstructor.once()

        # the missing frag is now in place but expired
        with self.assertRaises(DirectClientException) as cm:
            self.direct_get(fail_node, self.opart)
        self.assertEqual(404, cm.exception.http_status)
        self.assertIn('X-Backend-Timestamp', cm.exception.http_headers)

        # check all frags are intact, durable and have expected metadata
        frag_headers, frag_etags = self._assert_all_nodes_have_frag(
            extra_headers={'X-Backend-Replication': 'True'})
        self.assertEqual(orig_frag_etags, frag_etags)
        self.maxDiff = None
        self.assertEqual(orig_frag_headers, frag_headers)

    def test_sync_unexpired_object_metadata(self):
        # verify that metadata can be sync'd to a frag that has missed a POST
        # and consequently that frag appears to be expired, when in fact the
        # POST removed the x-delete-at header
        client.put_container(self.url, self.token, self.container_name,
                             headers={'x-storage-policy': self.policy.name})
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        delete_at = int(time.time() + 3)
        contents = ('body-%s' % uuid.uuid4()).encode()
        headers = {'x-delete-at': delete_at}
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, headers=headers, contents=contents)
        # fail a primary
        post_fail_node = random.choice(onodes)
        post_fail_path = self.device_dir(post_fail_node)
        self.kill_drive(post_fail_path)
        # post over w/o x-delete-at
        client.post_object(self.url, self.token, self.container_name,
                           self.object_name, {'content-type': 'something-new'})
        # revive failed primary
        self.revive_drive(post_fail_path)
        # wait for the delete_at to pass, and check that it thinks the object
        # is expired
        timeout = time.time() + 5
        err = None
        while time.time() < timeout:
            try:
                direct_client.direct_head_object(
                    post_fail_node, opart, self.account, self.container_name,
                    self.object_name, headers={
                        'X-Backend-Storage-Policy-Index': int(self.policy)})
            except direct_client.ClientException as client_err:
                if client_err.http_status != 404:
                    raise
                err = client_err
                break
            else:
                time.sleep(0.1)
        else:
            self.fail('Failed to get a 404 from node with expired object')
        self.assertEqual(err.http_status, 404)
        self.assertIn('X-Backend-Timestamp', err.http_headers)

        # but from the proxy we've got the whole story
        headers, body = client.get_object(self.url, self.token,
                                          self.container_name,
                                          self.object_name)
        self.assertNotIn('X-Delete-At', headers)
        self.reconstructor.once()

        # ... and all the nodes have the final unexpired state
        for node in onodes:
            headers = direct_client.direct_head_object(
                node, opart, self.account, self.container_name,
                self.object_name, headers={
                    'X-Backend-Storage-Policy-Index': int(self.policy)})
            self.assertNotIn('X-Delete-At', headers)

    def test_rebuild_quarantines_lonely_frag(self):
        # fail one device while the object is deleted so we are left with one
        # fragment and some tombstones
        failed_node = self.onodes[0]
        device_path = self.device_dir(failed_node)
        self.kill_drive(device_path)
        self.assert_direct_get_fails(failed_node, self.opart, 507)  # sanity

        # delete object
        client.delete_object(self.url, self.token, self.container_name,
                             self.object_name)

        # check we have tombstones
        for node in self.onodes[1:]:
            err = self.assert_direct_get_fails(node, self.opart, 404)
            self.assertIn('X-Backend-Timestamp', err.http_headers)

        # run the reconstructor with zero reclaim age to clean up tombstones
        for conf_index in self.configs['object-reconstructor'].keys():
            self.run_custom_daemon(
                ObjectReconstructor, 'object-reconstructor', conf_index,
                {'reclaim_age': '0'})

        # check we no longer have tombstones
        for node in self.onodes[1:]:
            err = self.assert_direct_get_fails(node, self.opart, 404)
            self.assertNotIn('X-Timestamp', err.http_headers)

        # revive the failed device and check it has a fragment
        self.revive_drive(device_path)
        self.assert_direct_get_succeeds(failed_node, self.opart)

        # restart proxy to clear error-limiting so that the revived drive
        # participates again
        Manager(['proxy-server']).restart()

        # client GET will fail with 503 ...
        with self.assertRaises(ClientException) as cm:
            client.get_object(self.url, self.token, self.container_name,
                              self.object_name)
        self.assertEqual(503, cm.exception.http_status)
        # ... but client HEAD succeeds

        path = self.int_client.make_path(
            self.account,
            self.container_name.decode('utf-8'),
            self.object_name.decode('utf-8'))
        resp = self.int_client.make_request(
            'HEAD', path, {}, acceptable_statuses=(2,))
        for key in self.headers_post:
            wsgi_key = str_to_wsgi(key)
            self.assertIn(wsgi_key, resp.headers)
            self.assertEqual(self.headers_post[key],
                             wsgi_to_str(resp.headers[wsgi_key]))

        # run the reconstructor without quarantine_threshold set
        error_lines = []
        warning_lines = []
        for conf_index in self.configs['object-reconstructor'].keys():
            reconstructor = self.run_custom_daemon(
                ObjectReconstructor, 'object-reconstructor', conf_index,
                {'quarantine_age': '0'})
            logger = reconstructor.logger
            error_lines.append(logger.get_lines_for_level('error'))
            warning_lines.append(logger.get_lines_for_level('warning'))

        # check logs for errors
        found_lines = False
        for lines in error_lines:
            if not lines:
                continue
            self.assertFalse(found_lines, error_lines)
            found_lines = True
            for line in itertools.islice(lines, 0, 6, 2):
                self.assertIn(
                    'Unable to get enough responses (1/4 from 1 ok '
                    'responses)', line, lines)
            for line in itertools.islice(lines, 1, 7, 2):
                self.assertIn(
                    'Unable to get enough responses (4 x 404 error '
                    'responses)', line, lines)
        self.assertTrue(found_lines, 'error lines not found')

        for lines in warning_lines:
            self.assertEqual([], lines)

        # check we have still have a single fragment and no tombstones
        self.assert_direct_get_succeeds(failed_node, self.opart)
        for node in self.onodes[1:]:
            err = self.assert_direct_get_fails(node, self.opart, 404)
            self.assertNotIn('X-Timestamp', err.http_headers)

        # run the reconstructor to quarantine the lonely frag
        error_lines = []
        warning_lines = []
        for conf_index in self.configs['object-reconstructor'].keys():
            reconstructor = self.run_custom_daemon(
                ObjectReconstructor, 'object-reconstructor', conf_index,
                {'quarantine_age': '0', 'quarantine_threshold': '1'})
            logger = reconstructor.logger
            error_lines.append(logger.get_lines_for_level('error'))
            warning_lines.append(logger.get_lines_for_level('warning'))

        # check logs for errors
        found_lines = False
        for index, lines in enumerate(error_lines):
            if not lines:
                continue
            self.assertFalse(found_lines, error_lines)
            found_lines = True
            for line in itertools.islice(lines, 0, 6, 2):
                self.assertIn(
                    'Unable to get enough responses (1/4 from 1 ok '
                    'responses)', line, lines)
            for line in itertools.islice(lines, 1, 7, 2):
                self.assertIn(
                    'Unable to get enough responses (6 x 404 error '
                    'responses)', line, lines)
        self.assertTrue(found_lines, 'error lines not found')

        # check logs for quarantine warning
        found_lines = False
        for lines in warning_lines:
            if not lines:
                continue
            self.assertFalse(found_lines, warning_lines)
            found_lines = True
            self.assertEqual(1, len(lines), lines)
            self.assertIn('Quarantined object', lines[0])
        self.assertTrue(found_lines, 'warning lines not found')

        # check we have nothing
        for node in self.onodes:
            err = self.assert_direct_get_fails(node, self.opart, 404)
            self.assertNotIn('X-Backend-Timestamp', err.http_headers)
        # client HEAD and GET now both 404
        with self.assertRaises(ClientException) as cm:
            client.get_object(self.url, self.token, self.container_name,
                              self.object_name)
        self.assertEqual(404, cm.exception.http_status)
        with self.assertRaises(ClientException) as cm:
            client.head_object(self.url, self.token, self.container_name,
                               self.object_name)
        self.assertEqual(404, cm.exception.http_status)

        # run the reconstructor once more - should see no errors in logs!
        error_lines = []
        warning_lines = []
        for conf_index in self.configs['object-reconstructor'].keys():
            reconstructor = self.run_custom_daemon(
                ObjectReconstructor, 'object-reconstructor', conf_index,
                {'quarantine_age': '0', 'quarantine_threshold': '1'})
            logger = reconstructor.logger
            error_lines.append(logger.get_lines_for_level('error'))
            warning_lines.append(logger.get_lines_for_level('warning'))

        for lines in error_lines:
            self.assertEqual([], lines)
        for lines in warning_lines:
            self.assertEqual([], lines)

    def test_rebuild_with_non_durable_newer_data(self):
        # Verify that reconstruction correctly rebuilds the durable version
        # when a newer non-durable version also exists on peer nodes.
        #
        # This exercises the X-Backend-Fragment-Preferences mechanism: the
        # reconstructor sends frag_prefs asking remote nodes for fragments at
        # the older durable timestamp. Remote nodes *should* return the older
        # durable fragment provided that the reconstructor sends the *correct*
        # timestamp in the fragment preferences header.
        # See https://bugs.launchpad.net/swift/+bug/2143206

        # setUp already did 1st PUT object (let's call it object v1) and
        # it's durable on all nodes
        fail_node = self.onodes[0]
        orig_frag_etag = self.frag_etags[fail_node['index']]

        # 2nd PUT object (let's call it object v2) with x-backend-no-commit to
        # all nodes: creates non-durable v2 fragments alongside the existing
        # durable v1 fragments on every primary.
        v1_chunk_size = len(b'test' * 16 * 2 ** 10)
        v1_total_bytes = int(3.5 * 2 ** 20)
        # v2 is larger than v1...
        v2_total_bytes = (v1_total_bytes // v1_chunk_size + 1) * v1_chunk_size
        internal_client = self.make_internal_client()
        v2_body = ProbeBody(total=v2_total_bytes)
        internal_client.upload_object(
            v2_body, self.account,
            self.container_name.decode('utf8'),
            self.object_name.decode('utf8'),
            {'x-backend-no-commit': 'True'})

        # proxy GET should still return v1 (the durable version)
        headers, etag = self.proxy_get()
        self.assertEqual(self.etag, etag)

        # each node has both v1 (durable) and v2 (non-durable),
        # and they are different fragments
        for node in self.onodes:
            _hdrs, durable_frag_etag = self.direct_get(
                node, self.opart)
            self.assertEqual(self.frag_etags[node['index']],
                             durable_frag_etag)
            _hdrs, newest_frag_etag = self.direct_get(
                node, self.opart, require_durable=False)
            self.assertNotEqual(durable_frag_etag, newest_frag_etag,
                                'Expected non-durable v2 frag to differ '
                                'from durable v1 frag on node %s'
                                % self._format_node(node))

        # delete node 0's partition entirely so it needs rebuilding
        self.break_nodes(self.onodes, self.opart, [0], [])
        self.assert_direct_get_fails(fail_node, self.opart, 404)

        # run the reconstructor to rebuild node 0's missing fragment
        self.reconstructor.once()

        # node 0 should have been rebuilt with the DURABLE version (v1).
        # The reconstructor on a peer node (e.g. node 1) opens its local
        # durable fragment (v1), then sends X-Backend-Fragment-Preferences
        # asking remote nodes for fragments at v1's timestamp. The frag_prefs
        # lookup should find a match; remote nodes return v1 fragments and
        # reconstruction produces v1 data.
        # If there's a bug that causes the frag_prefs lookup to not find a
        # match, the remote nodes would fall back to returning "newest" (v2)
        # fragments.
        #
        # XXX If the remote nodes do return different fragments than
        # requested then the reconstructor erroneously proceeds to rebuild the
        # missing v1 fragment using v2 fragments!!!
        # See https://bugs.launchpad.net/swift/+bug/2143202
        rebuilt_hdrs, rebuilt_etag = self.direct_get(
            fail_node, self.opart)
        self.assertEqual(orig_frag_etag, rebuilt_etag,
                         'Rebuilt fragment does not match original v1 '
                         'fragment; reconstruction has incorrectly '
                         'used newer non-durable version data')

        # the rebuilt frag should be durable
        self.assertIn('X-Backend-Durable-Timestamp', rebuilt_hdrs)
        durable_timestamp = self.frag_headers[0]['X-Backend-Durable-Timestamp']
        self.assertEqual(durable_timestamp,
                         rebuilt_hdrs.get('X-Backend-Durable-Timestamp'))
        # proxy GET should still return v1 with correct data
        headers, etag = self.proxy_get()
        self.assertEqual(self.etag, etag)
        self.assertEqual(durable_timestamp,
                         headers.get('X-Backend-Data-Timestamp'))


class TestReconstructorRebuildUTF8(TestReconstructorRebuild):

    def _make_name(self, prefix):
        return b'%s\xc3\xa8-%s' % (
            prefix.encode(), str(uuid.uuid4()).encode())


if __name__ == "__main__":
    unittest.main()
