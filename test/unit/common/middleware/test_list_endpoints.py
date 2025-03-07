# Copyright (c) 2012 OpenStack Foundation
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

import array
import json
import unittest
from tempfile import mkdtemp
from shutil import rmtree

import os
from unittest import mock
from swift.common import ring, utils
from swift.common.utils import split_path
from swift.common.swob import Request, Response
from swift.common.middleware import list_endpoints
from swift.common.storage_policy import StoragePolicy, POLICIES
from test.unit import patch_policies


class FakeApp(object):
    def __call__(self, env, start_response):
        return Response(body="FakeApp")(env, start_response)


def start_response(*args):
    pass


@patch_policies([StoragePolicy(0, 'zero', False),
                StoragePolicy(1, 'one', True)])
class TestListEndpoints(unittest.TestCase):
    def setUp(self):
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b''
        self.testdir = mkdtemp()

        accountgz = os.path.join(self.testdir, 'account.ring.gz')
        containergz = os.path.join(self.testdir, 'container.ring.gz')
        objectgz = os.path.join(self.testdir, 'object.ring.gz')
        objectgz_1 = os.path.join(self.testdir, 'object-1.ring.gz')
        self.policy_to_test = 0
        self.expected_path = ('v1', 'a', 'c', 'o1')

        # Let's make the rings slightly different so we can test
        # that the correct ring is consulted (e.g. we don't consult
        # the object ring to get nodes for a container)
        intended_replica2part2dev_id_a = [
            array.array('H', [3, 1, 3, 1]),
            array.array('H', [0, 3, 1, 4]),
            array.array('H', [1, 4, 0, 3])]
        intended_replica2part2dev_id_c = [
            array.array('H', [4, 3, 0, 1]),
            array.array('H', [0, 1, 3, 4]),
            array.array('H', [3, 4, 0, 1])]
        intended_replica2part2dev_id_o = [
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [3, 4, 3, 4])]
        intended_replica2part2dev_id_o_1 = [
            array.array('H', [1, 0, 1, 0]),
            array.array('H', [1, 0, 1, 0]),
            array.array('H', [4, 3, 4, 3])]
        intended_devs = [{'id': 0, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6200,
                          'device': 'sda1'},
                         {'id': 1, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6200,
                          'device': 'sdb1'},
                         None,
                         {'id': 3, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.1', 'port': 6200,
                          'device': 'sdc1'},
                         {'id': 4, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.2', 'port': 6200,
                          'device': 'sdd1'}]
        intended_part_shift = 30
        ring.RingData(intended_replica2part2dev_id_a,
                      intended_devs, intended_part_shift).save(accountgz)
        ring.RingData(intended_replica2part2dev_id_c,
                      intended_devs, intended_part_shift).save(containergz)
        ring.RingData(intended_replica2part2dev_id_o,
                      intended_devs, intended_part_shift).save(objectgz)
        ring.RingData(intended_replica2part2dev_id_o_1,
                      intended_devs, intended_part_shift).save(objectgz_1)

        self.app = FakeApp()
        self.list_endpoints = list_endpoints.filter_factory(
            {'swift_dir': self.testdir})(self.app)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def FakeGetInfo(self, env, app, swift_source=None):
        info = {'status': 0, 'sync_key': None, 'meta': {},
                'cors': {'allow_origin': None, 'expose_headers': None,
                         'max_age': None},
                'sysmeta': {}, 'read_acl': None,
                'object_count': None, 'write_acl': None, 'versions': None,
                'bytes': None}
        info['storage_policy'] = self.policy_to_test
        (version, account, container, unused) = \
            split_path(env['PATH_INFO'], 3, 4, True)
        self.assertEqual((version, account, container),
                         self.expected_path[:3])
        return info

    def test_parse_response_version(self):
        expectations = {
            '': 1.0,  # legacy compat
            '/1': 1.0,
            '/v1': 1.0,
            '/1.0': 1.0,
            '/v1.0': 1.0,
            '/2': 2.0,
            '/v2': 2.0,
            '/2.0': 2.0,
            '/v2.0': 2.0,
        }
        accounts = (
            'AUTH_test',
            'test',
            'verybadreseller_prefix'
            'verybadaccount'
        )
        for expected_account in accounts:
            for version, expected in expectations.items():
                path = '/endpoints%s/%s/c/o' % (version, expected_account)
                req = Request.blank(path)
                version, account, container, obj = \
                    self.list_endpoints._parse_path(req)
                try:
                    self.assertEqual(version, expected)
                    self.assertEqual(account, expected_account)
                except AssertionError:
                    self.fail('Unexpected result from parse path %r: %r != %r'
                              % (path, (version, account),
                                 (expected, expected_account)))

    def test_parse_version_that_looks_like_account(self):
        """
        Demonstrate the failure mode for versions that look like accounts,
        if you can make _parse_path better and this is the *only* test that
        fails you can delete it ;)
        """
        bad_versions = (
            'v_3',
            'verybadreseller_prefix',
        )
        for bad_version in bad_versions:
            req = Request.blank('/endpoints/%s/a/c/o' % bad_version)
            version, account, container, obj = \
                self.list_endpoints._parse_path(req)
            self.assertEqual(version, 1.0)
            self.assertEqual(account, bad_version)
            self.assertEqual(container, 'a')
            self.assertEqual(obj, 'c/o')

    def test_parse_account_that_looks_like_version(self):
        """
        Demonstrate the failure mode for accounts that looks like versions,
        if you can make _parse_path better and this is the *only* test that
        fails you can delete it ;)
        """
        bad_accounts = (
            'v3.0', 'verybaddaccountwithnoprefix',
        )
        for bad_account in bad_accounts:
            req = Request.blank('/endpoints/%s/c/o' % bad_account)
            self.assertRaises(ValueError,
                              self.list_endpoints._parse_path, req)
        even_worse_accounts = {
            'v1': 1.0,
            'v2.0': 2.0,
        }
        for bad_account, guessed_version in even_worse_accounts.items():
            req = Request.blank('/endpoints/%s/c/o' % bad_account)
            version, account, container, obj = \
                self.list_endpoints._parse_path(req)
            self.assertEqual(version, guessed_version)
            self.assertEqual(account, 'c')
            self.assertEqual(container, 'o')
            self.assertIsNone(obj)

    def test_get_object_ring(self):
        self.assertEqual(isinstance(self.list_endpoints.get_object_ring(0),
                                    ring.Ring), True)
        self.assertEqual(isinstance(self.list_endpoints.get_object_ring(1),
                                    ring.Ring), True)
        self.assertRaises(ValueError, self.list_endpoints.get_object_ring, 99)

    def test_parse_path_no_version_specified(self):
        req = Request.blank('/endpoints/a/c/o1')
        version, account, container, obj = \
            self.list_endpoints._parse_path(req)
        self.assertEqual(account, 'a')
        self.assertEqual(container, 'c')
        self.assertEqual(obj, 'o1')

    def test_parse_path_with_valid_version(self):
        req = Request.blank('/endpoints/v2/a/c/o1')
        version, account, container, obj = \
            self.list_endpoints._parse_path(req)
        self.assertEqual(version, 2.0)
        self.assertEqual(account, 'a')
        self.assertEqual(container, 'c')
        self.assertEqual(obj, 'o1')

    def test_parse_path_with_invalid_version(self):
        req = Request.blank('/endpoints/v3/a/c/o1')
        self.assertRaises(ValueError, self.list_endpoints._parse_path,
                          req)

    def test_parse_path_with_no_account(self):
        bad_paths = ('v1', 'v2', '')
        for path in bad_paths:
            req = Request.blank('/endpoints/%s' % path)
            try:
                self.list_endpoints._parse_path(req)
                self.fail('Expected ValueError to be raised')
            except ValueError as err:
                self.assertEqual(str(err), 'No account specified')

    def test_get_endpoint(self):
        # Expected results for objects taken from test_ring
        # Expected results for others computed by manually invoking
        # ring.get_nodes().
        resp = Request.blank('/endpoints/a/c/o1').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.1.1:6200/sdb1/1/a/c/o1",
            "http://10.1.2.2:6200/sdd1/1/a/c/o1"
        ])

        # test policies with no version endpoint name
        expected = [[
                    "http://10.1.1.1:6200/sdb1/1/a/c/o1",
                    "http://10.1.2.2:6200/sdd1/1/a/c/o1"], [
                    "http://10.1.1.1:6200/sda1/1/a/c/o1",
                    "http://10.1.2.1:6200/sdc1/1/a/c/o1"
                    ]]
        PATCHGI = 'swift.common.middleware.list_endpoints.get_container_info'
        for pol in POLICIES:
            self.policy_to_test = pol.idx
            with mock.patch(PATCHGI, self.FakeGetInfo):
                resp = Request.blank('/endpoints/a/c/o1').get_response(
                    self.list_endpoints)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            self.assertEqual(json.loads(resp.body),
                             expected[pol.idx])

        # Here, 'o1/' is the object name.
        resp = Request.blank('/endpoints/a/c/o1/').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.1.1:6200/sdb1/3/a/c/o1/",
            "http://10.1.2.2:6200/sdd1/3/a/c/o1/"
        ])

        resp = Request.blank('/endpoints/a/c2').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.1.1:6200/sda1/2/a/c2",
            "http://10.1.2.1:6200/sdc1/2/a/c2"
        ])

        resp = Request.blank('/endpoints/a1').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.2.1:6200/sdc1/0/a1",
            "http://10.1.1.1:6200/sda1/0/a1",
            "http://10.1.1.1:6200/sdb1/0/a1"
        ])

        resp = Request.blank('/endpoints/').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 400)

        resp = Request.blank('/endpoints/a/c 2').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.1.1:6200/sdb1/3/a/c%202",
            "http://10.1.2.2:6200/sdd1/3/a/c%202"
        ])

        resp = Request.blank('/endpoints/a/c%202').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.1.1:6200/sdb1/3/a/c%202",
            "http://10.1.2.2:6200/sdd1/3/a/c%202"
        ])

        resp = Request.blank('/endpoints/ac%20count/con%20tainer/ob%20ject') \
            .get_response(self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(json.loads(resp.body), [
            "http://10.1.1.1:6200/sdb1/3/ac%20count/con%20tainer/ob%20ject",
            "http://10.1.2.2:6200/sdd1/3/ac%20count/con%20tainer/ob%20ject"
        ])

        resp = Request.blank('/endpoints/a/c/o1', {'REQUEST_METHOD': 'POST'}) \
            .get_response(self.list_endpoints)
        self.assertEqual(resp.status_int, 405)
        self.assertEqual(resp.status, '405 Method Not Allowed')
        self.assertEqual(resp.headers['allow'], 'GET')

        resp = Request.blank('/not-endpoints').get_response(
            self.list_endpoints)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body, b'FakeApp')

        # test policies with custom endpoint name
        for pol in POLICIES:
            # test custom path with trailing slash
            custom_path_le = list_endpoints.filter_factory({
                'swift_dir': self.testdir,
                'list_endpoints_path': '/some/another/path/'
            })(self.app)
            self.policy_to_test = pol.idx
            with mock.patch(PATCHGI, self.FakeGetInfo):
                resp = Request.blank('/some/another/path/a/c/o1') \
                    .get_response(custom_path_le)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            self.assertEqual(json.loads(resp.body),
                             expected[pol.idx])

            # test custom path without trailing slash
            custom_path_le = list_endpoints.filter_factory({
                'swift_dir': self.testdir,
                'list_endpoints_path': '/some/another/path'
            })(self.app)
            self.policy_to_test = pol.idx
            with mock.patch(PATCHGI, self.FakeGetInfo):
                resp = Request.blank('/some/another/path/a/c/o1') \
                    .get_response(custom_path_le)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'application/json')
            self.assertEqual(json.loads(resp.body),
                             expected[pol.idx])

    def test_v1_response(self):
        req = Request.blank('/endpoints/v1/a/c/o1')
        resp = req.get_response(self.list_endpoints)
        expected = ["http://10.1.1.1:6200/sdb1/1/a/c/o1",
                    "http://10.1.2.2:6200/sdd1/1/a/c/o1"]
        self.assertEqual(json.loads(resp.body), expected)

    def test_v2_obj_response(self):
        req = Request.blank('/endpoints/v2/a/c/o1')
        resp = req.get_response(self.list_endpoints)
        expected = {
            'endpoints': ["http://10.1.1.1:6200/sdb1/1/a/c/o1",
                          "http://10.1.2.2:6200/sdd1/1/a/c/o1"],
            'headers': {'X-Backend-Storage-Policy-Index': "0"},
        }
        self.assertEqual(json.loads(resp.body), expected)
        for policy in POLICIES:
            patch_path = 'swift.common.middleware.list_endpoints' \
                '.get_container_info'
            mock_get_container_info = lambda *args, **kwargs: \
                {'storage_policy': int(policy)}
            with mock.patch(patch_path, mock_get_container_info):
                resp = req.get_response(self.list_endpoints)
            part, nodes = policy.object_ring.get_nodes('a', 'c', 'o1')
            [node.update({'part': part}) for node in nodes]
            path = 'http://%(ip)s:%(port)s/%(device)s/%(part)s/a/c/o1'
            expected = {
                'headers': {
                    'X-Backend-Storage-Policy-Index': str(int(policy))},
                'endpoints': [path % node for node in nodes],
            }
            self.assertEqual(json.loads(resp.body), expected)

    def test_v2_non_obj_response(self):
        # account
        req = Request.blank('/endpoints/v2/a')
        resp = req.get_response(self.list_endpoints)
        expected = {
            'endpoints': ["http://10.1.2.1:6200/sdc1/0/a",
                          "http://10.1.1.1:6200/sda1/0/a",
                          "http://10.1.1.1:6200/sdb1/0/a"],
            'headers': {},
        }
        # container
        self.assertEqual(json.loads(resp.body), expected)
        req = Request.blank('/endpoints/v2/a/c')
        resp = req.get_response(self.list_endpoints)
        expected = {
            'endpoints': ["http://10.1.2.2:6200/sdd1/0/a/c",
                          "http://10.1.1.1:6200/sda1/0/a/c",
                          "http://10.1.2.1:6200/sdc1/0/a/c"],
            'headers': {},
        }
        self.assertEqual(json.loads(resp.body), expected)

    def test_version_account_response(self):
        req = Request.blank('/endpoints/a')
        resp = req.get_response(self.list_endpoints)
        expected = ["http://10.1.2.1:6200/sdc1/0/a",
                    "http://10.1.1.1:6200/sda1/0/a",
                    "http://10.1.1.1:6200/sdb1/0/a"]
        self.assertEqual(json.loads(resp.body), expected)
        req = Request.blank('/endpoints/v1.0/a')
        resp = req.get_response(self.list_endpoints)
        self.assertEqual(json.loads(resp.body), expected)

        req = Request.blank('/endpoints/v2/a')
        resp = req.get_response(self.list_endpoints)
        expected = {
            'endpoints': ["http://10.1.2.1:6200/sdc1/0/a",
                          "http://10.1.1.1:6200/sda1/0/a",
                          "http://10.1.1.1:6200/sdb1/0/a"],
            'headers': {},
        }
        self.assertEqual(json.loads(resp.body), expected)


if __name__ == '__main__':
    unittest.main()
