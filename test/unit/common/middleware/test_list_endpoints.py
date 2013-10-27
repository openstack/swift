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
import unittest
from shutil import rmtree

import os
from swift.common import ring, utils
from swift.common.utils import json
from swift.common.swob import Request, Response
from swift.common.middleware import list_endpoints


class FakeApp(object):
    def __call__(self, env, start_response):
        return Response(body="FakeApp")(env, start_response)


def start_response(*args):
    pass


class TestListEndpoints(unittest.TestCase):
    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        self.testdir = os.path.join(os.path.dirname(__file__), 'ring')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)

        accountgz = os.path.join(self.testdir, 'account.ring.gz')
        containergz = os.path.join(self.testdir, 'container.ring.gz')
        objectgz = os.path.join(self.testdir, 'object.ring.gz')

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
        intended_devs = [{'id': 0, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6000,
                          'device': 'sda1'},
                         {'id': 1, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6000,
                          'device': 'sdb1'},
                         None,
                         {'id': 3, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.1', 'port': 6000,
                          'device': 'sdc1'},
                         {'id': 4, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.2', 'port': 6000,
                          'device': 'sdd1'}]
        intended_part_shift = 30
        ring.RingData(intended_replica2part2dev_id_a,
                      intended_devs, intended_part_shift).save(accountgz)
        ring.RingData(intended_replica2part2dev_id_c,
                      intended_devs, intended_part_shift).save(containergz)
        ring.RingData(intended_replica2part2dev_id_o,
                      intended_devs, intended_part_shift).save(objectgz)

        self.app = FakeApp()
        self.list_endpoints = list_endpoints.filter_factory(
            {'swift_dir': self.testdir})(self.app)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_get_endpoint(self):
        # Expected results for objects taken from test_ring
        # Expected results for others computed by manually invoking
        # ring.get_nodes().
        resp = Request.blank('/endpoints/a/c/o1').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/1/a/c/o1",
            "http://10.1.2.2:6000/sdd1/1/a/c/o1"
        ])

        # Here, 'o1/' is the object name.
        resp = Request.blank('/endpoints/a/c/o1/').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/3/a/c/o1/",
            "http://10.1.2.2:6000/sdd1/3/a/c/o1/"
        ])

        resp = Request.blank('/endpoints/a/c2').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sda1/2/a/c2",
            "http://10.1.2.1:6000/sdc1/2/a/c2"
        ])

        resp = Request.blank('/endpoints/a1').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.2.1:6000/sdc1/0/a1",
            "http://10.1.1.1:6000/sda1/0/a1",
            "http://10.1.1.1:6000/sdb1/0/a1"
        ])

        resp = Request.blank('/endpoints/').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 400)

        resp = Request.blank('/endpoints/a/c 2').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/3/a/c%202",
            "http://10.1.2.2:6000/sdd1/3/a/c%202"
        ])

        resp = Request.blank('/endpoints/a/c%202').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/3/a/c%202",
            "http://10.1.2.2:6000/sdd1/3/a/c%202"
        ])

        resp = Request.blank('/endpoints/ac%20count/con%20tainer/ob%20ject') \
            .get_response(self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/3/ac%20count/con%20tainer/ob%20ject",
            "http://10.1.2.2:6000/sdd1/3/ac%20count/con%20tainer/ob%20ject"
        ])

        resp = Request.blank('/endpoints/a/c/o1', {'REQUEST_METHOD': 'POST'}) \
            .get_response(self.list_endpoints)
        self.assertEquals(resp.status_int, 405)
        self.assertEquals(resp.status, '405 Method Not Allowed')
        self.assertEquals(resp.headers['allow'], 'GET')

        resp = Request.blank('/not-endpoints').get_response(
            self.list_endpoints)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.status, '200 OK')
        self.assertEquals(resp.body, 'FakeApp')

        # test custom path with trailing slash
        custom_path_le = list_endpoints.filter_factory({
            'swift_dir': self.testdir,
            'list_endpoints_path': '/some/another/path/'
        })(self.app)
        resp = Request.blank('/some/another/path/a/c/o1') \
            .get_response(custom_path_le)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/1/a/c/o1",
            "http://10.1.2.2:6000/sdd1/1/a/c/o1"
        ])

        # test ustom path without trailing slash
        custom_path_le = list_endpoints.filter_factory({
            'swift_dir': self.testdir,
            'list_endpoints_path': '/some/another/path'
        })(self.app)
        resp = Request.blank('/some/another/path/a/c/o1') \
            .get_response(custom_path_le)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(json.loads(resp.body), [
            "http://10.1.1.1:6000/sdb1/1/a/c/o1",
            "http://10.1.2.2:6000/sdd1/1/a/c/o1"
        ])


if __name__ == '__main__':
    unittest.main()
