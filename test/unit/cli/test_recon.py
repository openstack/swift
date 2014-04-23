# Copyright (c) 2013 Christian Schwede <christian.schwede@enovance.com>
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

import json
import mock
import os
import random
import string
import tempfile
import time
import unittest
import urlparse

from eventlet.green import urllib2

from swift.cli import recon
from swift.common import utils
from swift.common.ring import builder


class TestHelpers(unittest.TestCase):
    def test_seconds2timeunit(self):
        self.assertEqual(recon.seconds2timeunit(10), (10, 'seconds'))
        self.assertEqual(recon.seconds2timeunit(600), (10, 'minutes'))
        self.assertEqual(recon.seconds2timeunit(36000), (10, 'hours'))
        self.assertEqual(recon.seconds2timeunit(60 * 60 * 24 * 10),
                         (10, 'days'))

    def test_size_suffix(self):
        self.assertEqual(recon.size_suffix(5 * 10 ** 2), '500 bytes')
        self.assertEqual(recon.size_suffix(5 * 10 ** 3), '5 kB')
        self.assertEqual(recon.size_suffix(5 * 10 ** 6), '5 MB')
        self.assertEqual(recon.size_suffix(5 * 10 ** 9), '5 GB')
        self.assertEqual(recon.size_suffix(5 * 10 ** 12), '5 TB')
        self.assertEqual(recon.size_suffix(5 * 10 ** 15), '5 PB')
        self.assertEqual(recon.size_suffix(5 * 10 ** 18), '5 EB')
        self.assertEqual(recon.size_suffix(5 * 10 ** 21), '5 ZB')


class TestScout(unittest.TestCase):
    def setUp(self, *_args, **_kwargs):
        self.scout_instance = recon.Scout("type", suppress_errors=True)
        self.url = 'http://127.0.0.1:8080/recon/type'

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_scout_ok(self, mock_urlopen):
        mock_urlopen.return_value.read = lambda: json.dumps([])
        url, content, status = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.url)
        self.assertEqual(content, [])
        self.assertEqual(status, 200)

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_scout_url_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.URLError("")
        url, content, status = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertTrue(isinstance(content, urllib2.URLError))
        self.assertEqual(url, self.url)
        self.assertEqual(status, -1)

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_scout_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.HTTPError(
            self.url, 404, "Internal error", None, None)
        url, content, status = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.url)
        self.assertTrue(isinstance(content, urllib2.HTTPError))
        self.assertEqual(status, 404)


class TestRecon(unittest.TestCase):
    def setUp(self, *_args, **_kwargs):
        self.recon_instance = recon.SwiftRecon()
        self.swift_dir = tempfile.gettempdir()
        self.ring_name = "test_object_%s" % (
            ''.join(random.choice(string.digits) for x in range(6)))
        self.tmpfile_name = "%s/%s.ring.gz" % (self.swift_dir, self.ring_name)

        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'

    def tearDown(self, *_args, **_kwargs):
        try:
            os.remove(self.tmpfile_name)
        except:
            pass

    def test_gen_stats(self):
        stats = self.recon_instance._gen_stats((1, 4, 10, None), 'Sample')
        self.assertEqual(stats.get('name'), 'Sample')
        self.assertEqual(stats.get('average'), 5.0)
        self.assertEqual(stats.get('high'), 10)
        self.assertEqual(stats.get('reported'), 3)
        self.assertEqual(stats.get('low'), 1)
        self.assertEqual(stats.get('total'), 15)
        self.assertEqual(stats.get('number_none'), 1)
        self.assertEqual(stats.get('perc_none'), 25.0)

    def test_ptime(self):
        with mock.patch('time.localtime') as mock_localtime:
            mock_localtime.return_value = time.struct_time(
                (2013, 12, 17, 10, 0, 0, 1, 351, 0))

            timestamp = self.recon_instance._ptime(1387274400)
            self.assertEqual(timestamp, "2013-12-17 10:00:00")
            mock_localtime.assertCalledWith(1387274400)

            timestamp2 = self.recon_instance._ptime()
            self.assertEqual(timestamp2, "2013-12-17 10:00:00")
            mock_localtime.assertCalledWith()

    def test_get_devices(self):
        ringbuilder = builder.RingBuilder(2, 3, 1)
        ringbuilder.add_dev({'id': 0, 'zone': 0, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 10000,
                             'device': 'sda1', 'region': 0})
        ringbuilder.add_dev({'id': 1, 'zone': 1, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 10001,
                             'device': 'sda1', 'region': 0})
        ringbuilder.rebalance()
        ringbuilder.get_ring().save(self.tmpfile_name)

        ips = self.recon_instance.get_devices(
            None, self.swift_dir, self.ring_name)
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001)]), ips)

        ips = self.recon_instance.get_devices(
            0, self.swift_dir, self.ring_name)
        self.assertEqual(set([('127.0.0.1', 10000)]), ips)

        ips = self.recon_instance.get_devices(
            1, self.swift_dir, self.ring_name)
        self.assertEqual(set([('127.0.0.1', 10001)]), ips)


class TestReconCommands(unittest.TestCase):
    def setUp(self):
        self.recon = recon.SwiftRecon()
        self.hosts = set([('127.0.0.1', 10000)])

    def mock_responses(self, resps):

        def fake_urlopen(url, timeout):
            scheme, netloc, path, _, _, _ = urlparse.urlparse(url)
            self.assertEqual(scheme, 'http')  # can't handle anything else
            self.assertTrue(path.startswith('/recon/'))

            if ':' in netloc:
                host, port = netloc.split(':', 1)
                port = int(port)
            else:
                host = netloc
                port = 80

            response_body = resps[(host, port, path[7:])]

            resp = mock.MagicMock()
            resp.read = mock.MagicMock(side_effect=[response_body])
            return resp

        return mock.patch('eventlet.green.urllib2.urlopen', fake_urlopen)

    def test_get_swiftconfmd5(self):
        hosts = set([('10.1.1.1', 10000),
                     ('10.2.2.2', 10000)])
        cksum = '729cf900f2876dead617d088ece7fe8c'

        responses = {
            ('10.1.1.1', 10000, 'swiftconfmd5'):
            json.dumps({'/etc/swift/swift.conf': cksum}),
            ('10.2.2.2', 10000, 'swiftconfmd5'):
            json.dumps({'/etc/swift/swift.conf': cksum})}

        printed = []
        with self.mock_responses(responses):
            with mock.patch.object(self.recon, '_md5_file', lambda _: cksum):
                self.recon.get_swiftconfmd5(hosts, printfn=printed.append)

        output = '\n'.join(printed) + '\n'
        self.assertTrue("2/2 hosts matched" in output)

    def test_get_swiftconfmd5_mismatch(self):
        hosts = set([('10.1.1.1', 10000),
                     ('10.2.2.2', 10000)])
        cksum = '29d5912b1fcfcc1066a7f51412769c1d'

        responses = {
            ('10.1.1.1', 10000, 'swiftconfmd5'):
            json.dumps({'/etc/swift/swift.conf': cksum}),
            ('10.2.2.2', 10000, 'swiftconfmd5'):
            json.dumps({'/etc/swift/swift.conf': 'bogus'})}

        printed = []
        with self.mock_responses(responses):
            with mock.patch.object(self.recon, '_md5_file', lambda _: cksum):
                self.recon.get_swiftconfmd5(hosts, printfn=printed.append)

        output = '\n'.join(printed) + '\n'
        self.assertTrue("1/2 hosts matched" in output)
        self.assertTrue("http://10.2.2.2:10000/recon/swiftconfmd5 (bogus) "
                        "doesn't match on disk md5sum" in output)
