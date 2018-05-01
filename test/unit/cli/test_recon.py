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
import re
import tempfile
import time
import unittest
import shutil
import string
import sys
import six

from eventlet.green import socket
from six import StringIO
from six.moves import urllib

from swift.cli import recon
from swift.common import utils
from swift.common.ring import builder
from swift.common.ring import utils as ring_utils
from swift.common.storage_policy import StoragePolicy, POLICIES
from test.unit import patch_policies

if six.PY3:
    from eventlet.green.urllib import request as urllib2
    GREEN_URLLIB_URLOPEN = 'eventlet.green.urllib.request.urlopen'
else:
    from eventlet.green import urllib2
    GREEN_URLLIB_URLOPEN = 'eventlet.green.urllib2.urlopen'


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
        self.server_type_url = 'http://127.0.0.1:8080/'

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_ok(self, mock_urlopen):
        mock_urlopen.return_value.read = lambda: json.dumps([])
        url, content, status, ts_start, ts_end = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.url)
        self.assertEqual(content, [])
        self.assertEqual(status, 200)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_url_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.URLError("")
        url, content, status, ts_start, ts_end = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertIsInstance(content, urllib2.URLError)
        self.assertEqual(url, self.url)
        self.assertEqual(status, -1)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.HTTPError(
            self.url, 404, "Internal error", None, None)
        url, content, status, ts_start, ts_end = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.url)
        self.assertIsInstance(content, urllib2.HTTPError)
        self.assertEqual(status, 404)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_socket_timeout(self, mock_urlopen):
        mock_urlopen.side_effect = socket.timeout("timeout")
        url, content, status, ts_start, ts_end = self.scout_instance.scout(
            ("127.0.0.1", "8080"))
        self.assertIsInstance(content, socket.timeout)
        self.assertEqual(url, self.url)
        self.assertEqual(status, -1)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_server_type_ok(self, mock_urlopen):
        def getheader(name):
            d = {'Server': 'server-type'}
            return d.get(name)
        mock_urlopen.return_value.info.return_value.get = getheader
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.server_type_url)
        self.assertEqual(content, 'server-type')
        self.assertEqual(status, 200)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_server_type_url_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.URLError("")
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertIsInstance(content, urllib2.URLError)
        self.assertEqual(url, self.server_type_url)
        self.assertEqual(status, -1)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_server_type_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.HTTPError(
            self.server_type_url, 404, "Internal error", None, None)
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.server_type_url)
        self.assertIsInstance(content, urllib2.HTTPError)
        self.assertEqual(status, 404)

    @mock.patch(GREEN_URLLIB_URLOPEN)
    def test_scout_server_type_socket_timeout(self, mock_urlopen):
        mock_urlopen.side_effect = socket.timeout("timeout")
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertIsInstance(content, socket.timeout)
        self.assertEqual(url, self.server_type_url)
        self.assertEqual(status, -1)


@patch_policies
class TestRecon(unittest.TestCase):
    def setUp(self, *_args, **_kwargs):
        self.swift_conf_file = utils.SWIFT_CONF_FILE
        self.recon_instance = recon.SwiftRecon()
        self.swift_dir = tempfile.mkdtemp()
        self.ring_name = POLICIES.legacy.ring_name
        self.tmpfile_name = os.path.join(
            self.swift_dir, self.ring_name + '.ring.gz')
        self.ring_name2 = POLICIES[1].ring_name
        self.tmpfile_name2 = os.path.join(
            self.swift_dir, self.ring_name2 + '.ring.gz')

        swift_conf = os.path.join(self.swift_dir, 'swift.conf')
        self.policy_name = ''.join(random.sample(string.ascii_letters, 20))
        swift_conf_data = '''
[swift-hash]
swift_hash_path_suffix = changeme

[storage-policy:0]
name = default
default = yes

[storage-policy:1]
name = unu
aliases = %s
''' % self.policy_name
        with open(swift_conf, "wb") as sc:
            sc.write(swift_conf_data.encode('utf8'))

    def tearDown(self, *_args, **_kwargs):
        utils.SWIFT_CONF_FILE = self.swift_conf_file
        shutil.rmtree(self.swift_dir, ignore_errors=True)

    def _make_object_rings(self):
        ringbuilder = builder.RingBuilder(2, 3, 1)
        devs = [
            'r0z0-127.0.0.1:10000/sda1',
            'r0z1-127.0.0.1:10001/sda1',
            'r1z0-127.0.0.1:10002/sda1',
            'r1z1-127.0.0.1:10003/sda1',
        ]
        for raw_dev_str in devs:
            dev = ring_utils.parse_add_value(raw_dev_str)
            dev['weight'] = 1.0
            ringbuilder.add_dev(dev)
        ringbuilder.rebalance()
        ringbuilder.get_ring().save(self.tmpfile_name)

        ringbuilder = builder.RingBuilder(2, 2, 1)
        devs = [
            'r0z0-127.0.0.1:10000/sda1',
            'r0z1-127.0.0.2:10004/sda1',
        ]
        for raw_dev_str in devs:
            dev = ring_utils.parse_add_value(raw_dev_str)
            dev['weight'] = 1.0
            ringbuilder.add_dev(dev)
        ringbuilder.rebalance()
        ringbuilder.get_ring().save(self.tmpfile_name2)

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
        with mock.patch('time.gmtime') as mock_gmtime:
            mock_gmtime.return_value = time.struct_time(
                (2013, 12, 17, 10, 0, 0, 1, 351, 0))

            timestamp = self.recon_instance._ptime(1387274400)
            self.assertEqual(timestamp, "2013-12-17 10:00:00")
            mock_gmtime.assert_called_with(1387274400)

            timestamp2 = self.recon_instance._ptime()
            self.assertEqual(timestamp2, "2013-12-17 10:00:00")
            mock_gmtime.assert_called_with()

    def test_get_hosts(self):
        self._make_object_rings()

        ips = self.recon_instance.get_hosts(
            None, None, self.swift_dir, [self.ring_name])
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001),
                 ('127.0.0.1', 10002), ('127.0.0.1', 10003)]), ips)

        ips = self.recon_instance.get_hosts(
            0, None, self.swift_dir, [self.ring_name])
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001)]), ips)

        ips = self.recon_instance.get_hosts(
            1, None, self.swift_dir, [self.ring_name])
        self.assertEqual(
            set([('127.0.0.1', 10002), ('127.0.0.1', 10003)]), ips)

        ips = self.recon_instance.get_hosts(
            0, 0, self.swift_dir, [self.ring_name])
        self.assertEqual(set([('127.0.0.1', 10000)]), ips)

        ips = self.recon_instance.get_hosts(
            1, 1, self.swift_dir, [self.ring_name])
        self.assertEqual(set([('127.0.0.1', 10003)]), ips)

        ips = self.recon_instance.get_hosts(
            None, None, self.swift_dir, [self.ring_name, self.ring_name2])
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001),
                 ('127.0.0.1', 10002), ('127.0.0.1', 10003),
                 ('127.0.0.2', 10004)]), ips)

        ips = self.recon_instance.get_hosts(
            0, None, self.swift_dir, [self.ring_name, self.ring_name2])
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001),
                 ('127.0.0.2', 10004)]), ips)

        ips = self.recon_instance.get_hosts(
            1, None, self.swift_dir, [self.ring_name, self.ring_name2])
        self.assertEqual(
            set([('127.0.0.1', 10002), ('127.0.0.1', 10003)]), ips)

        ips = self.recon_instance.get_hosts(
            0, 1, self.swift_dir, [self.ring_name, self.ring_name2])
        self.assertEqual(set([('127.0.0.1', 10001),
                              ('127.0.0.2', 10004)]), ips)

    def test_get_error_ringnames(self):
        # create invalid ring name files
        invalid_ring_file_names = ('object.sring.gz',
                                   'object-1.sring.gz',
                                   'broken')
        for invalid_ring in invalid_ring_file_names:
            ring_path = os.path.join(self.swift_dir, invalid_ring)
            with open(ring_path, 'w'):
                pass

        hosts = [("127.0.0.1", "8080")]
        self.recon_instance.verbose = True
        self.recon_instance.server_type = 'object'
        stdout = StringIO()
        with mock.patch('sys.stdout', new=stdout), \
                mock.patch('swift.common.utils.md5'):
            self.recon_instance.get_ringmd5(hosts, self.swift_dir)
        output = stdout.getvalue()
        self.assertNotIn('On disk ', output)

    def test_get_ringmd5(self):
        for server_type in ('account', 'container', 'object', 'object-1'):
            ring_name = '%s.ring.gz' % server_type
            ring_file = os.path.join(self.swift_dir, ring_name)
            open(ring_file, 'w')

        empty_file_hash = 'd41d8cd98f00b204e9800998ecf8427e'
        bad_file_hash = '00000000000000000000000000000000'
        hosts = [("127.0.0.1", "8080")]
        with mock.patch('swift.cli.recon.Scout') as mock_scout:
            scout_instance = mock.MagicMock()
            url = 'http://%s:%s/recon/ringmd5' % hosts[0]
            response = {
                '/etc/swift/account.ring.gz': empty_file_hash,
                '/etc/swift/container.ring.gz': empty_file_hash,
                '/etc/swift/object.ring.gz': empty_file_hash,
                '/etc/swift/object-1.ring.gz': empty_file_hash,
            }
            status = 200
            scout_instance.scout.return_value = (url, response, status, 0, 0)
            mock_scout.return_value = scout_instance
            mock_hash = mock.MagicMock()

            # Check correct account, container and object ring hashes
            for server_type in ('account', 'container', 'object'):
                self.recon_instance.server_type = server_type
                stdout = StringIO()
                with mock.patch('sys.stdout', new=stdout), \
                        mock.patch('swift.common.utils.md5', new=mock_hash):
                    mock_hash.return_value.hexdigest.return_value = \
                        empty_file_hash
                    self.recon_instance.get_ringmd5(hosts, self.swift_dir)
                output = stdout.getvalue()
                expected = '1/1 hosts matched'
                found = False
                for line in output.splitlines():
                    if '!!' in line:
                        self.fail('Unexpected Error in output: %r' % line)
                    if expected in line:
                        found = True
                if not found:
                    self.fail('Did not find expected substring %r '
                              'in output:\n%s' % (expected, output))

            # Check bad container ring hash
            self.recon_instance.server_type = 'container'
            response = {
                '/etc/swift/account.ring.gz': empty_file_hash,
                '/etc/swift/container.ring.gz': bad_file_hash,
                '/etc/swift/object.ring.gz': empty_file_hash,
                '/etc/swift/object-1.ring.gz': empty_file_hash,
            }
            scout_instance.scout.return_value = (url, response, status, 0, 0)
            mock_scout.return_value = scout_instance
            stdout = StringIO()
            with mock.patch('sys.stdout', new=stdout), \
                    mock.patch('swift.common.utils.md5', new=mock_hash):
                mock_hash.return_value.hexdigest.return_value = \
                    empty_file_hash
                self.recon_instance.get_ringmd5(hosts, self.swift_dir)
            output = stdout.getvalue()
            expected = '0/1 hosts matched'
            found = False
            for line in output.splitlines():
                if '!!' in line:
                    self.assertIn('doesn\'t match on disk md5sum', line)
                if expected in line:
                    found = True
            if not found:
                self.fail('Did not find expected substring %r '
                          'in output:\n%s' % (expected, output))

            # Check object ring, container mismatch should be ignored
            self.recon_instance.server_type = 'object'
            stdout = StringIO()
            with mock.patch('sys.stdout', new=stdout), \
                    mock.patch('swift.common.utils.md5', new=mock_hash):
                mock_hash.return_value.hexdigest.return_value = \
                    empty_file_hash
                self.recon_instance.get_ringmd5(hosts, self.swift_dir)
            output = stdout.getvalue()
            expected = '1/1 hosts matched'
            for line in output.splitlines():
                if '!!' in line:
                    self.fail('Unexpected Error in output: %r' % line)
                if expected in line:
                    found = True
            if not found:
                self.fail('Did not find expected substring %r '
                          'in output:\n%s' % (expected, output))

        # Cleanup
        self.recon_instance.server_type = 'object'
        for ring in ('account', 'container', 'object', 'object-1'):
            os.remove(os.path.join(self.swift_dir, "%s.ring.gz" % ring))

    def test_quarantine_check(self):
        hosts = [('127.0.0.1', 6010), ('127.0.0.1', 6020),
                 ('127.0.0.1', 6030), ('127.0.0.1', 6040),
                 ('127.0.0.1', 6050)]
        # sample json response from http://<host>:<port>/recon/quarantined
        responses = {6010: {'accounts': 0, 'containers': 0, 'objects': 1,
                            'policies': {'0': {'objects': 0},
                                         '1': {'objects': 1}}},
                     6020: {'accounts': 1, 'containers': 1, 'objects': 3,
                            'policies': {'0': {'objects': 1},
                                         '1': {'objects': 2}}},
                     6030: {'accounts': 2, 'containers': 2, 'objects': 5,
                            'policies': {'0': {'objects': 2},
                                         '1': {'objects': 3}}},
                     6040: {'accounts': 3, 'containers': 3, 'objects': 7,
                            'policies': {'0': {'objects': 3},
                                         '1': {'objects': 4}}},
                     # A server without storage policies enabled
                     6050: {'accounts': 0, 'containers': 0, 'objects': 4}}
        # <low> <high> <avg> <total> <Failed> <no_result> <reported>
        expected = {'objects_0': (0, 3, 1.5, 6, 0.0, 0, 4),
                    'objects_1': (1, 4, 2.5, 10, 0.0, 0, 4),
                    'objects': (1, 7, 4.0, 20, 0.0, 0, 5),
                    'accounts': (0, 3, 1.2, 6, 0.0, 0, 5),
                    'containers': (0, 3, 1.2, 6, 0.0, 0, 5)}

        def mock_scout_quarantine(app, host):
            url = 'http://%s:%s/recon/quarantined' % host
            response = responses[host[1]]
            status = 200
            return url, response, status, 0, 0

        stdout = StringIO()
        with mock.patch('swift.cli.recon.Scout.scout',
                        mock_scout_quarantine), \
                mock.patch('sys.stdout', new=stdout):
            self.recon_instance.quarantine_check(hosts)

        output = stdout.getvalue()
        r = re.compile("\[quarantined_(.*)\](.*)")
        for line in output.splitlines():
            m = r.match(line)
            if m:
                ex = expected.pop(m.group(1))
                self.assertEqual(m.group(2),
                                 " low: %s, high: %s, avg: %s, total: %s,"
                                 " Failed: %s%%, no_result: %s, reported: %s"
                                 % ex)
        self.assertFalse(expected)

    def test_async_check(self):
        hosts = [('127.0.0.1', 6011), ('127.0.0.1', 6021),
                 ('127.0.0.1', 6031), ('127.0.0.1', 6041)]
        # sample json response from http://<host>:<port>/recon/async
        responses = {6011: {'async_pending': 15},
                     6021: {'async_pending': 0},
                     6031: {'async_pending': 257},
                     6041: {'async_pending': 56}}
        # <low> <high> <avg> <total> <Failed> <no_result> <reported>
        expected = (0, 257, 82.0, 328, 0.0, 0, 4)

        def mock_scout_async(app, host):
            url = 'http://%s:%s/recon/async' % host
            response = responses[host[1]]
            status = 200
            return url, response, status, 0, 0

        stdout = StringIO()
        with mock.patch('swift.cli.recon.Scout.scout',
                        mock_scout_async), \
                mock.patch('sys.stdout', new=stdout):
            self.recon_instance.async_check(hosts)

        output = stdout.getvalue()
        r = re.compile("\[async_pending(.*)\](.*)")
        lines = output.splitlines()
        self.assertTrue(lines)
        for line in lines:
            m = r.match(line)
            if m:
                self.assertEqual(m.group(2),
                                 " low: %s, high: %s, avg: %s, total: %s,"
                                 " Failed: %s%%, no_result: %s, reported: %s"
                                 % expected)
                break
        else:
            self.fail('The expected line is not found')

    def test_umount_check(self):
        hosts = [('127.0.0.1', 6010), ('127.0.0.1', 6020),
                 ('127.0.0.1', 6030), ('127.0.0.1', 6040)]
        # sample json response from http://<host>:<port>/recon/unmounted
        responses = {6010: [{'device': 'sdb1', 'mounted': False}],
                     6020: [{'device': 'sdb2', 'mounted': False}],
                     6030: [{'device': 'sdb3', 'mounted': False}],
                     6040: [{'device': 'sdb4', 'mounted': 'bad'}]}

        expected = ['Not mounted: sdb1 on 127.0.0.1:6010',
                    'Not mounted: sdb2 on 127.0.0.1:6020',
                    'Not mounted: sdb3 on 127.0.0.1:6030',
                    'Device errors: sdb4 on 127.0.0.1:6040']

        def mock_scout_umount(app, host):
            url = 'http://%s:%s/recon/unmounted' % host
            response = responses[host[1]]
            status = 200
            return url, response, status, 0, 0

        stdout = StringIO()
        with mock.patch('swift.cli.recon.Scout.scout',
                        mock_scout_umount), \
                mock.patch('sys.stdout', new=stdout):
            self.recon_instance.umount_check(hosts)

        output = stdout.getvalue()
        r = re.compile("^Not mounted:|Device errors: .*")
        lines = output.splitlines()
        self.assertTrue(lines)
        for line in lines:
            m = r.match(line)
            if m:
                self.assertIn(line, expected)
                expected.remove(line)
        self.assertFalse(expected)

    def test_drive_audit_check(self):
        hosts = [('127.0.0.1', 6010), ('127.0.0.1', 6020),
                 ('127.0.0.1', 6030), ('127.0.0.1', 6040)]
        # sample json response from http://<host>:<port>/recon/driveaudit
        responses = {6010: {'drive_audit_errors': 15},
                     6020: {'drive_audit_errors': 0},
                     6030: {'drive_audit_errors': 257},
                     6040: {'drive_audit_errors': 56}}
        # <low> <high> <avg> <total> <Failed> <no_result> <reported>
        expected = (0, 257, 82.0, 328, 0.0, 0, 4)

        def mock_scout_driveaudit(app, host):
            url = 'http://%s:%s/recon/driveaudit' % host
            response = responses[host[1]]
            status = 200
            return url, response, status, 0, 0

        stdout = StringIO()
        with mock.patch('swift.cli.recon.Scout.scout',
                        mock_scout_driveaudit), \
                mock.patch('sys.stdout', new=stdout):
            self.recon_instance.driveaudit_check(hosts)

        output = stdout.getvalue()
        r = re.compile("\[drive_audit_errors(.*)\](.*)")
        lines = output.splitlines()
        self.assertTrue(lines)
        for line in lines:
            m = r.match(line)
            if m:
                self.assertEqual(m.group(2),
                                 " low: %s, high: %s, avg: %s, total: %s,"
                                 " Failed: %s%%, no_result: %s, reported: %s"
                                 % expected)

    def test_get_ring_names(self):
        self.recon_instance.server_type = 'not-object'
        self.assertEqual(self.recon_instance._get_ring_names(), ['not-object'])

        self.recon_instance.server_type = 'object'

        with patch_policies([StoragePolicy(0, 'zero', is_default=True)]):
            self.assertEqual(self.recon_instance._get_ring_names(),
                             ['object'])

        with patch_policies([StoragePolicy(0, 'zero', is_default=True),
                             StoragePolicy(1, 'one')]):
            self.assertEqual(self.recon_instance._get_ring_names(),
                             ['object', 'object-1'])
            self.assertEqual(self.recon_instance._get_ring_names('0'),
                             ['object'])
            self.assertEqual(self.recon_instance._get_ring_names('zero'),
                             ['object'])
            self.assertEqual(self.recon_instance._get_ring_names('1'),
                             ['object-1'])
            self.assertEqual(self.recon_instance._get_ring_names('one'),
                             ['object-1'])

            self.assertEqual(self.recon_instance._get_ring_names('3'), [])
            self.assertEqual(self.recon_instance._get_ring_names('wrong'),
                             [])

    def test_main_object_hosts_default_all_policies(self):
        self._make_object_rings()
        discovered_hosts = set()

        def server_type_check(hosts):
            for h in hosts:
                discovered_hosts.add(h)

        self.recon_instance.server_type_check = server_type_check
        with mock.patch.object(sys, 'argv', [
                "prog", "object", "--swiftdir=%s" % self.swift_dir,
                "--validate-servers"]):
            self.recon_instance.main()

        expected = set([
            ('127.0.0.1', 10000),
            ('127.0.0.1', 10001),
            ('127.0.0.1', 10002),
            ('127.0.0.1', 10003),
            ('127.0.0.2', 10004),
        ])

        self.assertEqual(expected, discovered_hosts)

    def _test_main_object_hosts_policy_name(self, policy_name='unu'):
        self._make_object_rings()
        discovered_hosts = set()

        def server_type_check(hosts):
            for h in hosts:
                discovered_hosts.add(h)

        self.recon_instance.server_type_check = server_type_check

        with mock.patch.object(sys, 'argv', [
                "prog", "object", "--swiftdir=%s" % self.swift_dir,
                "--validate-servers", '--policy', policy_name]):

            self.recon_instance.main()

        expected = set([
            ('127.0.0.1', 10000),
            ('127.0.0.2', 10004),
        ])
        self.assertEqual(expected, discovered_hosts)

    def test_main_object_hosts_default_unu(self):
        self._test_main_object_hosts_policy_name()

    def test_main_object_hosts_default_alias(self):
        self._test_main_object_hosts_policy_name(self.policy_name)

    def test_main_object_hosts_default_invalid(self):
        self._make_object_rings()
        stdout = StringIO()
        with mock.patch.object(sys, 'argv', [
                "prog", "object", "--swiftdir=%s" % self.swift_dir,
                "--validate-servers", '--policy=invalid']),\
                mock.patch('sys.stdout', stdout):
            self.assertRaises(SystemExit, recon.main)
            self.assertIn('Invalid Storage Policy', stdout.getvalue())


class TestReconCommands(unittest.TestCase):
    def setUp(self):
        self.recon = recon.SwiftRecon()
        self.hosts = set([('127.0.0.1', 10000)])

    def mock_responses(self, resps):

        def fake_urlopen(url, timeout):
            scheme, netloc, path, _, _, _ = urllib.parse.urlparse(url)
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
            resp.read = mock.MagicMock(side_effect=[
                response_body if six.PY2 else response_body.encode('utf8')])
            return resp

        return mock.patch(GREEN_URLLIB_URLOPEN, fake_urlopen)

    def test_server_type_check(self):
        hosts = [('127.0.0.1', 6010), ('127.0.0.1', 6011),
                 ('127.0.0.1', 6012)]

        # sample json response from http://<host>:<port>/
        responses = {6010: 'object-server', 6011: 'container-server',
                     6012: 'account-server'}

        def mock_scout_server_type(app, host):
            url = 'http://%s:%s/' % (host[0], host[1])
            response = responses[host[1]]
            status = 200
            return url, response, status

        stdout = StringIO()
        res_object = 'Invalid: http://127.0.0.1:6010/ is object-server'
        res_container = 'Invalid: http://127.0.0.1:6011/ is container-server'
        res_account = 'Invalid: http://127.0.0.1:6012/ is account-server'
        valid = "1/1 hosts ok, 0 error[s] while checking hosts."

        # Test for object server type - default
        with mock.patch('swift.cli.recon.Scout.scout_server_type',
                        mock_scout_server_type), \
                mock.patch('sys.stdout', new=stdout):
            self.recon.server_type_check(hosts)

        output = stdout.getvalue()
        self.assertIn(res_container, output.splitlines())
        self.assertIn(res_account, output.splitlines())
        stdout.truncate(0)

        # Test ok for object server type - default
        with mock.patch('swift.cli.recon.Scout.scout_server_type',
                        mock_scout_server_type), \
                mock.patch('sys.stdout', new=stdout):
            self.recon.server_type_check([hosts[0]])

        output = stdout.getvalue()
        self.assertIn(valid, output.splitlines())
        stdout.truncate(0)

        # Test for account server type
        with mock.patch('swift.cli.recon.Scout.scout_server_type',
                        mock_scout_server_type), \
                mock.patch('sys.stdout', new=stdout):
            self.recon.server_type = 'account'
            self.recon.server_type_check(hosts)

        output = stdout.getvalue()
        self.assertIn(res_container, output.splitlines())
        self.assertIn(res_object, output.splitlines())
        stdout.truncate(0)

        # Test ok for account server type
        with mock.patch('swift.cli.recon.Scout.scout_server_type',
                        mock_scout_server_type), \
                mock.patch('sys.stdout', new=stdout):
            self.recon.server_type = 'account'
            self.recon.server_type_check([hosts[2]])

        output = stdout.getvalue()
        self.assertIn(valid, output.splitlines())
        stdout.truncate(0)

        # Test for container server type
        with mock.patch('swift.cli.recon.Scout.scout_server_type',
                        mock_scout_server_type), \
                mock.patch('sys.stdout', new=stdout):
            self.recon.server_type = 'container'
            self.recon.server_type_check(hosts)

        output = stdout.getvalue()
        self.assertIn(res_account, output.splitlines())
        self.assertIn(res_object, output.splitlines())
        stdout.truncate(0)

        # Test ok for container server type
        with mock.patch('swift.cli.recon.Scout.scout_server_type',
                        mock_scout_server_type), \
                mock.patch('sys.stdout', new=stdout):
            self.recon.server_type = 'container'
            self.recon.server_type_check([hosts[1]])

        output = stdout.getvalue()
        self.assertIn(valid, output.splitlines())

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
            with mock.patch('swift.cli.recon.md5_hash_for_file',
                            lambda _: cksum):
                self.recon.get_swiftconfmd5(hosts, printfn=printed.append)

        output = '\n'.join(printed) + '\n'
        self.assertIn("2/2 hosts matched", output)

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
            with mock.patch('swift.cli.recon.md5_hash_for_file',
                            lambda _: cksum):
                self.recon.get_swiftconfmd5(hosts, printfn=printed.append)

        output = '\n'.join(printed) + '\n'
        self.assertIn("1/2 hosts matched", output)
        self.assertIn("http://10.2.2.2:10000/recon/swiftconfmd5 (bogus) "
                      "doesn't match on disk md5sum", output)

    def test_object_auditor_check(self):
        # Recon middleware response from an object server
        def dummy_request(*args, **kwargs):
            values = {
                'passes': 0, 'errors': 0, 'audit_time': 0,
                'start_time': 0, 'quarantined': 0, 'bytes_processed': 0}

            return [('http://127.0.0.1:6010/recon/auditor/object', {
                'object_auditor_stats_ALL': values,
                'object_auditor_stats_ZBF': values,
            }, 200, 0, 0)]

        response = {}

        def catch_print(computed):
            response[computed.get('name')] = computed

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request
        cli._print_stats = catch_print

        cli.object_auditor_check([('127.0.0.1', 6010)])

        # Now check that output contains all keys and names
        keys = ['average', 'number_none', 'high',
                'reported', 'low', 'total', 'perc_none']

        names = [
            'ALL_audit_time_last_path',
            'ALL_quarantined_last_path',
            'ALL_errors_last_path',
            'ALL_passes_last_path',
            'ALL_bytes_processed_last_path',
            'ZBF_audit_time_last_path',
            'ZBF_quarantined_last_path',
            'ZBF_errors_last_path',
            'ZBF_bytes_processed_last_path'
        ]

        for name in names:
            computed = response.get(name)
            self.assertTrue(computed)
            for key in keys:
                self.assertIn(key, computed)

    def test_disk_usage(self):
        def dummy_request(*args, **kwargs):
            return [('http://127.0.0.1:6010/recon/diskusage', [
                {"device": "sdb1", "mounted": True,
                 "avail": 10, "used": 90, "size": 100},
                {"device": "sdc1", "mounted": True,
                 "avail": 15, "used": 85, "size": 100},
                {"device": "sdd1", "mounted": True,
                 "avail": 15, "used": 85, "size": 100}],
                200,
                0,
                0)]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call('Distribution Graph:'),
            mock.call(' 85%    2 **********************************' +
                      '***********************************'),
            mock.call(' 90%    1 **********************************'),
            mock.call('Disk usage: space used: 260 of 300'),
            mock.call('Disk usage: space free: 40 of 300'),
            mock.call('Disk usage: lowest: 85.0%, ' +
                      'highest: 90.0%%, avg: %s' %
                      ('86.6666666667%' if six.PY2 else
                       '86.66666666666667%')),
            mock.call('=' * 79),
        ]

        with mock.patch('six.moves.builtins.print') as mock_print:
            cli.disk_usage([('127.0.0.1', 6010)])
            mock_print.assert_has_calls(default_calls)

        with mock.patch('six.moves.builtins.print') as mock_print:
            expected_calls = default_calls + [
                mock.call('LOWEST 5'),
                mock.call('85.00%  127.0.0.1       sdc1'),
                mock.call('85.00%  127.0.0.1       sdd1'),
                mock.call('90.00%  127.0.0.1       sdb1')
            ]
            cli.disk_usage([('127.0.0.1', 6010)], 0, 5)
            mock_print.assert_has_calls(expected_calls)

        with mock.patch('six.moves.builtins.print') as mock_print:
            expected_calls = default_calls + [
                mock.call('TOP 5'),
                mock.call('90.00%  127.0.0.1       sdb1'),
                mock.call('85.00%  127.0.0.1       sdc1'),
                mock.call('85.00%  127.0.0.1       sdd1')
            ]
            cli.disk_usage([('127.0.0.1', 6010)], 5, 0)
            mock_print.assert_has_calls(expected_calls)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('time.time')
    def test_replication_check(self, mock_now, mock_print):
        now = 1430000000.0

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6011/recon/replication/container',
                 {"replication_last": now,
                  "replication_stats": {
                      "no_change": 2, "rsync": 0, "success": 3, "failure": 1,
                      "attempted": 0, "ts_repl": 0, "remove": 0,
                      "remote_merge": 0, "diff_capped": 0, "start": now,
                      "hashmatch": 0, "diff": 0, "empty": 0},
                  "replication_time": 42},
                 200,
                 0,
                 0),
                ('http://127.0.0.1:6021/recon/replication/container',
                 {"replication_last": now,
                  "replication_stats": {
                      "no_change": 0, "rsync": 0, "success": 1, "failure": 0,
                      "attempted": 0, "ts_repl": 0, "remove": 0,
                      "remote_merge": 0, "diff_capped": 0, "start": now,
                      "hashmatch": 0, "diff": 0, "empty": 0},
                  "replication_time": 23},
                 200,
                 0,
                 0),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call('[replication_failure] low: 0, high: 1, avg: 0.5, ' +
                      'total: 1, Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('[replication_success] low: 1, high: 3, avg: 2.0, ' +
                      'total: 4, Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('[replication_time] low: 23, high: 42, avg: 32.5, ' +
                      'total: 65, Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('[replication_attempted] low: 0, high: 0, avg: 0.0, ' +
                      'total: 0, Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('Oldest completion was 2015-04-25 22:13:20 ' +
                      '(42 seconds ago) by 127.0.0.1:6011.'),
            mock.call('Most recent completion was 2015-04-25 22:13:20 ' +
                      '(42 seconds ago) by 127.0.0.1:6011.'),
        ]

        mock_now.return_value = now + 42
        cli.replication_check([('127.0.0.1', 6011), ('127.0.0.1', 6021)])
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('time.time')
    def test_load_check(self, mock_now, mock_print):
        now = 1430000000.0

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/load',
                 {"1m": 0.2, "5m": 0.4, "15m": 0.25,
                  "processes": 10000, "tasks": "1/128"},
                 200,
                 0,
                 0),
                ('http://127.0.0.1:6020/recon/load',
                 {"1m": 0.4, "5m": 0.8, "15m": 0.75,
                  "processes": 9000, "tasks": "1/200"},
                 200,
                 0,
                 0),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call('[5m_load_avg] low: 0, high: 0, avg: 0.6, total: 1, ' +
                      'Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('[15m_load_avg] low: 0, high: 0, avg: 0.5, total: 1, ' +
                      'Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('[1m_load_avg] low: 0, high: 0, avg: 0.3, total: 0, ' +
                      'Failed: 0.0%, no_result: 0, reported: 2'),
        ]

        mock_now.return_value = now + 42
        cli.load_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)])
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('time.time')
    def test_time_check(self, mock_now, mock_print):
        now = 1430000000.0
        mock_now.return_value = now

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/time',
                 now,
                 200,
                 now - 0.5,
                 now + 0.5),
                ('http://127.0.0.1:6020/recon/time',
                 now,
                 200,
                 now,
                 now),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call('2/2 hosts matched, 0 error[s] while checking hosts.')
        ]

        cli.time_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)])
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('time.time')
    def test_time_check_mismatch(self, mock_now, mock_print):
        now = 1430000000.0
        mock_now.return_value = now

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/time',
                 now,
                 200,
                 now + 0.5,
                 now + 1.3),
                ('http://127.0.0.1:6020/recon/time',
                 now,
                 200,
                 now,
                 now),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call("!! http://127.0.0.1:6010/recon/time current time is "
                      "2015-04-25 22:13:21, but remote is "
                      "2015-04-25 22:13:20, differs by 1.3000 sec"),
            mock.call('1/2 hosts matched, 0 error[s] while checking hosts.'),
        ]

        cli.time_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)])

        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('time.time')
    def test_time_check_jitter(self, mock_now, mock_print):
        now = 1430000000.0
        mock_now.return_value = now

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/time',
                 now - 2,
                 200,
                 now,
                 now + 3),
                ('http://127.0.0.1:6020/recon/time',
                 now + 2,
                 200,
                 now - 3,
                 now),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call('2/2 hosts matched, 0 error[s] while checking hosts.')
        ]

        cli.time_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)], 3)
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    def test_version_check(self, mock_print):
        version = "2.7.1.dev144"

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/version',
                 {'version': version},
                 200,
                 0,
                 0),
                ('http://127.0.0.1:6020/recon/version',
                 {'version': version},
                 200,
                 0,
                 0),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call("Versions matched (%s), "
                      "0 error[s] while checking hosts." % version)
        ]

        cli.version_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)])
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('time.time')
    def test_time_check_jitter_mismatch(self, mock_now, mock_print):
        now = 1430000000.0
        mock_now.return_value = now

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/time',
                 now - 4,
                 200,
                 now,
                 now + 2),
                ('http://127.0.0.1:6020/recon/time',
                 now + 4,
                 200,
                 now - 2,
                 now),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call("!! http://127.0.0.1:6010/recon/time current time is "
                      "2015-04-25 22:13:22, but remote is "
                      "2015-04-25 22:13:16, differs by 6.0000 sec"),
            mock.call("!! http://127.0.0.1:6020/recon/time current time is "
                      "2015-04-25 22:13:20, but remote is "
                      "2015-04-25 22:13:24, differs by 4.0000 sec"),
            mock.call('0/2 hosts matched, 0 error[s] while checking hosts.'),
        ]

        cli.time_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)], 3)
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    def test_version_check_differs(self, mock_print):
        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/version',
                 {'version': "2.7.1.dev144"},
                 200,
                 0,
                 0),
                ('http://127.0.0.1:6020/recon/version',
                 {'version': "2.7.1.dev145"},
                 200,
                 0,
                 0),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call("Versions not matched (2.7.1.dev144, 2.7.1.dev145), "
                      "0 error[s] while checking hosts.")
        ]

        cli.version_check([('127.0.0.1', 6010), ('127.0.0.1', 6020)])
        # We need any_order=True because the order of calls depends on the dict
        # that is returned from the recon middleware, thus can't rely on it
        mock_print.assert_has_calls(default_calls, any_order=True)

    @mock.patch('six.moves.builtins.print')
    @mock.patch('swift.cli.recon.SwiftRecon.get_hosts')
    def test_multiple_server_types(self, mock_get_hosts, mock_print):
        mock_get_hosts.return_value = set([('127.0.0.1', 10000)])

        self.recon.object_auditor_check = mock.MagicMock()
        self.recon.auditor_check = mock.MagicMock()

        with mock.patch.object(
                sys, 'argv',
                ["prog", "account", "container", "object", "--auditor"]):
            self.recon.main()
        expected_calls = [
            mock.call("--> Starting reconnaissance on 1 hosts (account)"),
            mock.call("--> Starting reconnaissance on 1 hosts (container)"),
            mock.call("--> Starting reconnaissance on 1 hosts (object)"),
        ]
        mock_print.assert_has_calls(expected_calls, any_order=True)

        expected = mock.call(set([('127.0.0.1', 10000)]))
        self.recon.object_auditor_check.assert_has_calls([expected])
        # Two calls expected - one account, one container
        self.recon.auditor_check.assert_has_calls([expected, expected])
