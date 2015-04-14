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

from contextlib import nested
import json
import mock
import os
import random
import re
import string
from StringIO import StringIO
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
        self.server_type_url = 'http://127.0.0.1:8080/'

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

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_scout_server_type_ok(self, mock_urlopen):
        def getheader(name):
            d = {'Server': 'server-type'}
            return d.get(name)
        mock_urlopen.return_value.info.return_value.getheader = getheader
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.server_type_url)
        self.assertEqual(content, 'server-type')
        self.assertEqual(status, 200)

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_scout_server_type_url_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.URLError("")
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertTrue(isinstance(content, urllib2.URLError))
        self.assertEqual(url, self.server_type_url)
        self.assertEqual(status, -1)

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_scout_server_type_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib2.HTTPError(
            self.server_type_url, 404, "Internal error", None, None)
        url, content, status = self.scout_instance.scout_server_type(
            ("127.0.0.1", "8080"))
        self.assertEqual(url, self.server_type_url)
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
        except OSError:
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
            mock_localtime.assert_called_with(1387274400)

            timestamp2 = self.recon_instance._ptime()
            self.assertEqual(timestamp2, "2013-12-17 10:00:00")
            mock_localtime.assert_called_with()

    def test_get_devices(self):
        ringbuilder = builder.RingBuilder(2, 3, 1)
        ringbuilder.add_dev({'id': 0, 'zone': 0, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 10000,
                             'device': 'sda1', 'region': 0})
        ringbuilder.add_dev({'id': 1, 'zone': 1, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 10001,
                             'device': 'sda1', 'region': 0})
        ringbuilder.add_dev({'id': 2, 'zone': 0, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 10002,
                             'device': 'sda1', 'region': 1})
        ringbuilder.add_dev({'id': 3, 'zone': 1, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 10003,
                             'device': 'sda1', 'region': 1})
        ringbuilder.rebalance()
        ringbuilder.get_ring().save(self.tmpfile_name)

        ips = self.recon_instance.get_devices(
            None, None, self.swift_dir, self.ring_name)
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001),
                 ('127.0.0.1', 10002), ('127.0.0.1', 10003)]), ips)

        ips = self.recon_instance.get_devices(
            0, None, self.swift_dir, self.ring_name)
        self.assertEqual(
            set([('127.0.0.1', 10000), ('127.0.0.1', 10001)]), ips)

        ips = self.recon_instance.get_devices(
            1, None, self.swift_dir, self.ring_name)
        self.assertEqual(
            set([('127.0.0.1', 10002), ('127.0.0.1', 10003)]), ips)

        ips = self.recon_instance.get_devices(
            0, 0, self.swift_dir, self.ring_name)
        self.assertEqual(set([('127.0.0.1', 10000)]), ips)

        ips = self.recon_instance.get_devices(
            1, 1, self.swift_dir, self.ring_name)
        self.assertEqual(set([('127.0.0.1', 10003)]), ips)

    def test_get_ringmd5(self):
        for server_type in ('account', 'container', 'object', 'object-1'):
            ring_name = '%s.ring.gz' % server_type
            ring_file = os.path.join(self.swift_dir, ring_name)
            open(ring_file, 'w')

        empty_file_hash = 'd41d8cd98f00b204e9800998ecf8427e'
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
            scout_instance.scout.return_value = (url, response, status)
            mock_scout.return_value = scout_instance
            stdout = StringIO()
            mock_hash = mock.MagicMock()
            patches = [
                mock.patch('sys.stdout', new=stdout),
                mock.patch('swift.cli.recon.md5', new=mock_hash),
            ]
            with nested(*patches):
                mock_hash.return_value.hexdigest.return_value = \
                    empty_file_hash
                self.recon_instance.get_ringmd5(hosts, self.swift_dir)
            output = stdout.getvalue()
            expected = '1/1 hosts matched'
            for line in output.splitlines():
                if '!!' in line:
                    self.fail('Unexpected Error in output: %r' % line)
                if expected in line:
                    break
            else:
                self.fail('Did not find expected substring %r '
                          'in output:\n%s' % (expected, output))

        for ring in ('account', 'container', 'object', 'object-1'):
            os.remove(os.path.join(self.swift_dir, "%s.ring.gz" % ring))

    def test_quarantine_check(self):
        hosts = [('127.0.0.1', 6010), ('127.0.0.1', 6020),
                 ('127.0.0.1', 6030), ('127.0.0.1', 6040)]
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
                                         '1': {'objects': 4}}}}
        # <low> <high> <avg> <total> <Failed> <no_result> <reported>
        expected = {'objects_0': (0, 3, 1.5, 6, 0.0, 0, 4),
                    'objects_1': (1, 4, 2.5, 10, 0.0, 0, 4),
                    'objects': (1, 7, 4.0, 16, 0.0, 0, 4),
                    'accounts': (0, 3, 1.5, 6, 0.0, 0, 4),
                    'containers': (0, 3, 1.5, 6, 0.0, 0, 4)}

        def mock_scout_quarantine(app, host):
            url = 'http://%s:%s/recon/quarantined' % host
            response = responses[host[1]]
            status = 200
            return url, response, status

        stdout = StringIO()
        patches = [
            mock.patch('swift.cli.recon.Scout.scout', mock_scout_quarantine),
            mock.patch('sys.stdout', new=stdout),
        ]
        with nested(*patches):
            self.recon_instance.quarantine_check(hosts)

        output = stdout.getvalue()
        r = re.compile("\[quarantined_(.*)\](.*)")
        for line in output.splitlines():
            m = r.match(line)
            if m:
                ex = expected.pop(m.group(1))
                self.assertEquals(m.group(2),
                                  " low: %s, high: %s, avg: %s, total: %s,"
                                  " Failed: %s%%, no_result: %s, reported: %s"
                                  % ex)
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
            return url, response, status

        stdout = StringIO()
        patches = [
            mock.patch('swift.cli.recon.Scout.scout', mock_scout_driveaudit),
            mock.patch('sys.stdout', new=stdout),
        ]
        with nested(*patches):
            self.recon_instance.driveaudit_check(hosts)

        output = stdout.getvalue()
        r = re.compile("\[drive_audit_errors(.*)\](.*)")
        lines = output.splitlines()
        self.assertTrue(lines)
        for line in lines:
            m = r.match(line)
            if m:
                self.assertEquals(m.group(2),
                                  " low: %s, high: %s, avg: %s, total: %s,"
                                  " Failed: %s%%, no_result: %s, reported: %s"
                                  % expected)


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
        patches = [
            mock.patch('swift.cli.recon.Scout.scout_server_type',
                       mock_scout_server_type),
            mock.patch('sys.stdout', new=stdout),
        ]

        res_object = 'Invalid: http://127.0.0.1:6010/ is object-server'
        res_container = 'Invalid: http://127.0.0.1:6011/ is container-server'
        res_account = 'Invalid: http://127.0.0.1:6012/ is account-server'
        valid = "1/1 hosts ok, 0 error[s] while checking hosts."

        #Test for object server type - default
        with nested(*patches):
            self.recon.server_type_check(hosts)

        output = stdout.getvalue()
        self.assertTrue(res_container in output.splitlines())
        self.assertTrue(res_account in output.splitlines())
        stdout.truncate(0)

        #Test ok for object server type - default
        with nested(*patches):
            self.recon.server_type_check([hosts[0]])

        output = stdout.getvalue()
        self.assertTrue(valid in output.splitlines())
        stdout.truncate(0)

        #Test for account server type
        with nested(*patches):
            self.recon.server_type = 'account'
            self.recon.server_type_check(hosts)

        output = stdout.getvalue()
        self.assertTrue(res_container in output.splitlines())
        self.assertTrue(res_object in output.splitlines())
        stdout.truncate(0)

        #Test ok for account server type
        with nested(*patches):
            self.recon.server_type = 'account'
            self.recon.server_type_check([hosts[2]])

        output = stdout.getvalue()
        self.assertTrue(valid in output.splitlines())
        stdout.truncate(0)

        #Test for container server type
        with nested(*patches):
            self.recon.server_type = 'container'
            self.recon.server_type_check(hosts)

        output = stdout.getvalue()
        self.assertTrue(res_account in output.splitlines())
        self.assertTrue(res_object in output.splitlines())
        stdout.truncate(0)

        #Test ok for container server type
        with nested(*patches):
            self.recon.server_type = 'container'
            self.recon.server_type_check([hosts[1]])

        output = stdout.getvalue()
        self.assertTrue(valid in output.splitlines())

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

    def test_object_auditor_check(self):
        # Recon middleware response from an object server
        def dummy_request(*args, **kwargs):
            values = {
                'passes': 0, 'errors': 0, 'audit_time': 0,
                'start_time': 0, 'quarantined': 0, 'bytes_processed': 0}

            return [('http://127.0.0.1:6010/recon/auditor/object', {
                'object_auditor_stats_ALL': values,
                'object_auditor_stats_ZBF': values,
            }, 200)]

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
                self.assertTrue(key in computed)

    def test_disk_usage(self):
        def dummy_request(*args, **kwargs):
            return [('http://127.0.0.1:6010/recon/diskusage', [
                {"device": "sdb1", "mounted": True,
                 "avail": 10, "used": 90, "size": 100},
                {"device": "sdc1", "mounted": True,
                 "avail": 15, "used": 85, "size": 100},
                {"device": "sdd1", "mounted": True,
                 "avail": 15, "used": 85, "size": 100}],
                200)]

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
                      'highest: 90.0%, avg: 86.6666666667%'),
            mock.call('=' * 79),
        ]

        with mock.patch('__builtin__.print') as mock_print:
            cli.disk_usage([('127.0.0.1', 6010)])
            mock_print.assert_has_calls(default_calls)

        with mock.patch('__builtin__.print') as mock_print:
            expected_calls = default_calls + [
                mock.call('LOWEST 5'),
                mock.call('85.00%  127.0.0.1       sdc1'),
                mock.call('85.00%  127.0.0.1       sdd1'),
                mock.call('90.00%  127.0.0.1       sdb1')
            ]
            cli.disk_usage([('127.0.0.1', 6010)], 0, 5)
            mock_print.assert_has_calls(expected_calls)

        with mock.patch('__builtin__.print') as mock_print:
            expected_calls = default_calls + [
                mock.call('TOP 5'),
                mock.call('90.00%  127.0.0.1       sdb1'),
                mock.call('85.00%  127.0.0.1       sdc1'),
                mock.call('85.00%  127.0.0.1       sdd1')
            ]
            cli.disk_usage([('127.0.0.1', 6010)], 5, 0)
            mock_print.assert_has_calls(expected_calls)

    @mock.patch('__builtin__.print')
    @mock.patch('time.time')
    def test_object_replication_check(self, mock_now, mock_print):
        now = 1430000000.0

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/replication/object',
                 {"object_replication_time": 61,
                  "object_replication_last": now},
                 200),
                ('http://127.0.0.1:6020/recon/replication/object',
                 {"object_replication_time": 23,
                  "object_replication_last": now},
                 200),
            ]

        cli = recon.SwiftRecon()
        cli.pool.imap = dummy_request

        default_calls = [
            mock.call('[replication_time] low: 23, high: 61, avg: 42.0, ' +
                      'total: 84, Failed: 0.0%, no_result: 0, reported: 2'),
            mock.call('Oldest completion was 2015-04-25 22:13:20 ' +
                      '(42 seconds ago) by 127.0.0.1:6010.'),
            mock.call('Most recent completion was 2015-04-25 22:13:20 ' +
                      '(42 seconds ago) by 127.0.0.1:6010.'),
        ]

        mock_now.return_value = now + 42
        cli.object_replication_check([('127.0.0.1', 6010),
                                      ('127.0.0.1', 6020)])
        mock_print.assert_has_calls(default_calls)

    @mock.patch('__builtin__.print')
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
                 200),
                ('http://127.0.0.1:6021/recon/replication/container',
                 {"replication_last": now,
                  "replication_stats": {
                      "no_change": 0, "rsync": 0, "success": 1, "failure": 0,
                      "attempted": 0, "ts_repl": 0, "remove": 0,
                      "remote_merge": 0, "diff_capped": 0, "start": now,
                      "hashmatch": 0, "diff": 0, "empty": 0},
                  "replication_time": 23},
                 200),
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

    @mock.patch('__builtin__.print')
    @mock.patch('time.time')
    def test_load_check(self, mock_now, mock_print):
        now = 1430000000.0

        def dummy_request(*args, **kwargs):
            return [
                ('http://127.0.0.1:6010/recon/load',
                 {"1m": 0.2, "5m": 0.4, "15m": 0.25,
                  "processes": 10000, "tasks": "1/128"},
                 200),
                ('http://127.0.0.1:6020/recon/load',
                 {"1m": 0.4, "5m": 0.8, "15m": 0.75,
                  "processes": 9000, "tasks": "1/200"},
                 200),
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
