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

from datetime import datetime
import email.parser
import hashlib
import locale
import random
import six
from six.moves import urllib
import time
import unittest2
import uuid
from copy import deepcopy
import eventlet
from swift.common.http import is_success, is_client_error
from email.utils import parsedate

import mock

from test.functional import normalized_urls, load_constraint, cluster_info
from test.functional import check_response, retry
import test.functional as tf
from test.functional.swift_test_client import Account, Connection, File, \
    ResponseError, SkipTest


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class Utils(object):
    @classmethod
    def create_ascii_name(cls, length=None):
        return uuid.uuid4().hex

    @classmethod
    def create_utf8_name(cls, length=None):
        if length is None:
            length = 15
        else:
            length = int(length)

        utf8_chars = u'\uF10F\uD20D\uB30B\u9409\u8508\u5605\u3703\u1801'\
                     u'\u0900\uF110\uD20E\uB30C\u940A\u8509\u5606\u3704'\
                     u'\u1802\u0901\uF111\uD20F\uB30D\u940B\u850A\u5607'\
                     u'\u3705\u1803\u0902\uF112\uD210\uB30E\u940C\u850B'\
                     u'\u5608\u3706\u1804\u0903\u03A9\u2603'
        ustr = u''.join([random.choice(utf8_chars)
                         for x in range(length)])
        if six.PY2:
            return ustr.encode('utf-8')
        return ustr

    create_name = create_ascii_name


class BaseEnv(object):
    account = conn = None

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

    @classmethod
    def tearDown(cls):
        pass


class Base(unittest2.TestCase):
    env = BaseEnv

    @classmethod
    def tearDownClass(cls):
        cls.env.tearDown()

    @classmethod
    def setUpClass(cls):
        cls.env.setUp()

    def setUp(self):
        if tf.in_process:
            tf.skip_if_no_xattrs()

    def assert_body(self, body):
        if not isinstance(body, bytes):
            body = body.encode('utf-8')
        response_body = self.env.conn.response.read()
        self.assertEqual(response_body, body,
                         'Body returned: %s' % (response_body))

    def assert_status(self, status_or_statuses):
        self.assertTrue(
            self.env.conn.response.status == status_or_statuses or
            (hasattr(status_or_statuses, '__iter__') and
                self.env.conn.response.status in status_or_statuses),
            'Status returned: %d Expected: %s' %
            (self.env.conn.response.status, status_or_statuses))

    def assert_header(self, header_name, expected_value):
        try:
            actual_value = self.env.conn.response.getheader(header_name)
        except KeyError:
            self.fail(
                'Expected header name %r not found in response.' % header_name)
        self.assertEqual(expected_value, actual_value)


class Base2(object):
    @classmethod
    def setUpClass(cls):
        Utils.create_name = Utils.create_utf8_name
        super(Base2, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        Utils.create_name = Utils.create_ascii_name


class TestAccountEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestAccountEnv, cls).setUp()
        cls.containers = []
        for i in range(10):
            cont = cls.account.container(Utils.create_name())
            if not cont.create():
                raise ResponseError(cls.conn.response)

            cls.containers.append(cont)


class TestAccountDev(Base):
    env = TestAccountEnv


class TestAccountDevUTF8(Base2, TestAccountDev):
    pass


class TestAccount(Base):
    env = TestAccountEnv

    def testNoAuthToken(self):
        self.assertRaises(ResponseError, self.env.account.info,
                          cfg={'no_auth_token': True})
        self.assert_status([401, 412])

        self.assertRaises(ResponseError, self.env.account.containers,
                          cfg={'no_auth_token': True})
        self.assert_status([401, 412])

    def testInvalidUTF8Path(self):
        valid_utf8 = Utils.create_utf8_name()
        if six.PY2:
            invalid_utf8 = valid_utf8[::-1]
        else:
            invalid_utf8 = (valid_utf8.encode('utf8')[::-1]).decode(
                'utf-8', 'surrogateescape')
        container = self.env.account.container(invalid_utf8)
        self.assertFalse(container.create(cfg={'no_path_quote': True}))
        self.assert_status(412)
        self.assert_body('Invalid UTF8 or contains NULL')

    def testVersionOnlyPath(self):
        self.env.account.conn.make_request('PUT',
                                           cfg={'version_only_path': True})
        self.assert_status(412)
        self.assert_body('Bad URL')

    def testInvalidPath(self):
        was_path = self.env.account.conn.storage_path
        if (normalized_urls):
            self.env.account.conn.storage_path = '/'
        else:
            self.env.account.conn.storage_path = "/%s" % was_path
        try:
            self.env.account.conn.make_request('GET')
            self.assert_status(404)
        finally:
            self.env.account.conn.storage_path = was_path

    def testPUTError(self):
        if load_constraint('allow_account_management'):
            raise SkipTest("Allow account management is enabled")
        self.env.account.conn.make_request('PUT')
        self.assert_status([403, 405])

    def testAccountHead(self):
        try_count = 0
        while try_count < 5:
            try_count += 1

            info = self.env.account.info()
            for field in ['object_count', 'container_count', 'bytes_used']:
                self.assertGreaterEqual(info[field], 0)

            if info['container_count'] == len(self.env.containers):
                break

            if try_count < 5:
                time.sleep(1)

        self.assertEqual(info['container_count'], len(self.env.containers))
        self.assert_status(204)

    def testContainerSerializedInfo(self):
        container_info = {}
        for container in self.env.containers:
            info = {'bytes': 0}
            info['count'] = random.randint(10, 30)
            for i in range(info['count']):
                file_item = container.file(Utils.create_name())
                bytes = random.randint(1, 32768)
                file_item.write_random(bytes)
                info['bytes'] += bytes

            container_info[container.name] = info

        for format_type in ['json', 'xml']:
            for a in self.env.account.containers(
                    parms={'format': format_type}):
                self.assertGreaterEqual(a['count'], 0)
                self.assertGreaterEqual(a['bytes'], 0)

            headers = dict(self.env.conn.response.getheaders())
            if format_type == 'json':
                self.assertEqual(headers['content-type'],
                                 'application/json; charset=utf-8')
            elif format_type == 'xml':
                self.assertEqual(headers['content-type'],
                                 'application/xml; charset=utf-8')

    def testListingLimit(self):
        limit = load_constraint('account_listing_limit')
        for l in (1, 100, limit / 2, limit - 1, limit, limit + 1, limit * 2):
            p = {'limit': l}

            if l <= limit:
                self.assertLessEqual(len(self.env.account.containers(parms=p)),
                                     l)
                self.assert_status(200)
            else:
                self.assertRaises(ResponseError,
                                  self.env.account.containers, parms=p)
                self.assert_status(412)

    def testContainerListing(self):
        a = sorted([c.name for c in self.env.containers])

        for format_type in [None, 'json', 'xml']:
            b = self.env.account.containers(parms={'format': format_type})

            if isinstance(b[0], dict):
                b = [x['name'] for x in b]

            self.assertEqual(a, b)

    def testListDelimiter(self):
        delimiter = '-'
        containers = ['test', delimiter.join(['test', 'bar']),
                      delimiter.join(['test', 'foo'])]
        for c in containers:
            cont = self.env.account.container(c)
            self.assertTrue(cont.create())

        results = self.env.account.containers(parms={'delimiter': delimiter})
        expected = ['test', 'test-']
        results = [r for r in results if r in expected]
        self.assertEqual(expected, results)

        results = self.env.account.containers(parms={'delimiter': delimiter,
                                                     'reverse': 'yes'})
        expected.reverse()
        results = [r for r in results if r in expected]
        self.assertEqual(expected, results)

    def testListDelimiterAndPrefix(self):
        delimiter = 'a'
        containers = ['bar', 'bazar']
        for c in containers:
            cont = self.env.account.container(c)
            self.assertTrue(cont.create())

        results = self.env.account.containers(parms={'delimiter': delimiter,
                                                     'prefix': 'ba'})
        expected = ['bar', 'baza']
        results = [r for r in results if r in expected]
        self.assertEqual(expected, results)

        results = self.env.account.containers(parms={'delimiter': delimiter,
                                                     'prefix': 'ba',
                                                     'reverse': 'yes'})
        expected.reverse()
        results = [r for r in results if r in expected]
        self.assertEqual(expected, results)

    def testContainerListingLastModified(self):
        expected = {}
        for container in self.env.containers:
            res = container.info()
            expected[container.name] = time.mktime(
                parsedate(res['last_modified']))

        for format_type in ['json', 'xml']:
            actual = {}
            containers = self.env.account.containers(
                parms={'format': format_type})
            if isinstance(containers[0], dict):
                for container in containers:
                    self.assertIn('name', container)  # sanity
                    self.assertIn('last_modified', container)  # sanity
                    # ceil by hand (wants easier way!)
                    datetime_str, micro_sec_str = \
                        container['last_modified'].split('.')
                    timestamp = time.mktime(
                        time.strptime(datetime_str,
                                      "%Y-%m-%dT%H:%M:%S"))
                    if int(micro_sec_str):
                        timestamp += 1
                    actual[container['name']] = timestamp

            self.assertEqual(expected, actual)

    def testInvalidAuthToken(self):
        hdrs = {'X-Auth-Token': 'bogus_auth_token'}
        self.assertRaises(ResponseError, self.env.account.info, hdrs=hdrs)
        self.assert_status(401)

    def testLastContainerMarker(self):
        for format_type in [None, 'json', 'xml']:
            containers = self.env.account.containers(parms={
                'format': format_type})
            self.assertEqual(len(containers), len(self.env.containers))
            self.assert_status(200)

            marker = (containers[-1] if format_type is None
                      else containers[-1]['name'])
            containers = self.env.account.containers(
                parms={'format': format_type, 'marker': marker})
            self.assertEqual(len(containers), 0)
            if format_type is None:
                self.assert_status(204)
            else:
                self.assert_status(200)

    def testMarkerLimitContainerList(self):
        for format_type in [None, 'json', 'xml']:
            for marker in ['0', 'A', 'I', 'R', 'Z', 'a', 'i', 'r', 'z',
                           'abc123', 'mnop', 'xyz']:

                limit = random.randint(2, 9)
                containers = self.env.account.containers(
                    parms={'format': format_type,
                           'marker': marker,
                           'limit': limit})
                self.assertLessEqual(len(containers), limit)
                if containers:
                    if isinstance(containers[0], dict):
                        containers = [x['name'] for x in containers]
                    self.assertGreater(locale.strcoll(containers[0], marker),
                                       0)

    def testContainersOrderedByName(self):
        for format_type in [None, 'json', 'xml']:
            containers = self.env.account.containers(
                parms={'format': format_type})
            if isinstance(containers[0], dict):
                containers = [x['name'] for x in containers]
            self.assertEqual(sorted(containers, key=locale.strxfrm),
                             containers)

    def testQuotedWWWAuthenticateHeader(self):
        # check that the www-authenticate header value with the swift realm
        # is correctly quoted.
        conn = Connection(tf.config)
        conn.authenticate()
        inserted_html = '<b>Hello World'
        hax = 'AUTH_haxx"\nContent-Length: %d\n\n%s' % (len(inserted_html),
                                                        inserted_html)
        quoted_hax = urllib.parse.quote(hax)
        conn.connection.request('GET', '/v1/' + quoted_hax, None, {})
        resp = conn.connection.getresponse()
        resp_headers = dict(resp.getheaders())
        self.assertIn('www-authenticate', resp_headers)
        actual = resp_headers['www-authenticate']
        expected = 'Swift realm="%s"' % quoted_hax
        # other middleware e.g. auth_token may also set www-authenticate
        # headers in which case actual values will be a comma separated list.
        # check that expected value is among the actual values
        self.assertIn(expected, actual)


class TestAccountUTF8(Base2, TestAccount):
    pass


class TestAccountNoContainers(Base):
    def testGetRequest(self):
        for format_type in [None, 'json', 'xml']:
            self.assertFalse(self.env.account.containers(
                parms={'format': format_type}))

            if format_type is None:
                self.assert_status(204)
            else:
                self.assert_status(200)


class TestAccountNoContainersUTF8(Base2, TestAccountNoContainers):
    pass


class TestAccountSortingEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestAccountSortingEnv, cls).setUp()
        postfix = Utils.create_name()
        cls.cont_items = ('a1', 'a2', 'A3', 'b1', 'B2', 'a10', 'b10', 'zz')
        cls.cont_items = ['%s%s' % (x, postfix) for x in cls.cont_items]

        for container in cls.cont_items:
            c = cls.account.container(container)
            if not c.create():
                raise ResponseError(cls.conn.response)


class TestAccountSorting(Base):
    env = TestAccountSortingEnv

    def testAccountContainerListSorting(self):
        # name (byte order) sorting.
        cont_list = sorted(self.env.cont_items)
        for reverse in ('false', 'no', 'off', '', 'garbage'):
            cont_listing = self.env.account.containers(
                parms={'reverse': reverse})
            self.assert_status(200)
            self.assertEqual(cont_list, cont_listing,
                             'Expected %s but got %s with reverse param %r'
                             % (cont_list, cont_listing, reverse))

    def testAccountContainerListSortingReverse(self):
        # name (byte order) sorting.
        cont_list = sorted(self.env.cont_items)
        cont_list.reverse()
        for reverse in ('true', '1', 'yes', 'on', 't', 'y'):
            cont_listing = self.env.account.containers(
                parms={'reverse': reverse})
            self.assert_status(200)
            self.assertEqual(cont_list, cont_listing,
                             'Expected %s but got %s with reverse param %r'
                             % (cont_list, cont_listing, reverse))

    def testAccountContainerListSortingByPrefix(self):
        cont_list = sorted(c for c in self.env.cont_items if c.startswith('a'))
        cont_list.reverse()
        cont_listing = self.env.account.containers(parms={
            'reverse': 'on', 'prefix': 'a'})
        self.assert_status(200)
        self.assertEqual(cont_list, cont_listing)

    def testAccountContainerListSortingByMarkersExclusive(self):
        first_item = self.env.cont_items[3]  # 'b1' + postfix
        last_item = self.env.cont_items[4]  # 'B2' + postfix

        cont_list = sorted(c for c in self.env.cont_items
                           if last_item < c < first_item)
        cont_list.reverse()
        cont_listing = self.env.account.containers(parms={
            'reverse': 'on', 'marker': first_item, 'end_marker': last_item})
        self.assert_status(200)
        self.assertEqual(cont_list, cont_listing)

    def testAccountContainerListSortingByMarkersInclusive(self):
        first_item = self.env.cont_items[3]  # 'b1' + postfix
        last_item = self.env.cont_items[4]  # 'B2' + postfix

        cont_list = sorted(c for c in self.env.cont_items
                           if last_item <= c <= first_item)
        cont_list.reverse()
        cont_listing = self.env.account.containers(parms={
            'reverse': 'on', 'marker': first_item + '\x00',
            'end_marker': last_item[:-1] + chr(ord(last_item[-1]) - 1)})
        self.assert_status(200)
        self.assertEqual(cont_list, cont_listing)

    def testAccountContainerListSortingByReversedMarkers(self):
        cont_listing = self.env.account.containers(parms={
            'reverse': 'on', 'marker': 'B', 'end_marker': 'b1'})
        self.assert_status(204)
        self.assertEqual([], cont_listing)


class TestContainerEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestContainerEnv, cls).setUp()
        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_count = 10
        cls.file_size = 128
        cls.files = list()
        for x in range(cls.file_count):
            file_item = cls.container.file(Utils.create_name())
            file_item.write_random(cls.file_size)
            cls.files.append(file_item.name)


class TestContainerDev(Base):
    env = TestContainerEnv


class TestContainerDevUTF8(Base2, TestContainerDev):
    pass


class TestContainer(Base):
    env = TestContainerEnv

    def testContainerNameLimit(self):
        limit = load_constraint('max_container_name_length')

        for l in (limit - 100, limit - 10, limit - 1, limit,
                  limit + 1, limit + 10, limit + 100):
            cont = self.env.account.container('a' * l)
            if l <= limit:
                self.assertTrue(cont.create())
                self.assert_status((201, 202))
            else:
                self.assertFalse(cont.create())
                self.assert_status(400)

    def testFileThenContainerDelete(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())
        file_item = cont.file(Utils.create_name())
        self.assertTrue(file_item.write_random())

        self.assertTrue(file_item.delete())
        self.assert_status(204)
        self.assertNotIn(file_item.name, cont.files())

        self.assertTrue(cont.delete())
        self.assert_status(204)
        self.assertNotIn(cont.name, self.env.account.containers())

    def testFileListingLimitMarkerPrefix(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())

        files = sorted([Utils.create_name() for x in range(10)])
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())

        for i in range(len(files)):
            f = files[i]
            for j in range(1, len(files) - i):
                self.assertEqual(cont.files(parms={'limit': j, 'marker': f}),
                                 files[i + 1: i + j + 1])
            self.assertEqual(cont.files(parms={'marker': f}), files[i + 1:])
            self.assertEqual(cont.files(parms={'marker': f, 'prefix': f}), [])
            self.assertEqual(cont.files(parms={'prefix': f}), [f])

    def testPrefixAndLimit(self):
        load_constraint('container_listing_limit')
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())

        prefix_file_count = 10
        limit_count = 2
        prefixs = ['alpha/', 'beta/', 'kappa/']
        prefix_files = {}

        for prefix in prefixs:
            prefix_files[prefix] = []

            for i in range(prefix_file_count):
                file_item = cont.file(prefix + Utils.create_name())
                file_item.write()
                prefix_files[prefix].append(file_item.name)

        for format_type in [None, 'json', 'xml']:
            for prefix in prefixs:
                files = cont.files(parms={'prefix': prefix,
                                          'format': format_type})
                if isinstance(files[0], dict):
                    files = [x.get('name', x.get('subdir')) for x in files]
                self.assertEqual(files, sorted(prefix_files[prefix]))

        for format_type in [None, 'json', 'xml']:
            for prefix in prefixs:
                files = cont.files(parms={'limit': limit_count,
                                          'prefix': prefix,
                                          'format': format_type})
                if isinstance(files[0], dict):
                    files = [x.get('name', x.get('subdir')) for x in files]
                self.assertEqual(len(files), limit_count)

                for file_item in files:
                    self.assertTrue(file_item.startswith(prefix))

    def testListDelimiter(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())

        delimiter = '-'
        files = ['test', delimiter.join(['test', 'bar']),
                 delimiter.join(['test', 'foo'])]
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())

        for format_type in [None, 'json', 'xml']:
            results = cont.files(parms={'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test', 'test-bar', 'test-foo'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test', 'test-'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'format': format_type,
                                        'reverse': 'yes'})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test-', 'test'])

    def testListDelimiterAndPrefix(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())

        delimiter = 'a'
        files = ['bar', 'bazar']
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())

        results = cont.files(parms={'delimiter': delimiter, 'prefix': 'ba'})
        self.assertEqual(results, ['bar', 'baza'])

        results = cont.files(parms={'delimiter': delimiter,
                                    'prefix': 'ba',
                                    'reverse': 'yes'})
        self.assertEqual(results, ['baza', 'bar'])

    def testLeadingDelimiter(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())

        delimiter = '/'
        files = ['test', delimiter.join(['', 'test', 'bar']),
                 delimiter.join(['', 'test', 'bar', 'foo'])]
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())

        results = cont.files(parms={'delimiter': delimiter})
        self.assertEqual(results, [delimiter, 'test'])

    def testCreate(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())
        self.assert_status(201)
        self.assertIn(cont.name, self.env.account.containers())

    def testContainerFileListOnContainerThatDoesNotExist(self):
        for format_type in [None, 'json', 'xml']:
            container = self.env.account.container(Utils.create_name())
            self.assertRaises(ResponseError, container.files,
                              parms={'format': format_type})
            self.assert_status(404)

    def testUtf8Container(self):
        valid_utf8 = Utils.create_utf8_name()
        if six.PY2:
            invalid_utf8 = valid_utf8[::-1]
        else:
            invalid_utf8 = (valid_utf8.encode('utf8')[::-1]).decode(
                'utf-8', 'surrogateescape')
        container = self.env.account.container(valid_utf8)
        self.assertTrue(container.create(cfg={'no_path_quote': True}))
        self.assertIn(container.name, self.env.account.containers())
        self.assertEqual(container.files(), [])
        self.assertTrue(container.delete())

        container = self.env.account.container(invalid_utf8)
        self.assertFalse(container.create(cfg={'no_path_quote': True}))
        self.assert_status(412)
        self.assertRaises(ResponseError, container.files,
                          cfg={'no_path_quote': True})
        self.assert_status(412)

    def testCreateOnExisting(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())
        self.assert_status(201)
        self.assertTrue(cont.create())
        self.assert_status(202)

    def testSlashInName(self):
        if six.PY2:
            cont_name = list(Utils.create_name().decode('utf-8'))
        else:
            cont_name = list(Utils.create_name())
        cont_name[random.randint(2, len(cont_name) - 2)] = '/'
        cont_name = ''.join(cont_name)
        if six.PY2:
            cont_name = cont_name.encode('utf-8')

        cont = self.env.account.container(cont_name)
        self.assertFalse(cont.create(cfg={'no_path_quote': True}),
                         'created container with name %s' % (cont_name))
        self.assert_status(404)
        self.assertNotIn(cont.name, self.env.account.containers())

    def testDelete(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())
        self.assert_status(201)
        self.assertTrue(cont.delete())
        self.assert_status(204)
        self.assertNotIn(cont.name, self.env.account.containers())

    def testDeleteOnContainerThatDoesNotExist(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertFalse(cont.delete())
        self.assert_status(404)

    def testDeleteOnContainerWithFiles(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertTrue(cont.create())
        file_item = cont.file(Utils.create_name())
        file_item.write_random(self.env.file_size)
        self.assertIn(file_item.name, cont.files())
        self.assertFalse(cont.delete())
        self.assert_status(409)

    def testFileCreateInContainerThatDoesNotExist(self):
        file_item = File(self.env.conn, self.env.account, Utils.create_name(),
                         Utils.create_name())
        self.assertRaises(ResponseError, file_item.write)
        self.assert_status(404)

    def testLastFileMarker(self):
        for format_type in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format': format_type})
            self.assertEqual(len(files), len(self.env.files))
            self.assert_status(200)

            marker = files[-1] if format_type is None else files[-1]['name']
            files = self.env.container.files(
                parms={'format': format_type, 'marker': marker})
            self.assertEqual(len(files), 0)

            if format_type is None:
                self.assert_status(204)
            else:
                self.assert_status(200)

    def testContainerFileList(self):
        for format_type in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format': format_type})
            self.assert_status(200)
            if isinstance(files[0], dict):
                files = [x['name'] for x in files]

            for file_item in self.env.files:
                self.assertIn(file_item, files)

            for file_item in files:
                self.assertIn(file_item, self.env.files)

    def _testContainerFormattedFileList(self, format_type):
        expected = {}
        for name in self.env.files:
            expected[name] = self.env.container.file(name).info()

        file_list = self.env.container.files(parms={'format': format_type})
        self.assert_status(200)
        for actual in file_list:
            name = actual['name']
            self.assertIn(name, expected)
            self.assertEqual(expected[name]['etag'], actual['hash'])
            self.assertEqual(
                expected[name]['content_type'], actual['content_type'])
            self.assertEqual(
                expected[name]['content_length'], actual['bytes'])
            expected.pop(name)
        self.assertFalse(expected)  # sanity check

    def testContainerJsonFileList(self):
        self._testContainerFormattedFileList('json')

    def testContainerXmlFileList(self):
        self._testContainerFormattedFileList('xml')

    def testMarkerLimitFileList(self):
        for format_type in [None, 'json', 'xml']:
            for marker in ['0', 'A', 'I', 'R', 'Z', 'a', 'i', 'r', 'z',
                           'abc123', 'mnop', 'xyz']:
                limit = random.randint(2, self.env.file_count - 1)
                files = self.env.container.files(parms={'format': format_type,
                                                        'marker': marker,
                                                        'limit': limit})

                if not files:
                    continue

                if isinstance(files[0], dict):
                    files = [x['name'] for x in files]

                self.assertLessEqual(len(files), limit)
                if files:
                    if isinstance(files[0], dict):
                        files = [x['name'] for x in files]
                    self.assertGreater(locale.strcoll(files[0], marker), 0)

    def testFileOrder(self):
        for format_type in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format': format_type})
            if isinstance(files[0], dict):
                files = [x['name'] for x in files]
            self.assertEqual(sorted(files, key=locale.strxfrm), files)

    def testContainerInfo(self):
        info = self.env.container.info()
        self.assert_status(204)
        self.assertEqual(info['object_count'], self.env.file_count)
        self.assertEqual(info['bytes_used'],
                         self.env.file_count * self.env.file_size)

    def testContainerInfoOnContainerThatDoesNotExist(self):
        container = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, container.info)
        self.assert_status(404)

    def testContainerFileListWithLimit(self):
        for format_type in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format': format_type,
                                                    'limit': 2})
            self.assertEqual(len(files), 2)

    def testContainerExistenceCachingProblem(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, cont.files)
        self.assertTrue(cont.create())
        self.assertEqual(cont.files(), [])

        cont = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, cont.files)
        self.assertTrue(cont.create())
        # NB: no GET! Make sure the PUT cleared the cached 404
        file_item = cont.file(Utils.create_name())
        file_item.write_random()

    def testContainerLastModified(self):
        container = self.env.account.container(Utils.create_name())
        self.assertTrue(container.create())
        info = container.info()
        t0 = info['last_modified']
        # last modified header is in date format which supports in second
        # so we need to wait to increment a sec in the header.
        eventlet.sleep(1)

        # POST container change last modified timestamp
        self.assertTrue(
            container.update_metadata({'x-container-meta-japan': 'mitaka'}))
        info = container.info()
        t1 = info['last_modified']
        self.assertNotEqual(t0, t1)
        eventlet.sleep(1)

        # PUT container (overwrite) also change last modified
        self.assertTrue(container.create())
        info = container.info()
        t2 = info['last_modified']
        self.assertNotEqual(t1, t2)
        eventlet.sleep(1)

        # PUT object doesn't change container last modified timestamp
        obj = container.file(Utils.create_name())
        self.assertTrue(
            obj.write(b"aaaaa", hdrs={'Content-Type': 'text/plain'}))
        info = container.info()
        t3 = info['last_modified']
        self.assertEqual(t2, t3)

        # POST object also doesn't change container last modified timestamp
        self.assertTrue(
            obj.sync_metadata({'us': 'austin'}))
        info = container.info()
        t4 = info['last_modified']
        self.assertEqual(t2, t4)


class TestContainerUTF8(Base2, TestContainer):
    pass


class TestContainerSortingEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestContainerSortingEnv, cls).setUp()
        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_items = ('a1', 'a2', 'A3', 'b1', 'B2', 'a10', 'b10', 'zz')
        cls.files = list()
        cls.file_size = 128
        for name in cls.file_items:
            file_item = cls.container.file(name)
            file_item.write_random(cls.file_size)
            cls.files.append(file_item.name)


class TestContainerSorting(Base):
    env = TestContainerSortingEnv

    def testContainerFileListSortingReversed(self):
        file_list = list(sorted(self.env.file_items))
        file_list.reverse()
        for reverse in ('true', '1', 'yes', 'on', 't', 'y'):
            cont_files = self.env.container.files(parms={'reverse': reverse})
            self.assert_status(200)
            self.assertEqual(file_list, cont_files,
                             'Expected %s but got %s with reverse param %r'
                             % (file_list, cont_files, reverse))

    def testContainerFileSortingByPrefixReversed(self):
        cont_list = sorted(c for c in self.env.file_items if c.startswith('a'))
        cont_list.reverse()
        cont_listing = self.env.container.files(parms={
            'reverse': 'on', 'prefix': 'a'})
        self.assert_status(200)
        self.assertEqual(cont_list, cont_listing)

    def testContainerFileSortingByMarkersExclusiveReversed(self):
        first_item = self.env.file_items[3]  # 'b1' + postfix
        last_item = self.env.file_items[4]  # 'B2' + postfix

        cont_list = sorted(c for c in self.env.file_items
                           if last_item < c < first_item)
        cont_list.reverse()
        cont_listing = self.env.container.files(parms={
            'reverse': 'on', 'marker': first_item, 'end_marker': last_item})
        self.assert_status(200)
        self.assertEqual(cont_list, cont_listing)

    def testContainerFileSortingByMarkersInclusiveReversed(self):
        first_item = self.env.file_items[3]  # 'b1' + postfix
        last_item = self.env.file_items[4]  # 'B2' + postfix

        cont_list = sorted(c for c in self.env.file_items
                           if last_item <= c <= first_item)
        cont_list.reverse()
        cont_listing = self.env.container.files(parms={
            'reverse': 'on', 'marker': first_item + '\x00',
            'end_marker': last_item[:-1] + chr(ord(last_item[-1]) - 1)})
        self.assert_status(200)
        self.assertEqual(cont_list, cont_listing)

    def testContainerFileSortingByReversedMarkersReversed(self):
        cont_listing = self.env.container.files(parms={
            'reverse': 'on', 'marker': 'B', 'end_marker': 'b1'})
        self.assert_status(204)
        self.assertEqual([], cont_listing)

    def testContainerFileListSorting(self):
        file_list = list(sorted(self.env.file_items))
        cont_files = self.env.container.files()
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        # Lets try again but with reverse is specifically turned off
        cont_files = self.env.container.files(parms={'reverse': 'off'})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        cont_files = self.env.container.files(parms={'reverse': 'false'})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        cont_files = self.env.container.files(parms={'reverse': 'no'})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        cont_files = self.env.container.files(parms={'reverse': ''})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        # Lets try again but with a incorrect reverse values
        cont_files = self.env.container.files(parms={'reverse': 'foo'})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        cont_files = self.env.container.files(parms={'reverse': 'hai'})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)

        cont_files = self.env.container.files(parms={'reverse': 'o=[]::::>'})
        self.assert_status(200)
        self.assertEqual(file_list, cont_files)


class TestContainerPathsEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestContainerPathsEnv, cls).setUp()
        cls.file_size = 8

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.files = [
            '/file1',
            '/file A',
            '/dir1/',
            '/dir2/',
            '/dir1/file2',
            '/dir1/subdir1/',
            '/dir1/subdir2/',
            '/dir1/subdir1/file2',
            '/dir1/subdir1/file3',
            '/dir1/subdir1/file4',
            '/dir1/subdir1/subsubdir1/',
            '/dir1/subdir1/subsubdir1/file5',
            '/dir1/subdir1/subsubdir1/file6',
            '/dir1/subdir1/subsubdir1/file7',
            '/dir1/subdir1/subsubdir1/file8',
            '/dir1/subdir1/subsubdir2/',
            '/dir1/subdir1/subsubdir2/file9',
            '/dir1/subdir1/subsubdir2/file0',
            'file1',
            'dir1/',
            'dir2/',
            'dir1/file2',
            'dir1/subdir1/',
            'dir1/subdir2/',
            'dir1/subdir1/file2',
            'dir1/subdir1/file3',
            'dir1/subdir1/file4',
            'dir1/subdir1/subsubdir1/',
            'dir1/subdir1/subsubdir1/file5',
            'dir1/subdir1/subsubdir1/file6',
            'dir1/subdir1/subsubdir1/file7',
            'dir1/subdir1/subsubdir1/file8',
            'dir1/subdir1/subsubdir2/',
            'dir1/subdir1/subsubdir2/file9',
            'dir1/subdir1/subsubdir2/file0',
            'dir1/subdir with spaces/',
            'dir1/subdir with spaces/file B',
            'dir1/subdir+with{whatever/',
            'dir1/subdir+with{whatever/file D',
        ]

        stored_files = set()
        for f in cls.files:
            file_item = cls.container.file(f)
            if f.endswith('/'):
                file_item.write(hdrs={'Content-Type': 'application/directory'})
            else:
                file_item.write_random(cls.file_size,
                                       hdrs={'Content-Type':
                                             'application/directory'})
            if (normalized_urls):
                nfile = '/'.join(filter(None, f.split('/')))
                if (f[-1] == '/'):
                    nfile += '/'
                stored_files.add(nfile)
            else:
                stored_files.add(f)
        cls.stored_files = sorted(stored_files)


class TestContainerPaths(Base):
    env = TestContainerPathsEnv

    def testTraverseContainer(self):
        found_files = []
        found_dirs = []

        def recurse_path(path, count=0):
            if count > 10:
                raise ValueError('too deep recursion')

            for file_item in self.env.container.files(parms={'path': path}):
                self.assertTrue(file_item.startswith(path))
                if file_item.endswith('/'):
                    recurse_path(file_item, count + 1)
                    found_dirs.append(file_item)
                else:
                    found_files.append(file_item)

        recurse_path('')
        for file_item in self.env.stored_files:
            if file_item.startswith('/'):
                self.assertNotIn(file_item, found_dirs)
                self.assertNotIn(file_item, found_files)
            elif file_item.endswith('/'):
                self.assertIn(file_item, found_dirs)
                self.assertNotIn(file_item, found_files)
            else:
                self.assertIn(file_item, found_files)
                self.assertNotIn(file_item, found_dirs)

        found_files = []
        found_dirs = []
        recurse_path('/')
        for file_item in self.env.stored_files:
            if not file_item.startswith('/'):
                self.assertNotIn(file_item, found_dirs)
                self.assertNotIn(file_item, found_files)
            elif file_item.endswith('/'):
                self.assertIn(file_item, found_dirs)
                self.assertNotIn(file_item, found_files)
            else:
                self.assertIn(file_item, found_files)
                self.assertNotIn(file_item, found_dirs)

    def testContainerListing(self):
        for format_type in (None, 'json', 'xml'):
            files = self.env.container.files(parms={'format': format_type})

            if isinstance(files[0], dict):
                files = [str(x['name']) for x in files]

            self.assertEqual(files, self.env.stored_files)

        for format_type in ('json', 'xml'):
            for file_item in self.env.container.files(parms={'format':
                                                             format_type}):
                self.assertGreaterEqual(int(file_item['bytes']), 0)
                self.assertIn('last_modified', file_item)
                if file_item['name'].endswith('/'):
                    self.assertEqual(file_item['content_type'],
                                     'application/directory')

    def testStructure(self):
        def assert_listing(path, file_list):
            files = self.env.container.files(parms={'path': path})
            self.assertEqual(sorted(file_list, key=locale.strxfrm), files)
        if not normalized_urls:
            assert_listing('/', ['/dir1/', '/dir2/', '/file1', '/file A'])
            assert_listing('/dir1',
                           ['/dir1/file2', '/dir1/subdir1/', '/dir1/subdir2/'])
            assert_listing('/dir1/',
                           ['/dir1/file2', '/dir1/subdir1/', '/dir1/subdir2/'])
            assert_listing('/dir1/subdir1',
                           ['/dir1/subdir1/subsubdir2/', '/dir1/subdir1/file2',
                            '/dir1/subdir1/file3', '/dir1/subdir1/file4',
                            '/dir1/subdir1/subsubdir1/'])
            assert_listing('/dir1/subdir2', [])
            assert_listing('', ['file1', 'dir1/', 'dir2/'])
        else:
            assert_listing('', ['file1', 'dir1/', 'dir2/', 'file A'])
        assert_listing('dir1', ['dir1/file2', 'dir1/subdir1/',
                                'dir1/subdir2/', 'dir1/subdir with spaces/',
                                'dir1/subdir+with{whatever/'])
        assert_listing('dir1/subdir1',
                       ['dir1/subdir1/file4', 'dir1/subdir1/subsubdir2/',
                        'dir1/subdir1/file2', 'dir1/subdir1/file3',
                        'dir1/subdir1/subsubdir1/'])
        assert_listing('dir1/subdir1/subsubdir1',
                       ['dir1/subdir1/subsubdir1/file7',
                        'dir1/subdir1/subsubdir1/file5',
                        'dir1/subdir1/subsubdir1/file8',
                        'dir1/subdir1/subsubdir1/file6'])
        assert_listing('dir1/subdir1/subsubdir1/',
                       ['dir1/subdir1/subsubdir1/file7',
                        'dir1/subdir1/subsubdir1/file5',
                        'dir1/subdir1/subsubdir1/file8',
                        'dir1/subdir1/subsubdir1/file6'])
        assert_listing('dir1/subdir with spaces/',
                       ['dir1/subdir with spaces/file B'])


class TestFileEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestFileEnv, cls).setUp()
        if not tf.skip2:
            # creating another account and connection
            # for account to account copy tests
            config2 = deepcopy(tf.config)
            config2['account'] = tf.config['account2']
            config2['username'] = tf.config['username2']
            config2['password'] = tf.config['password2']
            cls.conn2 = Connection(config2)
            cls.conn2.authenticate()

            cls.account2 = cls.conn2.get_account()
            cls.account2.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_size = 128

        # With keystoneauth we need the accounts to have had the project
        # domain id persisted as sysmeta prior to testing ACLs. This may
        # not be the case if, for example, the account was created using
        # a request with reseller_admin role, when project domain id may
        # not have been known. So we ensure that the project domain id is
        # in sysmeta by making a POST to the accounts using an admin role.
        cls.account.update_metadata()
        if not tf.skip2:
            cls.account2.update_metadata()


class TestFileDev(Base):
    env = TestFileEnv


class TestFileDevUTF8(Base2, TestFileDev):
    pass


class TestFile(Base):
    env = TestFileEnv

    def testGetResponseHeaders(self):
        obj_data = b'test_body'

        def do_test(put_hdrs, get_hdrs, expected_hdrs, unexpected_hdrs):
            filename = Utils.create_name()
            file_item = self.env.container.file(filename)
            resp = file_item.write(
                data=obj_data, hdrs=put_hdrs, return_resp=True)

            # put then get an object
            resp.read()
            read_data = file_item.read(hdrs=get_hdrs)
            self.assertEqual(obj_data, read_data)  # sanity check
            resp_headers = file_item.conn.response.getheaders()

            # check the *list* of all header (name, value) pairs rather than
            # constructing a dict in case of repeated names in the list
            errors = []
            for k, v in resp_headers:
                if k.lower() in unexpected_hdrs:
                    errors.append('Found unexpected header %s: %s' % (k, v))
            for k, v in expected_hdrs.items():
                matches = [hdr for hdr in resp_headers if hdr[0] == k]
                if not matches:
                    errors.append('Missing expected header %s' % k)
                for (got_k, got_v) in matches:
                    if got_v != v:
                        errors.append('Expected %s but got %s for %s' %
                                      (v, got_v, k))
            if errors:
                self.fail(
                    'Errors in response headers:\n  %s' % '\n  '.join(errors))

        put_headers = {'X-Object-Meta-Fruit': 'Banana',
                       'X-Delete-After': '10000',
                       'Content-Type': 'application/test'}
        expected_headers = {'content-length': str(len(obj_data)),
                            'x-object-meta-fruit': 'Banana',
                            'accept-ranges': 'bytes',
                            'content-type': 'application/test',
                            'etag': hashlib.md5(obj_data).hexdigest(),
                            'last-modified': mock.ANY,
                            'date': mock.ANY,
                            'x-delete-at': mock.ANY,
                            'x-trans-id': mock.ANY,
                            'x-openstack-request-id': mock.ANY}
        unexpected_headers = ['connection', 'x-delete-after']
        do_test(put_headers, {}, expected_headers, unexpected_headers)

        get_headers = {'Connection': 'keep-alive'}
        expected_headers['connection'] = 'keep-alive'
        unexpected_headers = ['x-delete-after']
        do_test(put_headers, get_headers, expected_headers, unexpected_headers)

    def testCopy(self):
        # makes sure to test encoded characters
        source_filename = 'dealde%2Fl04 011e%204c8df/flash.png'
        file_item = self.env.container.file(source_filename)

        metadata = {}
        metadata[Utils.create_ascii_name()] = Utils.create_name()
        put_headers = {'Content-Type': 'application/test',
                       'Content-Encoding': 'gzip',
                       'Content-Disposition': 'attachment; filename=myfile'}
        file_item.metadata = metadata
        data = file_item.write_random(hdrs=put_headers)

        # the allowed headers are configurable in object server, so we cannot
        # assert that content-encoding and content-disposition get *copied*
        # unless they were successfully set on the original PUT, so populate
        # expected_headers by making a HEAD on the original object
        file_item.initialize()
        self.assertEqual('application/test', file_item.content_type)
        resp_headers = dict(file_item.conn.response.getheaders())
        expected_headers = {}
        for k, v in put_headers.items():
            if k.lower() in resp_headers:
                expected_headers[k] = v

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        # copy both from within and across containers
        for cont in (self.env.container, dest_cont):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                extra_hdrs = {'X-Object-Meta-Extra': 'fresh'}
                self.assertTrue(file_item.copy(
                    '%s%s' % (prefix, cont), dest_filename, hdrs=extra_hdrs))

                # verify container listing for copy
                listing = cont.files(parms={'format': 'json'})
                for obj in listing:
                    if obj['name'] == dest_filename:
                        break
                else:
                    self.fail('Failed to find %s in listing' % dest_filename)

                self.assertEqual(file_item.size, obj['bytes'])
                self.assertEqual(file_item.etag, obj['hash'])
                self.assertEqual(file_item.content_type, obj['content_type'])

                file_copy = cont.file(dest_filename)

                self.assertEqual(data, file_copy.read())
                self.assertTrue(file_copy.initialize())
                expected_metadata = dict(metadata)
                # new metadata should be merged with existing
                expected_metadata['extra'] = 'fresh'
                self.assertDictEqual(expected_metadata, file_copy.metadata)
                resp_headers = dict(file_copy.conn.response.getheaders())
                for k, v in expected_headers.items():
                    self.assertIn(k.lower(), resp_headers)
                    self.assertEqual(v, resp_headers[k.lower()])

                # repeat copy with updated content-type, content-encoding and
                # content-disposition, which should get updated
                extra_hdrs = {
                    'X-Object-Meta-Extra': 'fresher',
                    'Content-Type': 'application/test-changed',
                    'Content-Encoding': 'not_gzip',
                    'Content-Disposition': 'attachment; filename=notmyfile'}
                self.assertTrue(file_item.copy(
                    '%s%s' % (prefix, cont), dest_filename, hdrs=extra_hdrs))

                self.assertIn(dest_filename, cont.files())

                file_copy = cont.file(dest_filename)

                self.assertEqual(data, file_copy.read())
                self.assertTrue(file_copy.initialize())
                expected_metadata['extra'] = 'fresher'
                self.assertDictEqual(expected_metadata, file_copy.metadata)
                resp_headers = dict(file_copy.conn.response.getheaders())
                # if k is in expected_headers then we can assert its new value
                for k, v in expected_headers.items():
                    v = extra_hdrs.get(k, v)
                    self.assertIn(k.lower(), resp_headers)
                    self.assertEqual(v, resp_headers[k.lower()])

                # verify container listing for copy
                listing = cont.files(parms={'format': 'json'})
                for obj in listing:
                    if obj['name'] == dest_filename:
                        break
                else:
                    self.fail('Failed to find %s in listing' % dest_filename)

                self.assertEqual(file_item.size, obj['bytes'])
                self.assertEqual(file_item.etag, obj['hash'])
                self.assertEqual(
                    'application/test-changed', obj['content_type'])

                # repeat copy with X-Fresh-Metadata header - existing user
                # metadata should not be copied, new completely replaces it.
                extra_hdrs = {'Content-Type': 'application/test-updated',
                              'X-Object-Meta-Extra': 'fresher',
                              'X-Fresh-Metadata': 'true'}
                self.assertTrue(file_item.copy(
                    '%s%s' % (prefix, cont), dest_filename, hdrs=extra_hdrs))

                self.assertIn(dest_filename, cont.files())

                file_copy = cont.file(dest_filename)

                self.assertEqual(data, file_copy.read())
                self.assertTrue(file_copy.initialize())
                self.assertEqual('application/test-updated',
                                 file_copy.content_type)
                expected_metadata = {'extra': 'fresher'}
                self.assertDictEqual(expected_metadata, file_copy.metadata)
                resp_headers = dict(file_copy.conn.response.getheaders())
                for k in ('Content-Disposition', 'Content-Encoding'):
                    self.assertNotIn(k.lower(), resp_headers)

                # verify container listing for copy
                listing = cont.files(parms={'format': 'json'})
                for obj in listing:
                    if obj['name'] == dest_filename:
                        break
                else:
                    self.fail('Failed to find %s in listing' % dest_filename)

                self.assertEqual(file_item.size, obj['bytes'])
                self.assertEqual(file_item.etag, obj['hash'])
                self.assertEqual(
                    'application/test-updated', obj['content_type'])

    def testCopyRange(self):
        # makes sure to test encoded characters
        source_filename = 'dealde%2Fl04 011e%204c8df/flash.png'
        file_item = self.env.container.file(source_filename)

        metadata = {Utils.create_ascii_name(): Utils.create_name()}

        data = file_item.write_random(1024)
        file_item.sync_metadata(metadata)
        file_item.initialize()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        expected_body = data[100:201]
        expected_etag = hashlib.md5(expected_body)
        # copy both from within and across containers
        for cont in (self.env.container, dest_cont):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file_item.copy('%s%s' % (prefix, cont), dest_filename,
                               hdrs={'Range': 'bytes=100-200'})
                self.assertEqual(201, file_item.conn.response.status)

                # verify container listing for copy
                listing = cont.files(parms={'format': 'json'})
                for obj in listing:
                    if obj['name'] == dest_filename:
                        break
                else:
                    self.fail('Failed to find %s in listing' % dest_filename)

                self.assertEqual(101, obj['bytes'])
                self.assertEqual(expected_etag.hexdigest(), obj['hash'])
                self.assertEqual(file_item.content_type, obj['content_type'])

                # verify copy object
                copy_file_item = cont.file(dest_filename)
                self.assertEqual(expected_body, copy_file_item.read())
                self.assertTrue(copy_file_item.initialize())
                self.assertEqual(metadata, copy_file_item.metadata)

    def testCopyAccount(self):
        # makes sure to test encoded characters
        source_filename = 'dealde%2Fl04 011e%204c8df/flash.png'
        file_item = self.env.container.file(source_filename)

        metadata = {Utils.create_ascii_name(): Utils.create_name()}

        data = file_item.write_random()
        file_item.sync_metadata(metadata)

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        acct = self.env.conn.account_name
        # copy both from within and across containers
        for cont in (self.env.container, dest_cont):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file_item = self.env.container.file(source_filename)
                file_item.copy_account(acct,
                                       '%s%s' % (prefix, cont),
                                       dest_filename)

                self.assertIn(dest_filename, cont.files())

                file_item = cont.file(dest_filename)

                self.assertEqual(data, file_item.read())
                self.assertTrue(file_item.initialize())
                self.assertEqual(metadata, file_item.metadata)

        if not tf.skip2:
            dest_cont = self.env.account2.container(Utils.create_name())
            self.assertTrue(dest_cont.create(hdrs={
                'X-Container-Write': self.env.conn.user_acl
            }))

            acct = self.env.conn2.account_name
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file_item = self.env.container.file(source_filename)
                file_item.copy_account(acct,
                                       '%s%s' % (prefix, dest_cont),
                                       dest_filename)

                self.assertIn(dest_filename, dest_cont.files())

                file_item = dest_cont.file(dest_filename)

                self.assertEqual(data, file_item.read())
                self.assertTrue(file_item.initialize())
                self.assertEqual(metadata, file_item.metadata)

    def testCopy404s(self):
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)
        file_item.write_random()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        for prefix in ('', '/'):
            # invalid source container
            source_cont = self.env.account.container(Utils.create_name())
            file_item = source_cont.file(source_filename)
            self.assertRaises(ResponseError, file_item.copy,
                              '%s%s' % (prefix, self.env.container),
                              Utils.create_name())
            self.assert_status(404)

            self.assertRaises(ResponseError, file_item.copy,
                              '%s%s' % (prefix, dest_cont),
                              Utils.create_name())
            self.assert_status(404)

            # invalid source object
            file_item = self.env.container.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.copy,
                              '%s%s' % (prefix, self.env.container),
                              Utils.create_name())
            self.assert_status(404)

            self.assertRaises(ResponseError, file_item.copy,
                              '%s%s' % (prefix, dest_cont),
                              Utils.create_name())
            self.assert_status(404)

            # invalid destination container
            file_item = self.env.container.file(source_filename)
            self.assertRaises(ResponseError, file_item.copy,
                              '%s%s' % (prefix, Utils.create_name()),
                              Utils.create_name())

    def testCopyAccount404s(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        acct = self.env.conn.account_name
        acct2 = self.env.conn2.account_name
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)
        file_item.write_random()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create(hdrs={
            'X-Container-Read': self.env.conn2.user_acl
        }))
        dest_cont2 = self.env.account2.container(Utils.create_name())
        self.assertTrue(dest_cont2.create(hdrs={
            'X-Container-Write': self.env.conn.user_acl,
            'X-Container-Read': self.env.conn.user_acl
        }))

        for acct, cont in ((acct, dest_cont), (acct2, dest_cont2)):
            for prefix in ('', '/'):
                # invalid source container
                source_cont = self.env.account.container(Utils.create_name())
                file_item = source_cont.file(source_filename)
                self.assertRaises(ResponseError, file_item.copy_account,
                                  acct,
                                  '%s%s' % (prefix, self.env.container),
                                  Utils.create_name())
                # there is no such source container but user has
                # permissions to do a GET (done internally via COPY) for
                # objects in his own account.
                self.assert_status(404)

                self.assertRaises(ResponseError, file_item.copy_account,
                                  acct,
                                  '%s%s' % (prefix, cont),
                                  Utils.create_name())
                self.assert_status(404)

                # invalid source object
                file_item = self.env.container.file(Utils.create_name())
                self.assertRaises(ResponseError, file_item.copy_account,
                                  acct,
                                  '%s%s' % (prefix, self.env.container),
                                  Utils.create_name())
                # there is no such source container but user has
                # permissions to do a GET (done internally via COPY) for
                # objects in his own account.
                self.assert_status(404)

                self.assertRaises(ResponseError, file_item.copy_account,
                                  acct,
                                  '%s%s' % (prefix, cont),
                                  Utils.create_name())
                self.assert_status(404)

                # invalid destination container
                file_item = self.env.container.file(source_filename)
                self.assertRaises(ResponseError, file_item.copy_account,
                                  acct,
                                  '%s%s' % (prefix, Utils.create_name()),
                                  Utils.create_name())
                if acct == acct2:
                    # there is no such destination container
                    # and foreign user can have no permission to write there
                    self.assert_status(403)
                else:
                    self.assert_status(404)

    def testCopyNoDestinationHeader(self):
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)
        file_item.write_random()

        file_item = self.env.container.file(source_filename)
        self.assertRaises(ResponseError, file_item.copy, Utils.create_name(),
                          Utils.create_name(),
                          cfg={'no_destination': True})
        self.assert_status(412)

    def testCopyDestinationSlashProblems(self):
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)
        file_item.write_random()

        # no slash
        self.assertRaises(ResponseError, file_item.copy, Utils.create_name(),
                          Utils.create_name(),
                          cfg={'destination': Utils.create_name()})
        self.assert_status(412)

        # too many slashes
        self.assertRaises(ResponseError, file_item.copy, Utils.create_name(),
                          Utils.create_name(),
                          cfg={'destination': '//%s' % Utils.create_name()})
        self.assert_status(412)

    def testCopyFromHeader(self):
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)

        metadata = {}
        for i in range(1):
            metadata[Utils.create_ascii_name()] = Utils.create_name()
        file_item.metadata = metadata

        data = file_item.write_random()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        # copy both from within and across containers
        for cont in (self.env.container, dest_cont):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file_item = cont.file(dest_filename)
                file_item.write(hdrs={'X-Copy-From': '%s%s/%s' % (
                    prefix, self.env.container.name, source_filename)})

                self.assertIn(dest_filename, cont.files())

                file_item = cont.file(dest_filename)

                self.assertEqual(data, file_item.read())
                self.assertTrue(file_item.initialize())
                self.assertEqual(metadata, file_item.metadata)

    def testCopyFromAccountHeader(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        acct = self.env.conn.account_name
        src_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(src_cont.create(hdrs={
            'X-Container-Read': self.env.conn2.user_acl
        }))
        source_filename = Utils.create_name()
        file_item = src_cont.file(source_filename)

        metadata = {}
        for i in range(1):
            metadata[Utils.create_ascii_name()] = Utils.create_name()
        file_item.metadata = metadata

        data = file_item.write_random()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())
        dest_cont2 = self.env.account2.container(Utils.create_name())
        self.assertTrue(dest_cont2.create(hdrs={
            'X-Container-Write': self.env.conn.user_acl
        }))

        for cont in (src_cont, dest_cont, dest_cont2):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file_item = cont.file(dest_filename)
                file_item.write(hdrs={'X-Copy-From-Account': acct,
                                      'X-Copy-From': '%s%s/%s' % (
                                          prefix,
                                          src_cont.name,
                                          source_filename)})

                self.assertIn(dest_filename, cont.files())

                file_item = cont.file(dest_filename)

                self.assertEqual(data, file_item.read())
                self.assertTrue(file_item.initialize())
                self.assertEqual(metadata, file_item.metadata)

    def testCopyFromHeader404s(self):
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)
        file_item.write_random()

        for prefix in ('', '/'):
            # invalid source container
            file_item = self.env.container.file(Utils.create_name())
            copy_from = ('%s%s/%s'
                         % (prefix, Utils.create_name(), source_filename))
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From': copy_from})
            self.assert_status(404)

            # invalid source object
            copy_from = ('%s%s/%s'
                         % (prefix, self.env.container.name,
                            Utils.create_name()))
            file_item = self.env.container.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From': copy_from})
            self.assert_status(404)

            # invalid destination container
            dest_cont = self.env.account.container(Utils.create_name())
            file_item = dest_cont.file(Utils.create_name())
            copy_from = ('%s%s/%s'
                         % (prefix, self.env.container.name, source_filename))
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From': copy_from})
            self.assert_status(404)

    def testCopyFromAccountHeader404s(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        acct = self.env.conn2.account_name
        src_cont = self.env.account2.container(Utils.create_name())
        self.assertTrue(src_cont.create(hdrs={
            'X-Container-Read': self.env.conn.user_acl
        }))
        source_filename = Utils.create_name()
        file_item = src_cont.file(source_filename)
        file_item.write_random()
        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        for prefix in ('', '/'):
            # invalid source container
            file_item = dest_cont.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From-Account': acct,
                                    'X-Copy-From': '%s%s/%s' %
                                                   (prefix,
                                                    Utils.create_name(),
                                                    source_filename)})
            self.assert_status(403)

            # invalid source object
            file_item = self.env.container.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From-Account': acct,
                                    'X-Copy-From': '%s%s/%s' %
                                                   (prefix,
                                                    src_cont,
                                                    Utils.create_name())})
            self.assert_status(404)

            # invalid destination container
            dest_cont = self.env.account.container(Utils.create_name())
            file_item = dest_cont.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From-Account': acct,
                                    'X-Copy-From': '%s%s/%s' %
                                                   (prefix,
                                                    src_cont,
                                                    source_filename)})
            self.assert_status(404)

    def testCopyFromAccountHeader403s(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        acct = self.env.conn2.account_name
        src_cont = self.env.account2.container(Utils.create_name())
        self.assertTrue(src_cont.create())  # Primary user has no access
        source_filename = Utils.create_name()
        file_item = src_cont.file(source_filename)
        file_item.write_random()
        dest_cont = self.env.account.container(Utils.create_name())
        self.assertTrue(dest_cont.create())

        for prefix in ('', '/'):
            # invalid source container
            file_item = dest_cont.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From-Account': acct,
                                    'X-Copy-From': '%s%s/%s' %
                                                   (prefix,
                                                    Utils.create_name(),
                                                    source_filename)})
            self.assert_status(403)

            # invalid source object
            file_item = self.env.container.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From-Account': acct,
                                    'X-Copy-From': '%s%s/%s' %
                                                   (prefix,
                                                    src_cont,
                                                    Utils.create_name())})
            self.assert_status(403)

            # invalid destination container
            dest_cont = self.env.account.container(Utils.create_name())
            file_item = dest_cont.file(Utils.create_name())
            self.assertRaises(ResponseError, file_item.write,
                              hdrs={'X-Copy-From-Account': acct,
                                    'X-Copy-From': '%s%s/%s' %
                                                   (prefix,
                                                    src_cont,
                                                    source_filename)})
            self.assert_status(403)

    def testNameLimit(self):
        limit = load_constraint('max_object_name_length')

        for l in (1, 10, limit // 2, limit - 1, limit, limit + 1, limit * 2):
            file_item = self.env.container.file('a' * l)

            if l <= limit:
                self.assertTrue(file_item.write())
                self.assert_status(201)
            else:
                self.assertRaises(ResponseError, file_item.write)
                self.assert_status(400)

    def testQuestionMarkInName(self):
        if Utils.create_name == Utils.create_ascii_name:
            file_name = list(Utils.create_name())
            file_name[random.randint(2, len(file_name) - 2)] = '?'
            file_name = "".join(file_name)
        else:
            file_name = Utils.create_name(6) + '?' + Utils.create_name(6)

        file_item = self.env.container.file(file_name)
        self.assertTrue(file_item.write(cfg={'no_path_quote': True}))
        self.assertNotIn(file_name, self.env.container.files())
        self.assertIn(file_name.split('?')[0], self.env.container.files())

    def testDeleteThen404s(self):
        file_item = self.env.container.file(Utils.create_name())
        self.assertTrue(file_item.write_random())
        self.assert_status(201)

        self.assertTrue(file_item.delete())
        self.assert_status(204)

        file_item.metadata = {Utils.create_ascii_name(): Utils.create_name()}

        for method in (file_item.info,
                       file_item.read,
                       file_item.sync_metadata,
                       file_item.delete):
            self.assertRaises(ResponseError, method)
            self.assert_status(404)

    def testBlankMetadataName(self):
        file_item = self.env.container.file(Utils.create_name())
        file_item.metadata = {'': Utils.create_name()}
        self.assertRaises(ResponseError, file_item.write_random)
        self.assert_status(400)

    def testMetadataNumberLimit(self):
        number_limit = load_constraint('max_meta_count')
        size_limit = load_constraint('max_meta_overall_size')

        for i in (number_limit - 10, number_limit - 1, number_limit,
                  number_limit + 1, number_limit + 10, number_limit + 100):

            j = size_limit // (i * 2)

            metadata = {}
            while len(metadata.keys()) < i:
                key = Utils.create_ascii_name()
                val = Utils.create_name()

                if len(key) > j:
                    key = key[:j]
                    val = val[:j]

                metadata[key] = val

            file_item = self.env.container.file(Utils.create_name())
            file_item.metadata = metadata

            if i <= number_limit:
                self.assertTrue(file_item.write())
                self.assert_status(201)
                self.assertTrue(file_item.sync_metadata())
                self.assert_status(202)
            else:
                self.assertRaises(ResponseError, file_item.write)
                self.assert_status(400)
                file_item.metadata = {}
                self.assertTrue(file_item.write())
                self.assert_status(201)
                file_item.metadata = metadata
                self.assertRaises(ResponseError, file_item.sync_metadata)
                self.assert_status(400)

    def testContentTypeGuessing(self):
        file_types = {'wav': 'audio/x-wav', 'txt': 'text/plain',
                      'zip': 'application/zip'}

        container = self.env.account.container(Utils.create_name())
        self.assertTrue(container.create())

        for i in file_types.keys():
            file_item = container.file(Utils.create_name() + '.' + i)
            file_item.write(b'', cfg={'no_content_type': True})

        file_types_read = {}
        for i in container.files(parms={'format': 'json'}):
            file_types_read[i['name'].split('.')[1]] = i['content_type']

        self.assertEqual(file_types, file_types_read)

    def testRangedGets(self):
        # We set the file_length to a strange multiple here. This is to check
        # that ranges still work in the EC case when the requested range
        # spans EC segment boundaries. The 1 MiB base value is chosen because
        # that's a common EC segment size. The 1.33 multiple is to ensure we
        # aren't aligned on segment boundaries
        file_length = int(1048576 * 1.33)
        range_size = file_length // 10
        file_item = self.env.container.file(Utils.create_name())
        data = file_item.write_random(file_length)

        for i in range(0, file_length, range_size):
            range_string = 'bytes=%d-%d' % (i, i + range_size - 1)
            hdrs = {'Range': range_string}
            self.assertEqual(
                data[i: i + range_size], file_item.read(hdrs=hdrs),
                range_string)

            range_string = 'bytes=-%d' % (i)
            hdrs = {'Range': range_string}
            if i == 0:
                # RFC 2616 14.35.1
                # "If a syntactically valid byte-range-set includes ... at
                # least one suffix-byte-range-spec with a NON-ZERO
                # suffix-length, then the byte-range-set is satisfiable.
                # Otherwise, the byte-range-set is unsatisfiable.
                self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
                self.assert_status(416)
                self.assert_header('content-range', 'bytes */%d' % file_length)
            else:
                self.assertEqual(file_item.read(hdrs=hdrs), data[-i:])
                self.assert_header('content-range', 'bytes %d-%d/%d' % (
                    file_length - i, file_length - 1, file_length))
            self.assert_header('etag', file_item.md5)
            self.assert_header('accept-ranges', 'bytes')

            range_string = 'bytes=%d-' % (i)
            hdrs = {'Range': range_string}
            self.assertEqual(
                file_item.read(hdrs=hdrs), data[i - file_length:],
                range_string)

        range_string = 'bytes=%d-%d' % (file_length + 1000, file_length + 2000)
        hdrs = {'Range': range_string}
        self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
        self.assert_status(416)
        self.assert_header('content-range', 'bytes */%d' % file_length)
        self.assert_header('etag', file_item.md5)
        self.assert_header('accept-ranges', 'bytes')

        range_string = 'bytes=%d-%d' % (file_length - 1000, file_length + 2000)
        hdrs = {'Range': range_string}
        self.assertEqual(file_item.read(hdrs=hdrs), data[-1000:], range_string)

        hdrs = {'Range': '0-4'}
        self.assertEqual(file_item.read(hdrs=hdrs), data, '0-4')

        # RFC 2616 14.35.1
        # "If the entity is shorter than the specified suffix-length, the
        # entire entity-body is used."
        range_string = 'bytes=-%d' % (file_length + 10)
        hdrs = {'Range': range_string}
        self.assertEqual(file_item.read(hdrs=hdrs), data, range_string)

    def testMultiRangeGets(self):
        file_length = 10000
        range_size = file_length // 10
        subrange_size = range_size // 10
        file_item = self.env.container.file(Utils.create_name())
        data = file_item.write_random(
            file_length, hdrs={"Content-Type":
                               "lovecraft/rugose; squamous=true"})

        for i in range(0, file_length, range_size):
            range_string = 'bytes=%d-%d,%d-%d,%d-%d' % (
                i, i + subrange_size - 1,
                i + 2 * subrange_size, i + 3 * subrange_size - 1,
                i + 4 * subrange_size, i + 5 * subrange_size - 1)
            hdrs = {'Range': range_string}

            fetched = file_item.read(hdrs=hdrs)
            self.assert_status(206)
            content_type = file_item.content_type
            self.assertTrue(content_type.startswith("multipart/byteranges"))
            self.assertIsNone(file_item.content_range)

            # email.parser.FeedParser wants a message with headers on the
            # front, then two CRLFs, and then a body (like emails have but
            # HTTP response bodies don't). We fake it out by constructing a
            # one-header preamble containing just the Content-Type, then
            # feeding in the response body.
            parser = email.parser.FeedParser()
            parser.feed("Content-Type: %s\r\n\r\n" % content_type)
            parser.feed(fetched)
            root_message = parser.close()
            self.assertTrue(root_message.is_multipart())

            byteranges = root_message.get_payload()
            self.assertEqual(len(byteranges), 3)

            self.assertEqual(byteranges[0]['Content-Type'],
                             "lovecraft/rugose; squamous=true")
            self.assertEqual(
                byteranges[0]['Content-Range'],
                "bytes %d-%d/%d" % (i, i + subrange_size - 1, file_length))
            self.assertEqual(
                byteranges[0].get_payload(),
                data[i:(i + subrange_size)])

            self.assertEqual(byteranges[1]['Content-Type'],
                             "lovecraft/rugose; squamous=true")
            self.assertEqual(
                byteranges[1]['Content-Range'],
                "bytes %d-%d/%d" % (i + 2 * subrange_size,
                                    i + 3 * subrange_size - 1, file_length))
            self.assertEqual(
                byteranges[1].get_payload(),
                data[(i + 2 * subrange_size):(i + 3 * subrange_size)])

            self.assertEqual(byteranges[2]['Content-Type'],
                             "lovecraft/rugose; squamous=true")
            self.assertEqual(
                byteranges[2]['Content-Range'],
                "bytes %d-%d/%d" % (i + 4 * subrange_size,
                                    i + 5 * subrange_size - 1, file_length))
            self.assertEqual(
                byteranges[2].get_payload(),
                data[(i + 4 * subrange_size):(i + 5 * subrange_size)])

        # The first two ranges are satisfiable but the third is not; the
        # result is a multipart/byteranges response containing only the two
        # satisfiable byteranges.
        range_string = 'bytes=%d-%d,%d-%d,%d-%d' % (
            0, subrange_size - 1,
            2 * subrange_size, 3 * subrange_size - 1,
            file_length, file_length + subrange_size - 1)
        hdrs = {'Range': range_string}
        fetched = file_item.read(hdrs=hdrs)
        self.assert_status(206)
        content_type = file_item.content_type
        self.assertTrue(content_type.startswith("multipart/byteranges"))
        self.assertIsNone(file_item.content_range)

        parser = email.parser.FeedParser()
        parser.feed("Content-Type: %s\r\n\r\n" % content_type)
        parser.feed(fetched)
        root_message = parser.close()

        self.assertTrue(root_message.is_multipart())
        byteranges = root_message.get_payload()
        self.assertEqual(len(byteranges), 2)

        self.assertEqual(byteranges[0]['Content-Type'],
                         "lovecraft/rugose; squamous=true")
        self.assertEqual(
            byteranges[0]['Content-Range'],
            "bytes %d-%d/%d" % (0, subrange_size - 1, file_length))
        self.assertEqual(byteranges[0].get_payload(), data[:subrange_size])

        self.assertEqual(byteranges[1]['Content-Type'],
                         "lovecraft/rugose; squamous=true")
        self.assertEqual(
            byteranges[1]['Content-Range'],
            "bytes %d-%d/%d" % (2 * subrange_size, 3 * subrange_size - 1,
                                file_length))
        self.assertEqual(
            byteranges[1].get_payload(),
            data[(2 * subrange_size):(3 * subrange_size)])

        # The first range is satisfiable but the second is not; the
        # result is either a multipart/byteranges response containing one
        # byterange or a normal, non-MIME 206 response.
        range_string = 'bytes=%d-%d,%d-%d' % (
            0, subrange_size - 1,
            file_length, file_length + subrange_size - 1)
        hdrs = {'Range': range_string}
        fetched = file_item.read(hdrs=hdrs)
        self.assert_status(206)
        content_type = file_item.content_type
        if content_type.startswith("multipart/byteranges"):
            self.assertIsNone(file_item.content_range)
            parser = email.parser.FeedParser()
            parser.feed("Content-Type: %s\r\n\r\n" % content_type)
            parser.feed(fetched)
            root_message = parser.close()

            self.assertTrue(root_message.is_multipart())
            byteranges = root_message.get_payload()
            self.assertEqual(len(byteranges), 1)

            self.assertEqual(byteranges[0]['Content-Type'],
                             "lovecraft/rugose; squamous=true")
            self.assertEqual(
                byteranges[0]['Content-Range'],
                "bytes %d-%d/%d" % (0, subrange_size - 1, file_length))
            self.assertEqual(byteranges[0].get_payload(), data[:subrange_size])
        else:
            self.assertEqual(
                file_item.content_range,
                "bytes %d-%d/%d" % (0, subrange_size - 1, file_length))
            self.assertEqual(content_type, "lovecraft/rugose; squamous=true")
            self.assertEqual(fetched, data[:subrange_size])

        # No byterange is satisfiable, so we get a 416 response.
        range_string = 'bytes=%d-%d,%d-%d' % (
            file_length, file_length + 2,
            file_length + 100, file_length + 102)
        hdrs = {'Range': range_string}

        self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
        self.assert_status(416)
        self.assert_header('content-range', 'bytes */%d' % file_length)

    def testRangedGetsWithLWSinHeader(self):
        file_length = 10000
        file_item = self.env.container.file(Utils.create_name())
        data = file_item.write_random(file_length)

        for r in ('BYTES=0-999', 'bytes = 0-999', 'BYTES = 0 - 999',
                  'bytes = 0 - 999', 'bytes=0 - 999', 'bytes=0-999 '):

            self.assertEqual(file_item.read(hdrs={'Range': r}), data[0:1000])

    def testFileSizeLimit(self):
        limit = load_constraint('max_file_size')
        tsecs = 3

        def timeout(seconds, method, *args, **kwargs):
            try:
                with eventlet.Timeout(seconds):
                    method(*args, **kwargs)
            except eventlet.Timeout:
                return True
            else:
                return False

        # This loop will result in fallocate calls for 4x the limit
        # (minus 111 bytes). With fallocate turned on in the object servers,
        # this may fail if you don't have 4x the limit available on your
        # data drives.

        # Note that this test does not actually send any data to the system.
        # All it does is ensure that a response (success or failure) comes
        # back within 3 seconds. For the successful tests (size smaller
        # than limit), the cluster will log a 499.

        for i in (limit - 100, limit - 10, limit - 1, limit, limit + 1,
                  limit + 10, limit + 100):

            file_item = self.env.container.file(Utils.create_name())

            if i <= limit:
                self.assertTrue(timeout(tsecs, file_item.write,
                                cfg={'set_content_length': i}))
            else:
                self.assertRaises(ResponseError, timeout, tsecs,
                                  file_item.write,
                                  cfg={'set_content_length': i})

    def testNoContentLengthForPut(self):
        file_item = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.write, b'testing',
                          cfg={'no_content_length': True})
        self.assert_status(411)

    def testDelete(self):
        file_item = self.env.container.file(Utils.create_name())
        file_item.write_random(self.env.file_size)

        self.assertIn(file_item.name, self.env.container.files())
        self.assertTrue(file_item.delete())
        self.assertNotIn(file_item.name, self.env.container.files())

    def testBadHeaders(self):
        file_length = 100

        # no content type on puts should be ok
        file_item = self.env.container.file(Utils.create_name())
        file_item.write_random(file_length, cfg={'no_content_type': True})
        self.assert_status(201)

        # content length x
        self.assertRaises(ResponseError, file_item.write_random, file_length,
                          hdrs={'Content-Length': 'X'},
                          cfg={'no_content_length': True})
        self.assert_status(400)

        # no content-length
        self.assertRaises(ResponseError, file_item.write_random, file_length,
                          cfg={'no_content_length': True})
        self.assert_status(411)

        self.assertRaises(ResponseError, file_item.write_random, file_length,
                          hdrs={'transfer-encoding': 'gzip,chunked'},
                          cfg={'no_content_length': True})
        self.assert_status(501)

        # bad request types
        # for req in ('LICK', 'GETorHEAD_base', 'container_info',
        #             'best_response'):
        for req in ('LICK', 'GETorHEAD_base'):
            self.env.account.conn.make_request(req)
            self.assert_status(405)

        # bad range headers
        self.assertEqual(
            len(file_item.read(hdrs={'Range': 'parsecs=8-12'})),
            file_length)
        self.assert_status(200)

    def testMetadataLengthLimits(self):
        key_limit = load_constraint('max_meta_name_length')
        value_limit = load_constraint('max_meta_value_length')
        lengths = [[key_limit, value_limit], [key_limit, value_limit + 1],
                   [key_limit + 1, value_limit], [key_limit, 0],
                   [key_limit, value_limit * 10],
                   [key_limit * 10, value_limit]]

        for l in lengths:
            metadata = {'a' * l[0]: 'b' * l[1]}
            file_item = self.env.container.file(Utils.create_name())
            file_item.metadata = metadata

            if l[0] <= key_limit and l[1] <= value_limit:
                self.assertTrue(file_item.write())
                self.assert_status(201)
                self.assertTrue(file_item.sync_metadata())
            else:
                self.assertRaises(ResponseError, file_item.write)
                self.assert_status(400)
                file_item.metadata = {}
                self.assertTrue(file_item.write())
                self.assert_status(201)
                file_item.metadata = metadata
                self.assertRaises(ResponseError, file_item.sync_metadata)
                self.assert_status(400)

    def testEtagWayoff(self):
        file_item = self.env.container.file(Utils.create_name())
        hdrs = {'etag': 'reallylonganddefinitelynotavalidetagvalue'}
        self.assertRaises(ResponseError, file_item.write_random, hdrs=hdrs)
        self.assert_status(422)

    def testFileCreate(self):
        for i in range(10):
            file_item = self.env.container.file(Utils.create_name())
            data = file_item.write_random()
            self.assert_status(201)
            self.assertEqual(data, file_item.read())
            self.assert_status(200)

    def testHead(self):
        file_name = Utils.create_name()
        content_type = Utils.create_name()

        file_item = self.env.container.file(file_name)
        file_item.content_type = content_type
        file_item.write_random(self.env.file_size)

        md5 = file_item.md5

        file_item = self.env.container.file(file_name)
        info = file_item.info()

        self.assert_status(200)
        self.assertEqual(info['content_length'], self.env.file_size)
        self.assertEqual(info['etag'], md5)
        self.assertEqual(info['content_type'], content_type)
        self.assertIn('last_modified', info)

    def testDeleteOfFileThatDoesNotExist(self):
        # in container that exists
        file_item = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.delete)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file_item = container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.delete)
        self.assert_status(404)

    def testHeadOnFileThatDoesNotExist(self):
        # in container that exists
        file_item = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.info)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file_item = container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.info)
        self.assert_status(404)

    def testMetadataOnPost(self):
        file_item = self.env.container.file(Utils.create_name())
        file_item.write_random(self.env.file_size)

        for i in range(10):
            metadata = {}
            for j in range(10):
                metadata[Utils.create_ascii_name()] = Utils.create_name()

            file_item.metadata = metadata
            self.assertTrue(file_item.sync_metadata())
            self.assert_status(202)

            file_item = self.env.container.file(file_item.name)
            self.assertTrue(file_item.initialize())
            self.assert_status(200)
            self.assertEqual(file_item.metadata, metadata)

    def testGetContentType(self):
        file_name = Utils.create_name()
        content_type = Utils.create_name()

        file_item = self.env.container.file(file_name)
        file_item.content_type = content_type
        file_item.write_random()

        file_item = self.env.container.file(file_name)
        file_item.read()

        self.assertEqual(content_type, file_item.content_type)

    def testGetOnFileThatDoesNotExist(self):
        # in container that exists
        file_item = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.read)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file_item = container.file(Utils.create_name())
        self.assertRaises(ResponseError, file_item.read)
        self.assert_status(404)

    def testPostOnFileThatDoesNotExist(self):
        # in container that exists
        file_item = self.env.container.file(Utils.create_name())
        file_item.metadata['Field'] = 'Value'
        self.assertRaises(ResponseError, file_item.sync_metadata)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file_item = container.file(Utils.create_name())
        file_item.metadata['Field'] = 'Value'
        self.assertRaises(ResponseError, file_item.sync_metadata)
        self.assert_status(404)

    def testMetadataOnPut(self):
        for i in range(10):
            metadata = {}
            for j in range(10):
                metadata[Utils.create_ascii_name()] = Utils.create_name()

            file_item = self.env.container.file(Utils.create_name())
            file_item.metadata = metadata
            file_item.write_random(self.env.file_size)

            file_item = self.env.container.file(file_item.name)
            self.assertTrue(file_item.initialize())
            self.assert_status(200)
            self.assertEqual(file_item.metadata, metadata)

    def testSerialization(self):
        container = self.env.account.container(Utils.create_name())
        self.assertTrue(container.create())

        files = []
        for i in (0, 1, 10, 100, 1000, 10000):
            files.append({'name': Utils.create_name(),
                          'content_type': Utils.create_name(), 'bytes': i})

        write_time = time.time()
        for f in files:
            file_item = container.file(f['name'])
            file_item.content_type = f['content_type']
            file_item.write_random(f['bytes'])

            f['hash'] = file_item.md5
            f['json'] = False
            f['xml'] = False
        write_time = time.time() - write_time

        for format_type in ['json', 'xml']:
            for file_item in container.files(parms={'format': format_type}):
                found = False
                for f in files:
                    if f['name'] != file_item['name']:
                        continue

                    self.assertEqual(file_item['content_type'],
                                     f['content_type'])
                    self.assertEqual(int(file_item['bytes']), f['bytes'])

                    d = datetime.strptime(
                        file_item['last_modified'].split('.')[0],
                        "%Y-%m-%dT%H:%M:%S")
                    lm = time.mktime(d.timetuple())

                    if 'last_modified' in f:
                        self.assertEqual(f['last_modified'], lm)
                    else:
                        f['last_modified'] = lm

                    f[format_type] = True
                    found = True

                self.assertTrue(
                    found, 'Unexpected file %s found in '
                    '%s listing' % (file_item['name'], format_type))

            headers = dict(self.env.conn.response.getheaders())
            if format_type == 'json':
                self.assertEqual(headers['content-type'],
                                 'application/json; charset=utf-8')
            elif format_type == 'xml':
                self.assertEqual(headers['content-type'],
                                 'application/xml; charset=utf-8')

        lm_diff = max([f['last_modified'] for f in files]) -\
            min([f['last_modified'] for f in files])
        self.assertLess(lm_diff, write_time + 1,
                        'Diff in last modified times '
                        'should be less than time to write files')

        for f in files:
            for format_type in ['json', 'xml']:
                self.assertTrue(
                    f[format_type], 'File %s not found in %s listing'
                    % (f['name'], format_type))

    def testStackedOverwrite(self):
        file_item = self.env.container.file(Utils.create_name())

        for i in range(1, 11):
            data = file_item.write_random(512)
            file_item.write(data)

        self.assertEqual(file_item.read(), data)

    def testZeroByteFile(self):
        file_item = self.env.container.file(Utils.create_name())

        self.assertTrue(file_item.write(b''))
        self.assertIn(file_item.name, self.env.container.files())
        self.assertEqual(file_item.read(), b'')

    def testEtagResponse(self):
        file_item = self.env.container.file(Utils.create_name())

        data = six.BytesIO(file_item.write_random(512))
        etag = File.compute_md5sum(data)

        headers = dict(self.env.conn.response.getheaders())
        self.assertIn('etag', headers.keys())

        header_etag = headers['etag'].strip('"')
        self.assertEqual(etag, header_etag)

    def testChunkedPut(self):
        if (tf.web_front_end == 'apache2'):
            raise SkipTest("Chunked PUT cannot be tested with apache2 web "
                           "front end")

        def chunks(s, length=3):
            i, j = 0, length
            while i < len(s):
                yield s[i:j]
                i, j = j, j + length

        data = File.random_data(10000)
        etag = File.compute_md5sum(data)

        for i in (1, 10, 100, 1000):
            file_item = self.env.container.file(Utils.create_name())

            for j in chunks(data, i):
                file_item.chunked_write(j)

            self.assertTrue(file_item.chunked_write())
            self.assertEqual(data, file_item.read())

            info = file_item.info()
            self.assertEqual(etag, info['etag'])

    def test_POST(self):
        # verify consistency between object and container listing metadata
        file_name = Utils.create_name()
        file_item = self.env.container.file(file_name)
        file_item.content_type = 'text/foobar'
        file_item.write_random(1024)

        # sanity check
        file_item = self.env.container.file(file_name)
        file_item.initialize()
        self.assertEqual('text/foobar', file_item.content_type)
        self.assertEqual(1024, file_item.size)
        etag = file_item.etag

        # check container listing is consistent
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_name:
                break
        else:
            self.fail('Failed to find file %r in listing' % file_name)
        self.assertEqual(1024, f_dict['bytes'])
        self.assertEqual('text/foobar', f_dict['content_type'])
        self.assertEqual(etag, f_dict['hash'])
        put_last_modified = f_dict['last_modified']

        # now POST updated content-type to each file
        file_item = self.env.container.file(file_name)
        file_item.content_type = 'image/foobarbaz'
        file_item.sync_metadata({'Test': 'blah'})

        # sanity check object metadata
        file_item = self.env.container.file(file_name)
        file_item.initialize()

        self.assertEqual(1024, file_item.size)
        self.assertEqual('image/foobarbaz', file_item.content_type)
        self.assertEqual(etag, file_item.etag)
        self.assertIn('test', file_item.metadata)

        # check for consistency between object and container listing
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_name:
                break
        else:
            self.fail('Failed to find file %r in listing' % file_name)
        self.assertEqual(1024, f_dict['bytes'])
        self.assertEqual('image/foobarbaz', f_dict['content_type'])
        self.assertLess(put_last_modified, f_dict['last_modified'])
        self.assertEqual(etag, f_dict['hash'])


class TestFileUTF8(Base2, TestFile):
    pass


class TestFileComparisonEnv(BaseEnv):
    @classmethod
    def setUp(cls):
        super(TestFileComparisonEnv, cls).setUp()
        cls.container = cls.account.container(Utils.create_name())

        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_count = 20
        cls.file_size = 128
        cls.files = list()
        for x in range(cls.file_count):
            file_item = cls.container.file(Utils.create_name())
            file_item.write_random(cls.file_size)
            cls.files.append(file_item)

        cls.time_old_f1 = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                        time.gmtime(time.time() - 86400))
        cls.time_old_f2 = time.strftime("%A, %d-%b-%y %H:%M:%S GMT",
                                        time.gmtime(time.time() - 86400))
        cls.time_old_f3 = time.strftime("%a %b %d %H:%M:%S %Y",
                                        time.gmtime(time.time() - 86400))
        cls.time_new = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                     time.gmtime(time.time() + 86400))


class TestFileComparison(Base):
    env = TestFileComparisonEnv

    def testIfMatch(self):
        for file_item in self.env.files:
            hdrs = {'If-Match': file_item.md5}
            self.assertTrue(file_item.read(hdrs=hdrs))

            hdrs = {'If-Match': 'bogus'}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(412)
            self.assert_header('etag', file_item.md5)

    def testIfMatchMultipleEtags(self):
        for file_item in self.env.files:
            hdrs = {'If-Match': '"bogus1", "%s", "bogus2"' % file_item.md5}
            self.assertTrue(file_item.read(hdrs=hdrs))

            hdrs = {'If-Match': '"bogus1", "bogus2", "bogus3"'}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(412)
            self.assert_header('etag', file_item.md5)

    def testIfNoneMatch(self):
        for file_item in self.env.files:
            hdrs = {'If-None-Match': 'bogus'}
            self.assertTrue(file_item.read(hdrs=hdrs))

            hdrs = {'If-None-Match': file_item.md5}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(304)
            self.assert_header('etag', file_item.md5)
            self.assert_header('accept-ranges', 'bytes')

    def testIfNoneMatchMultipleEtags(self):
        for file_item in self.env.files:
            hdrs = {'If-None-Match': '"bogus1", "bogus2", "bogus3"'}
            self.assertTrue(file_item.read(hdrs=hdrs))

            hdrs = {'If-None-Match':
                    '"bogus1", "bogus2", "%s"' % file_item.md5}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(304)
            self.assert_header('etag', file_item.md5)
            self.assert_header('accept-ranges', 'bytes')

    def testIfModifiedSince(self):
        for file_item in self.env.files:
            hdrs = {'If-Modified-Since': self.env.time_old_f1}
            self.assertTrue(file_item.read(hdrs=hdrs))
            self.assertTrue(file_item.info(hdrs=hdrs))

            hdrs = {'If-Modified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(304)
            self.assert_header('etag', file_item.md5)
            self.assert_header('accept-ranges', 'bytes')
            self.assertRaises(ResponseError, file_item.info, hdrs=hdrs)
            self.assert_status(304)
            self.assert_header('etag', file_item.md5)
            self.assert_header('accept-ranges', 'bytes')

    def testIfUnmodifiedSince(self):
        for file_item in self.env.files:
            hdrs = {'If-Unmodified-Since': self.env.time_new}
            self.assertTrue(file_item.read(hdrs=hdrs))
            self.assertTrue(file_item.info(hdrs=hdrs))

            hdrs = {'If-Unmodified-Since': self.env.time_old_f2}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(412)
            self.assert_header('etag', file_item.md5)
            self.assertRaises(ResponseError, file_item.info, hdrs=hdrs)
            self.assert_status(412)
            self.assert_header('etag', file_item.md5)

    def testIfMatchAndUnmodified(self):
        for file_item in self.env.files:
            hdrs = {'If-Match': file_item.md5,
                    'If-Unmodified-Since': self.env.time_new}
            self.assertTrue(file_item.read(hdrs=hdrs))

            hdrs = {'If-Match': 'bogus',
                    'If-Unmodified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(412)
            self.assert_header('etag', file_item.md5)

            hdrs = {'If-Match': file_item.md5,
                    'If-Unmodified-Since': self.env.time_old_f3}
            self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
            self.assert_status(412)
            self.assert_header('etag', file_item.md5)

    def testLastModified(self):
        file_name = Utils.create_name()
        content_type = Utils.create_name()

        file_item = self.env.container.file(file_name)
        file_item.content_type = content_type
        resp = file_item.write_random_return_resp(self.env.file_size)
        put_last_modified = resp.getheader('last-modified')
        etag = file_item.md5

        file_item = self.env.container.file(file_name)
        info = file_item.info()
        self.assertIn('last_modified', info)
        last_modified = info['last_modified']
        self.assertEqual(put_last_modified, info['last_modified'])

        hdrs = {'If-Modified-Since': last_modified}
        self.assertRaises(ResponseError, file_item.read, hdrs=hdrs)
        self.assert_status(304)
        self.assert_header('etag', etag)
        self.assert_header('accept-ranges', 'bytes')

        hdrs = {'If-Unmodified-Since': last_modified}
        self.assertTrue(file_item.read(hdrs=hdrs))


class TestFileComparisonUTF8(Base2, TestFileComparison):
    pass


class TestServiceToken(unittest2.TestCase):

    def setUp(self):
        if tf.skip_service_tokens:
            raise SkipTest

        if tf.in_process:
            tf.skip_if_no_xattrs()

        self.SET_TO_USERS_TOKEN = 1
        self.SET_TO_SERVICE_TOKEN = 2

        # keystoneauth and tempauth differ in allowing PUT account
        # Even if keystoneauth allows it, the proxy-server uses
        # allow_account_management to decide if accounts can be created
        self.put_account_expect = is_client_error
        if tf.swift_test_auth_version != '1':
            if cluster_info.get('swift').get('allow_account_management'):
                self.put_account_expect = is_success

    def _scenario_generator(self):
        paths = ((None, None), ('c', None), ('c', 'o'))
        for path in paths:
            for method in ('PUT', 'POST', 'HEAD', 'GET', 'OPTIONS'):
                yield method, path[0], path[1]
        for path in reversed(paths):
            yield 'DELETE', path[0], path[1]

    def _assert_is_authed_response(self, method, container, object, resp):
        resp.read()
        expect = is_success
        if method == 'DELETE' and not container:
            expect = is_client_error
        if method == 'PUT' and not container:
            expect = self.put_account_expect
        self.assertTrue(expect(resp.status), 'Unexpected %s for %s %s %s'
                        % (resp.status, method, container, object))

    def _assert_not_authed_response(self, method, container, object, resp):
        resp.read()
        expect = is_client_error
        if method == 'OPTIONS':
            expect = is_success
        self.assertTrue(expect(resp.status), 'Unexpected %s for %s %s %s'
                        % (resp.status, method, container, object))

    def prepare_request(self, method, use_service_account=False,
                        container=None, obj=None, body=None, headers=None,
                        x_auth_token=None,
                        x_service_token=None, dbg=False):
        """
        Setup for making the request

        When retry() calls the do_request() function, it calls it the
        test user's token, the parsed path, a connection and (optionally)
        a token from the test service user. We save options here so that
        do_request() can make the appropriate request.

        :param method: The operation (e.g. 'HEAD')
        :param use_service_account: Optional. Set True to change the path to
               be the service account
        :param container: Optional. Adds a container name to the path
        :param obj: Optional. Adds an object name to the path
        :param body: Optional. Adds a body (string) in the request
        :param headers: Optional. Adds additional headers.
        :param x_auth_token: Optional. Default is SET_TO_USERS_TOKEN. One of:
                   SET_TO_USERS_TOKEN     Put the test user's token in
                                          X-Auth-Token
                   SET_TO_SERVICE_TOKEN   Put the service token in X-Auth-Token
        :param x_service_token: Optional. Default is to not set X-Service-Token
                   to any value. If specified, is one of following:
                   SET_TO_USERS_TOKEN     Put the test user's token in
                                          X-Service-Token
                   SET_TO_SERVICE_TOKEN   Put the service token in
                                          X-Service-Token
        :param dbg: Optional. Set true to check request arguments
        """
        self.method = method
        self.use_service_account = use_service_account
        self.container = container
        self.obj = obj
        self.body = body
        self.headers = headers
        if x_auth_token:
            self.x_auth_token = x_auth_token
        else:
            self.x_auth_token = self.SET_TO_USERS_TOKEN
        self.x_service_token = x_service_token
        self.dbg = dbg

    def do_request(self, url, token, parsed, conn, service_token=''):
            if self.use_service_account:
                path = self._service_account(parsed.path)
            else:
                path = parsed.path
            if self.container:
                path += '/%s' % self.container
            if self.obj:
                path += '/%s' % self.obj
            headers = {}
            if self.body:
                headers.update({'Content-Length': len(self.body)})
            if self.x_auth_token == self.SET_TO_USERS_TOKEN:
                headers.update({'X-Auth-Token': token})
            elif self.x_auth_token == self.SET_TO_SERVICE_TOKEN:
                headers.update({'X-Auth-Token': service_token})
            if self.x_service_token == self.SET_TO_USERS_TOKEN:
                headers.update({'X-Service-Token': token})
            elif self.x_service_token == self.SET_TO_SERVICE_TOKEN:
                headers.update({'X-Service-Token': service_token})
            if self.dbg:
                print('DEBUG: conn.request: method:%s path:%s'
                      ' body:%s headers:%s' % (self.method, path, self.body,
                                               headers))
            conn.request(self.method, path, self.body, headers=headers)
            return check_response(conn)

    def _service_account(self, path):
        parts = path.split('/', 3)
        account = parts[2]
        try:
            project_id = account[account.index('_') + 1:]
        except ValueError:
            project_id = account
        parts[2] = '%s%s' % (tf.swift_test_service_prefix, project_id)
        return '/'.join(parts)

    def test_user_access_own_auth_account(self):
        # This covers ground tested elsewhere (tests a user doing HEAD
        # on own account). However, if this fails, none of the remaining
        # tests will work
        self.prepare_request('HEAD')
        resp = retry(self.do_request)
        resp.read()
        self.assertIn(resp.status, (200, 204))

    def test_user_cannot_access_service_account(self):
        for method, container, obj in self._scenario_generator():
            self.prepare_request(method, use_service_account=True,
                                 container=container, obj=obj)
            resp = retry(self.do_request)
            self._assert_not_authed_response(method, container, obj, resp)

    def test_service_user_denied_with_x_auth_token(self):
        for method, container, obj in self._scenario_generator():
            self.prepare_request(method, use_service_account=True,
                                 container=container, obj=obj,
                                 x_auth_token=self.SET_TO_SERVICE_TOKEN)
            resp = retry(self.do_request, service_user=5)
            self._assert_not_authed_response(method, container, obj, resp)

    def test_service_user_denied_with_x_service_token(self):
        for method, container, obj in self._scenario_generator():
            self.prepare_request(method, use_service_account=True,
                                 container=container, obj=obj,
                                 x_auth_token=self.SET_TO_SERVICE_TOKEN,
                                 x_service_token=self.SET_TO_SERVICE_TOKEN)
            resp = retry(self.do_request, service_user=5)
            self._assert_not_authed_response(method, container, obj, resp)

    def test_user_plus_service_can_access_service_account(self):
        for method, container, obj in self._scenario_generator():
            self.prepare_request(method, use_service_account=True,
                                 container=container, obj=obj,
                                 x_auth_token=self.SET_TO_USERS_TOKEN,
                                 x_service_token=self.SET_TO_SERVICE_TOKEN)
            resp = retry(self.do_request, service_user=5)
            self._assert_is_authed_response(method, container, obj, resp)


if __name__ == '__main__':
    unittest2.main()
