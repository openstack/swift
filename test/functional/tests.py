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
import hmac
import itertools
import json
import locale
import random
import six
from six.moves import urllib
import time
import unittest2
import uuid
from copy import deepcopy
import eventlet
from unittest2 import SkipTest
from swift.common.http import is_success, is_client_error

from test.functional import normalized_urls, load_constraint, cluster_info
from test.functional import check_response, retry, requires_acls
import test.functional as tf
from test.functional.swift_test_client import Account, Connection, File, \
    ResponseError


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
        return ''.join([random.choice(utf8_chars)
                        for x in range(length)]).encode('utf-8')

    create_name = create_ascii_name


class Base(unittest2.TestCase):
    def setUp(self):
        cls = type(self)
        if not cls.set_up:
            cls.env.setUp()
            cls.set_up = True

    def assert_body(self, body):
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
    def setUp(self):
        Utils.create_name = Utils.create_utf8_name
        super(Base2, self).setUp()

    def tearDown(self):
        Utils.create_name = Utils.create_ascii_name


class TestAccountEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

        cls.containers = []
        for i in range(10):
            cont = cls.account.container(Utils.create_name())
            if not cont.create():
                raise ResponseError(cls.conn.response)

            cls.containers.append(cont)


class TestAccountDev(Base):
    env = TestAccountEnv
    set_up = False


class TestAccountDevUTF8(Base2, TestAccountDev):
    set_up = False


class TestAccount(Base):
    env = TestAccountEnv
    set_up = False

    def testNoAuthToken(self):
        self.assertRaises(ResponseError, self.env.account.info,
                          cfg={'no_auth_token': True})
        self.assert_status([401, 412])

        self.assertRaises(ResponseError, self.env.account.containers,
                          cfg={'no_auth_token': True})
        self.assert_status([401, 412])

    def testInvalidUTF8Path(self):
        invalid_utf8 = Utils.create_utf8_name()[::-1]
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
        was_url = self.env.account.conn.storage_url
        if (normalized_urls):
            self.env.account.conn.storage_url = '/'
        else:
            self.env.account.conn.storage_url = "/%s" % was_url
        self.env.account.conn.make_request('GET')
        try:
            self.assert_status(404)
        finally:
            self.env.account.conn.storage_url = was_url

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

    def testInvalidAuthToken(self):
        hdrs = {'X-Auth-Token': 'bogus_auth_token'}
        self.assertRaises(ResponseError, self.env.account.info, hdrs=hdrs)
        self.assert_status(401)

    def testLastContainerMarker(self):
        for format_type in [None, 'json', 'xml']:
            containers = self.env.account.containers({'format': format_type})
            self.assertEqual(len(containers), len(self.env.containers))
            self.assert_status(200)

            containers = self.env.account.containers(
                parms={'format': format_type, 'marker': containers[-1]})
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
            self.assertEqual(sorted(containers, cmp=locale.strcoll),
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
    set_up = False


class TestAccountNoContainersEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()


class TestAccountNoContainers(Base):
    env = TestAccountNoContainersEnv
    set_up = False

    def testGetRequest(self):
        for format_type in [None, 'json', 'xml']:
            self.assertFalse(self.env.account.containers(
                parms={'format': format_type}))

            if format_type is None:
                self.assert_status(204)
            else:
                self.assert_status(200)


class TestAccountNoContainersUTF8(Base2, TestAccountNoContainers):
    set_up = False


class TestAccountSortingEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

        postfix = Utils.create_name()
        cls.cont_items = ('a1', 'a2', 'A3', 'b1', 'B2', 'a10', 'b10', 'zz')
        cls.cont_items = ['%s%s' % (x, postfix) for x in cls.cont_items]

        for container in cls.cont_items:
            c = cls.account.container(container)
            if not c.create():
                raise ResponseError(cls.conn.response)


class TestAccountSorting(Base):
    env = TestAccountSortingEnv
    set_up = False

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


class TestContainerEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

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
    set_up = False


class TestContainerDevUTF8(Base2, TestContainerDev):
    set_up = False


class TestContainer(Base):
    env = TestContainerEnv
    set_up = False

    def testContainerNameLimit(self):
        limit = load_constraint('max_container_name_length')

        for l in (limit - 100, limit - 10, limit - 1, limit,
                  limit + 1, limit + 10, limit + 100):
            cont = self.env.account.container('a' * l)
            if l <= limit:
                self.assertTrue(cont.create())
                self.assert_status(201)
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
        invalid_utf8 = valid_utf8[::-1]
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
        if Utils.create_name == Utils.create_utf8_name:
            cont_name = list(six.text_type(Utils.create_name(), 'utf-8'))
        else:
            cont_name = list(Utils.create_name())

        cont_name[random.randint(2, len(cont_name) - 2)] = '/'
        cont_name = ''.join(cont_name)

        if Utils.create_name == Utils.create_utf8_name:
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
            files = self.env.container.files({'format': format_type})
            self.assertEqual(len(files), len(self.env.files))
            self.assert_status(200)

            files = self.env.container.files(
                parms={'format': format_type, 'marker': files[-1]})
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
            self.assertEqual(sorted(files, cmp=locale.strcoll), files)

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

    def testTooLongName(self):
        cont = self.env.account.container('x' * 257)
        self.assertFalse(cont.create(),
                         'created container with name %s' % (cont.name))
        self.assert_status(400)

    def testContainerExistenceCachingProblem(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, cont.files)
        self.assertTrue(cont.create())
        cont.files()

        cont = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, cont.files)
        self.assertTrue(cont.create())
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
            obj.write("aaaaa", hdrs={'Content-Type': 'text/plain'}))
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
    set_up = False


class TestContainerSortingEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

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
    set_up = False

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


class TestContainerPathsEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

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
    set_up = False

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
            self.assertEqual(sorted(file_list, cmp=locale.strcoll), files)
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


class TestFileEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        # creating another account and connection
        # for account to account copy tests
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.conn2.authenticate()

        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()
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
        cls.account2.update_metadata()


class TestFileDev(Base):
    env = TestFileEnv
    set_up = False


class TestFileDevUTF8(Base2, TestFileDev):
    set_up = False


class TestFile(Base):
    env = TestFileEnv
    set_up = False

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
            self.assertFalse(file_item.copy(
                '%s%s' % (prefix, self.env.container),
                Utils.create_name()))
            self.assert_status(404)

            self.assertFalse(file_item.copy('%s%s' % (prefix, dest_cont),
                             Utils.create_name()))
            self.assert_status(404)

            # invalid source object
            file_item = self.env.container.file(Utils.create_name())
            self.assertFalse(file_item.copy(
                '%s%s' % (prefix, self.env.container),
                Utils.create_name()))
            self.assert_status(404)

            self.assertFalse(file_item.copy('%s%s' % (prefix, dest_cont),
                                            Utils.create_name()))
            self.assert_status(404)

            # invalid destination container
            file_item = self.env.container.file(source_filename)
            self.assertFalse(file_item.copy(
                '%s%s' % (prefix, Utils.create_name()),
                Utils.create_name()))

    def testCopyAccount404s(self):
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
                self.assertFalse(file_item.copy_account(
                    acct,
                    '%s%s' % (prefix, self.env.container),
                    Utils.create_name()))
                # there is no such source container but user has
                # permissions to do a GET (done internally via COPY) for
                # objects in his own account.
                self.assert_status(404)

                self.assertFalse(file_item.copy_account(
                    acct,
                    '%s%s' % (prefix, cont),
                    Utils.create_name()))
                self.assert_status(404)

                # invalid source object
                file_item = self.env.container.file(Utils.create_name())
                self.assertFalse(file_item.copy_account(
                    acct,
                    '%s%s' % (prefix, self.env.container),
                    Utils.create_name()))
                # there is no such source container but user has
                # permissions to do a GET (done internally via COPY) for
                # objects in his own account.
                self.assert_status(404)

                self.assertFalse(file_item.copy_account(
                    acct,
                    '%s%s' % (prefix, cont),
                    Utils.create_name()))
                self.assert_status(404)

                # invalid destination container
                file_item = self.env.container.file(source_filename)
                self.assertFalse(file_item.copy_account(
                    acct,
                    '%s%s' % (prefix, Utils.create_name()),
                    Utils.create_name()))
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
        self.assertFalse(file_item.copy(Utils.create_name(),
                         Utils.create_name(),
                         cfg={'no_destination': True}))
        self.assert_status(412)

    def testCopyDestinationSlashProblems(self):
        source_filename = Utils.create_name()
        file_item = self.env.container.file(source_filename)
        file_item.write_random()

        # no slash
        self.assertFalse(file_item.copy(Utils.create_name(),
                         Utils.create_name(),
                         cfg={'destination': Utils.create_name()}))
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
            # looks like cached responses leak "not found"
            # to un-authorized users, not going to fix it now, but...
            self.assert_status([403, 404])

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

    def testNameLimit(self):
        limit = load_constraint('max_object_name_length')

        for l in (1, 10, limit / 2, limit - 1, limit, limit + 1, limit * 2):
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

            j = size_limit / (i * 2)

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
                self.assert_status((201, 202))
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
            file_item.write('', cfg={'no_content_type': True})

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
        range_size = file_length / 10
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
            else:
                self.assertEqual(file_item.read(hdrs=hdrs), data[-i:])
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
        range_size = file_length / 10
        subrange_size = range_size / 10
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
        self.assertRaises(ResponseError, file_item.write, 'testing',
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
            self.assert_status((201, 202))

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

    def testTooLongName(self):
        file_item = self.env.container.file('x' * 1025)
        self.assertRaises(ResponseError, file_item.write)
        self.assert_status(400)

    def testZeroByteFile(self):
        file_item = self.env.container.file(Utils.create_name())

        self.assertTrue(file_item.write(''))
        self.assertIn(file_item.name, self.env.container.files())
        self.assertEqual(file_item.read(), '')

    def testEtagResponse(self):
        file_item = self.env.container.file(Utils.create_name())

        data = six.StringIO(file_item.write_random(512))
        etag = File.compute_md5sum(data)

        headers = dict(self.env.conn.response.getheaders())
        self.assertIn('etag', headers.keys())

        header_etag = headers['etag'].strip('"')
        self.assertEqual(etag, header_etag)

    def testChunkedPut(self):
        if (tf.web_front_end == 'apache2'):
            raise SkipTest("Chunked PUT can only be tested with apache2 web"
                           " front end")

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
        self.assertEqual(etag, f_dict['hash'])


class TestFileUTF8(Base2, TestFile):
    set_up = False


class TestDloEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        config2 = tf.config.copy()
        config2['username'] = tf.config['username3']
        config2['password'] = tf.config['password3']
        cls.conn2 = Connection(config2)
        cls.conn2.authenticate()

        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        cls.container2 = cls.account.container(Utils.create_name())

        for cont in (cls.container, cls.container2):
            if not cont.create():
                raise ResponseError(cls.conn.response)

        # avoid getting a prefix that stops halfway through an encoded
        # character
        prefix = Utils.create_name().decode("utf-8")[:10].encode("utf-8")
        cls.segment_prefix = prefix

        for letter in ('a', 'b', 'c', 'd', 'e'):
            file_item = cls.container.file("%s/seg_lower%s" % (prefix, letter))
            file_item.write(letter * 10)

            file_item = cls.container.file("%s/seg_upper%s" % (prefix, letter))
            file_item.write(letter.upper() * 10)

        for letter in ('f', 'g', 'h', 'i', 'j'):
            file_item = cls.container2.file("%s/seg_lower%s" %
                                            (prefix, letter))
            file_item.write(letter * 10)

        man1 = cls.container.file("man1")
        man1.write('man1-contents',
                   hdrs={"X-Object-Manifest": "%s/%s/seg_lower" %
                         (cls.container.name, prefix)})

        man2 = cls.container.file("man2")
        man2.write('man2-contents',
                   hdrs={"X-Object-Manifest": "%s/%s/seg_upper" %
                         (cls.container.name, prefix)})

        manall = cls.container.file("manall")
        manall.write('manall-contents',
                     hdrs={"X-Object-Manifest": "%s/%s/seg" %
                           (cls.container.name, prefix)})

        mancont2 = cls.container.file("mancont2")
        mancont2.write(
            'mancont2-contents',
            hdrs={"X-Object-Manifest": "%s/%s/seg_lower" %
                                       (cls.container2.name, prefix)})


class TestDlo(Base):
    env = TestDloEnv
    set_up = False

    def test_get_manifest(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")

        file_item = self.env.container.file('man2')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE")

        file_item = self.env.container.file('manall')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            ("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee" +
             "AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDDEEEEEEEEEE"))

    def test_get_manifest_document_itself(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(file_contents, "man1-contents")
        self.assertEqual(file_item.info()['x_object_manifest'],
                         "%s/%s/seg_lower" %
                         (self.env.container.name, self.env.segment_prefix))

    def test_get_range(self):
        file_item = self.env.container.file('man1')
        file_contents = file_item.read(size=25, offset=8)
        self.assertEqual(file_contents, "aabbbbbbbbbbccccccccccddd")

        file_contents = file_item.read(size=1, offset=47)
        self.assertEqual(file_contents, "e")

    def test_get_range_out_of_range(self):
        file_item = self.env.container.file('man1')

        self.assertRaises(ResponseError, file_item.read, size=7, offset=50)
        self.assert_status(416)

    def test_copy(self):
        # Adding a new segment, copying the manifest, and then deleting the
        # segment proves that the new object is really the concatenated
        # segments and not just a manifest.
        f_segment = self.env.container.file("%s/seg_lowerf" %
                                            (self.env.segment_prefix))
        f_segment.write('ffffffffff')
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy(self.env.container.name, "copied-man1")
        finally:
            # try not to leave this around for other tests to stumble over
            f_segment.delete()

        file_item = self.env.container.file('copied-man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
        # The copied object must not have X-Object-Manifest
        self.assertNotIn("x_object_manifest", file_item.info())

    def test_copy_account(self):
        # dlo use same account and same container only
        acct = self.env.conn.account_name
        # Adding a new segment, copying the manifest, and then deleting the
        # segment proves that the new object is really the concatenated
        # segments and not just a manifest.
        f_segment = self.env.container.file("%s/seg_lowerf" %
                                            (self.env.segment_prefix))
        f_segment.write('ffffffffff')
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy_account(acct,
                                   self.env.container.name,
                                   "copied-man1")
        finally:
            # try not to leave this around for other tests to stumble over
            f_segment.delete()

        file_item = self.env.container.file('copied-man1')
        file_contents = file_item.read()
        self.assertEqual(
            file_contents,
            "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
        # The copied object must not have X-Object-Manifest
        self.assertNotIn("x_object_manifest", file_item.info())

    def test_copy_manifest(self):
        # Copying the manifest with multipart-manifest=get query string
        # should result in another manifest
        try:
            man1_item = self.env.container.file('man1')
            man1_item.copy(self.env.container.name, "copied-man1",
                           parms={'multipart-manifest': 'get'})

            copied = self.env.container.file("copied-man1")
            copied_contents = copied.read(parms={'multipart-manifest': 'get'})
            self.assertEqual(copied_contents, "man1-contents")

            copied_contents = copied.read()
            self.assertEqual(
                copied_contents,
                "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee")
            self.assertEqual(man1_item.info()['x_object_manifest'],
                             copied.info()['x_object_manifest'])
        finally:
            # try not to leave this around for other tests to stumble over
            self.env.container.file("copied-man1").delete()

    def test_dlo_if_match_get(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.read(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_dlo_if_none_match_get(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.read(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_dlo_if_match_head(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.info(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_dlo_if_none_match_head(self):
        manifest = self.env.container.file("man1")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.info(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_dlo_referer_on_segment_container(self):
        # First the account2 (test3) should fail
        headers = {'X-Auth-Token': self.env.conn2.storage_token,
                   'Referer': 'http://blah.example.com'}
        dlo_file = self.env.container.file("mancont2")
        self.assertRaises(ResponseError, dlo_file.read,
                          hdrs=headers)
        self.assert_status(403)

        # Now set the referer on the dlo container only
        referer_metadata = {'X-Container-Read': '.r:*.example.com,.rlistings'}
        self.env.container.update_metadata(referer_metadata)

        self.assertRaises(ResponseError, dlo_file.read,
                          hdrs=headers)
        self.assert_status(403)

        # Finally set the referer on the segment container
        self.env.container2.update_metadata(referer_metadata)

        contents = dlo_file.read(hdrs=headers)
        self.assertEqual(
            contents,
            "ffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj")

    def test_dlo_post_with_manifest_header(self):
        # verify that performing a POST to a DLO manifest
        # preserves the fact that it is a manifest file.
        # verify that the x-object-manifest header may be updated.

        # create a new manifest for this test to avoid test coupling.
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']
        file_item = self.env.container.file(Utils.create_name())
        file_item.write('manifest-contents', hdrs={"X-Object-Manifest": x_o_m})

        # sanity checks
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # POST a modified x-object-manifest value
        new_x_o_m = x_o_m.rstrip('lower') + 'upper'
        file_item.post({'x-object-meta-foo': 'bar',
                        'x-object-manifest': new_x_o_m})

        # verify that x-object-manifest was updated
        file_item.info()
        resp_headers = file_item.conn.response.getheaders()
        self.assertIn(('x-object-manifest', new_x_o_m), resp_headers)
        self.assertIn(('x-object-meta-foo', 'bar'), resp_headers)

        # verify that manifest content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)

        # verify that updated manifest points to new content
        expected_contents = ''.join([(c * 10) for c in 'ABCDE'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # Now revert the manifest to point to original segments, including a
        # multipart-manifest=get param just to check that has no effect
        file_item.post({'x-object-manifest': x_o_m},
                       parms={'multipart-manifest': 'get'})

        # verify that x-object-manifest was reverted
        info = file_item.info()
        self.assertIn('x_object_manifest', info)
        self.assertEqual(x_o_m, info['x_object_manifest'])

        # verify that manifest content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)

        # verify that updated manifest points new content
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

    def test_dlo_post_without_manifest_header(self):
        # verify that a POST to a DLO manifest object with no
        # x-object-manifest header will cause the existing x-object-manifest
        # header to be lost

        # create a new manifest for this test to avoid test coupling.
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']
        file_item = self.env.container.file(Utils.create_name())
        file_item.write('manifest-contents', hdrs={"X-Object-Manifest": x_o_m})

        # sanity checks
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)

        # POST with no x-object-manifest header
        file_item.post({})

        # verify that existing x-object-manifest was removed
        info = file_item.info()
        self.assertNotIn('x_object_manifest', info)

        # verify that object content was not changed
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('manifest-contents', manifest_contents)

        # verify that object is no longer a manifest
        contents = file_item.read(parms={})
        self.assertEqual('manifest-contents', contents)

    def test_dlo_post_with_manifest_regular_object(self):
        # verify that performing a POST to a regular object
        # with a manifest header will create a DLO.

        # Put a regular object
        file_item = self.env.container.file(Utils.create_name())
        file_item.write('file contents', hdrs={})

        # sanity checks
        file_contents = file_item.read(parms={})
        self.assertEqual('file contents', file_contents)

        # get the path associated with man1
        x_o_m = self.env.container.file('man1').info()['x_object_manifest']

        # POST a x-object-manifest value to the regular object
        file_item.post({'x-object-manifest': x_o_m})

        # verify that the file is now a manifest
        manifest_contents = file_item.read(parms={'multipart-manifest': 'get'})
        self.assertEqual('file contents', manifest_contents)
        expected_contents = ''.join([(c * 10) for c in 'abcde'])
        contents = file_item.read(parms={})
        self.assertEqual(expected_contents, contents)
        file_item.info()
        resp_headers = file_item.conn.response.getheaders()
        self.assertIn(('x-object-manifest', x_o_m), resp_headers)


class TestDloUTF8(Base2, TestDlo):
    set_up = False


class TestFileComparisonEnv(object):
    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

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
    set_up = False

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
    set_up = False


class TestSloEnv(object):
    slo_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def create_segments(cls, container):
        seg_info = {}
        for letter, size in (('a', 1024 * 1024),
                             ('b', 1024 * 1024),
                             ('c', 1024 * 1024),
                             ('d', 1024 * 1024),
                             ('e', 1)):
            seg_name = "seg_%s" % letter
            file_item = container.file(seg_name)
            file_item.write(letter * size)
            seg_info[seg_name] = {
                'size_bytes': size,
                'etag': file_item.md5,
                'path': '/%s/%s' % (container.name, seg_name)}
        return seg_info

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.conn2.authenticate()
        cls.account2 = cls.conn2.get_account()
        cls.account2.delete_containers()
        config3 = tf.config.copy()
        config3['username'] = tf.config['username3']
        config3['password'] = tf.config['password3']
        cls.conn3 = Connection(config3)
        cls.conn3.authenticate()

        if cls.slo_enabled is None:
            cls.slo_enabled = 'slo' in cluster_info
            if not cls.slo_enabled:
                return

        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        cls.container2 = cls.account.container(Utils.create_name())

        for cont in (cls.container, cls.container2):
            if not cont.create():
                raise ResponseError(cls.conn.response)

        cls.seg_info = seg_info = cls.create_segments(cls.container)

        file_item = cls.container.file("manifest-abcde")
        file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]),
            parms={'multipart-manifest': 'put'})

        # Put the same manifest in the container2
        file_item = cls.container2.file("manifest-abcde")
        file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]),
            parms={'multipart-manifest': 'put'})

        file_item = cls.container.file('manifest-cd')
        cd_json = json.dumps([seg_info['seg_c'], seg_info['seg_d']])
        file_item.write(cd_json, parms={'multipart-manifest': 'put'})
        cd_etag = hashlib.md5(seg_info['seg_c']['etag'] +
                              seg_info['seg_d']['etag']).hexdigest()

        file_item = cls.container.file("manifest-bcd-submanifest")
        file_item.write(
            json.dumps([seg_info['seg_b'],
                        {'etag': cd_etag,
                         'size_bytes': (seg_info['seg_c']['size_bytes'] +
                                        seg_info['seg_d']['size_bytes']),
                         'path': '/%s/%s' % (cls.container.name,
                                             'manifest-cd')}]),
            parms={'multipart-manifest': 'put'})
        bcd_submanifest_etag = hashlib.md5(
            seg_info['seg_b']['etag'] + cd_etag).hexdigest()

        file_item = cls.container.file("manifest-abcde-submanifest")
        file_item.write(
            json.dumps([
                seg_info['seg_a'],
                {'etag': bcd_submanifest_etag,
                 'size_bytes': (seg_info['seg_b']['size_bytes'] +
                                seg_info['seg_c']['size_bytes'] +
                                seg_info['seg_d']['size_bytes']),
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-bcd-submanifest')},
                seg_info['seg_e']]),
            parms={'multipart-manifest': 'put'})
        abcde_submanifest_etag = hashlib.md5(
            seg_info['seg_a']['etag'] + bcd_submanifest_etag +
            seg_info['seg_e']['etag']).hexdigest()
        abcde_submanifest_size = (seg_info['seg_a']['size_bytes'] +
                                  seg_info['seg_b']['size_bytes'] +
                                  seg_info['seg_c']['size_bytes'] +
                                  seg_info['seg_d']['size_bytes'] +
                                  seg_info['seg_e']['size_bytes'])

        file_item = cls.container.file("ranged-manifest")
        file_item.write(
            json.dumps([
                {'etag': abcde_submanifest_etag,
                 'size_bytes': abcde_submanifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-abcde-submanifest'),
                 'range': '-1048578'},  # 'c' + ('d' * 2**20) + 'e'
                {'etag': abcde_submanifest_etag,
                 'size_bytes': abcde_submanifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-abcde-submanifest'),
                 'range': '524288-1572863'},  # 'a' * 2**19 + 'b' * 2**19
                {'etag': abcde_submanifest_etag,
                 'size_bytes': abcde_submanifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'manifest-abcde-submanifest'),
                 'range': '3145727-3145728'}]),  # 'cd'
            parms={'multipart-manifest': 'put'})
        ranged_manifest_etag = hashlib.md5(
            abcde_submanifest_etag + ':3145727-4194304;' +
            abcde_submanifest_etag + ':524288-1572863;' +
            abcde_submanifest_etag + ':3145727-3145728;').hexdigest()
        ranged_manifest_size = 2 * 1024 * 1024 + 4

        file_item = cls.container.file("ranged-submanifest")
        file_item.write(
            json.dumps([
                seg_info['seg_c'],
                {'etag': ranged_manifest_etag,
                 'size_bytes': ranged_manifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'ranged-manifest')},
                {'etag': ranged_manifest_etag,
                 'size_bytes': ranged_manifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'ranged-manifest'),
                 'range': '524289-1572865'},
                {'etag': ranged_manifest_etag,
                 'size_bytes': ranged_manifest_size,
                 'path': '/%s/%s' % (cls.container.name,
                                     'ranged-manifest'),
                 'range': '-3'}]),
            parms={'multipart-manifest': 'put'})

        file_item = cls.container.file("manifest-db")
        file_item.write(
            json.dumps([
                {'path': seg_info['seg_d']['path'], 'etag': None,
                 'size_bytes': None},
                {'path': seg_info['seg_b']['path'], 'etag': None,
                 'size_bytes': None},
            ]), parms={'multipart-manifest': 'put'})

        file_item = cls.container.file("ranged-manifest-repeated-segment")
        file_item.write(
            json.dumps([
                {'path': seg_info['seg_a']['path'], 'etag': None,
                 'size_bytes': None, 'range': '-1048578'},
                {'path': seg_info['seg_a']['path'], 'etag': None,
                 'size_bytes': None},
                {'path': seg_info['seg_b']['path'], 'etag': None,
                 'size_bytes': None, 'range': '-1048578'},
            ]), parms={'multipart-manifest': 'put'})


class TestSlo(Base):
    env = TestSloEnv
    set_up = False

    def setUp(self):
        super(TestSlo, self).setUp()
        if self.env.slo_enabled is False:
            raise SkipTest("SLO not enabled")
        elif self.env.slo_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected slo_enabled to be True/False, got %r" %
                (self.env.slo_enabled,))

    def test_slo_get_simple_manifest(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read()
        self.assertEqual(4 * 1024 * 1024 + 1, len(file_contents))
        self.assertEqual('a', file_contents[0])
        self.assertEqual('a', file_contents[1024 * 1024 - 1])
        self.assertEqual('b', file_contents[1024 * 1024])
        self.assertEqual('d', file_contents[-2])
        self.assertEqual('e', file_contents[-1])

    def test_slo_container_listing(self):
        # the listing object size should equal the sum of the size of the
        # segments, not the size of the manifest body
        file_item = self.env.container.file(Utils.create_name())
        file_item.write(
            json.dumps([self.env.seg_info['seg_a']]),
            parms={'multipart-manifest': 'put'})
        # The container listing has the etag of the actual manifest object
        # contents which we get using multipart-manifest=get. Arguably this
        # should be the etag that we get when NOT using multipart-manifest=get,
        # to be consistent with size and content-type. But here we at least
        # verify that it remains consistent when the object is updated with a
        # POST.
        file_item.initialize(parms={'multipart-manifest': 'get'})
        expected_etag = file_item.etag

        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual('application/octet-stream',
                                 f_dict['content_type'])
                self.assertEqual(expected_etag, f_dict['hash'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

        # now POST updated content-type file
        file_item.content_type = 'image/jpeg'
        file_item.sync_metadata({'X-Object-Meta-Test': 'blah'})
        file_item.initialize()
        self.assertEqual('image/jpeg', file_item.content_type)  # sanity

        # verify that the container listing is consistent with the file
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual(file_item.content_type,
                                 f_dict['content_type'])
                self.assertEqual(expected_etag, f_dict['hash'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

        # now POST with no change to content-type
        file_item.sync_metadata({'X-Object-Meta-Test': 'blah'},
                                cfg={'no_content_type': True})
        file_item.initialize()
        self.assertEqual('image/jpeg', file_item.content_type)  # sanity

        # verify that the container listing is consistent with the file
        listing = self.env.container.files(parms={'format': 'json'})
        for f_dict in listing:
            if f_dict['name'] == file_item.name:
                self.assertEqual(1024 * 1024, f_dict['bytes'])
                self.assertEqual(file_item.content_type,
                                 f_dict['content_type'])
                self.assertEqual(expected_etag, f_dict['hash'])
                break
        else:
            self.fail('Failed to find manifest file in container listing')

    def test_slo_get_nested_manifest(self):
        file_item = self.env.container.file('manifest-abcde-submanifest')
        file_contents = file_item.read()
        self.assertEqual(4 * 1024 * 1024 + 1, len(file_contents))
        self.assertEqual('a', file_contents[0])
        self.assertEqual('a', file_contents[1024 * 1024 - 1])
        self.assertEqual('b', file_contents[1024 * 1024])
        self.assertEqual('d', file_contents[-2])
        self.assertEqual('e', file_contents[-1])

    def test_slo_get_ranged_manifest(self):
        file_item = self.env.container.file('ranged-manifest')
        grouped_file_contents = [
            (char, sum(1 for _char in grp))
            for char, grp in itertools.groupby(file_item.read())]
        self.assertEqual([
            ('c', 1),
            ('d', 1024 * 1024),
            ('e', 1),
            ('a', 512 * 1024),
            ('b', 512 * 1024),
            ('c', 1),
            ('d', 1)], grouped_file_contents)

    def test_slo_get_ranged_manifest_repeated_segment(self):
        file_item = self.env.container.file('ranged-manifest-repeated-segment')
        grouped_file_contents = [
            (char, sum(1 for _char in grp))
            for char, grp in itertools.groupby(file_item.read())]
        self.assertEqual(
            [('a', 2097152), ('b', 1048576)],
            grouped_file_contents)

    def test_slo_get_ranged_submanifest(self):
        file_item = self.env.container.file('ranged-submanifest')
        grouped_file_contents = [
            (char, sum(1 for _char in grp))
            for char, grp in itertools.groupby(file_item.read())]
        self.assertEqual([
            ('c', 1024 * 1024 + 1),
            ('d', 1024 * 1024),
            ('e', 1),
            ('a', 512 * 1024),
            ('b', 512 * 1024),
            ('c', 1),
            ('d', 512 * 1024 + 1),
            ('e', 1),
            ('a', 512 * 1024),
            ('b', 1),
            ('c', 1),
            ('d', 1)], grouped_file_contents)

    def test_slo_ranged_get(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read(size=1024 * 1024 + 2,
                                       offset=1024 * 1024 - 1)
        self.assertEqual('a', file_contents[0])
        self.assertEqual('b', file_contents[1])
        self.assertEqual('b', file_contents[-2])
        self.assertEqual('c', file_contents[-1])

    def test_slo_multi_ranged_get(self):
        file_item = self.env.container.file('manifest-abcde')
        file_contents = file_item.read(
            hdrs={"Range": "bytes=1048571-1048580,2097147-2097156"})

        # See testMultiRangeGets for explanation
        parser = email.parser.FeedParser()
        parser.feed("Content-Type: %s\r\n\r\n" % file_item.content_type)
        parser.feed(file_contents)

        root_message = parser.close()
        self.assertTrue(root_message.is_multipart())  # sanity check

        byteranges = root_message.get_payload()
        self.assertEqual(len(byteranges), 2)

        self.assertEqual(byteranges[0]['Content-Type'],
                         "application/octet-stream")
        self.assertEqual(
            byteranges[0]['Content-Range'], "bytes 1048571-1048580/4194305")
        self.assertEqual(byteranges[0].get_payload(), "aaaaabbbbb")

        self.assertEqual(byteranges[1]['Content-Type'],
                         "application/octet-stream")
        self.assertEqual(
            byteranges[1]['Content-Range'], "bytes 2097147-2097156/4194305")
        self.assertEqual(byteranges[1].get_payload(), "bbbbbccccc")

    def test_slo_ranged_submanifest(self):
        file_item = self.env.container.file('manifest-abcde-submanifest')
        file_contents = file_item.read(size=1024 * 1024 + 2,
                                       offset=1024 * 1024 * 2 - 1)
        self.assertEqual('b', file_contents[0])
        self.assertEqual('c', file_contents[1])
        self.assertEqual('c', file_contents[-2])
        self.assertEqual('d', file_contents[-1])

    def test_slo_etag_is_hash_of_etags(self):
        expected_hash = hashlib.md5()
        expected_hash.update(hashlib.md5('a' * 1024 * 1024).hexdigest())
        expected_hash.update(hashlib.md5('b' * 1024 * 1024).hexdigest())
        expected_hash.update(hashlib.md5('c' * 1024 * 1024).hexdigest())
        expected_hash.update(hashlib.md5('d' * 1024 * 1024).hexdigest())
        expected_hash.update(hashlib.md5('e').hexdigest())
        expected_etag = expected_hash.hexdigest()

        file_item = self.env.container.file('manifest-abcde')
        self.assertEqual(expected_etag, file_item.info()['etag'])

    def test_slo_etag_is_hash_of_etags_submanifests(self):

        def hd(x):
            return hashlib.md5(x).hexdigest()

        expected_etag = hd(hd('a' * 1024 * 1024) +
                           hd(hd('b' * 1024 * 1024) +
                              hd(hd('c' * 1024 * 1024) +
                                 hd('d' * 1024 * 1024))) +
                           hd('e'))

        file_item = self.env.container.file('manifest-abcde-submanifest')
        self.assertEqual(expected_etag, file_item.info()['etag'])

    def test_slo_etag_mismatch(self):
        file_item = self.env.container.file("manifest-a-bad-etag")
        try:
            file_item.write(
                json.dumps([{
                    'size_bytes': 1024 * 1024,
                    'etag': 'not it',
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a')}]),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_size_mismatch(self):
        file_item = self.env.container.file("manifest-a-bad-size")
        try:
            file_item.write(
                json.dumps([{
                    'size_bytes': 1024 * 1024 - 1,
                    'etag': hashlib.md5('a' * 1024 * 1024).hexdigest(),
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a')}]),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_unspecified_etag(self):
        file_item = self.env.container.file("manifest-a-unspecified-etag")
        file_item.write(
            json.dumps([{
                'size_bytes': 1024 * 1024,
                'etag': None,
                'path': '/%s/%s' % (self.env.container.name, 'seg_a')}]),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_unspecified_size(self):
        file_item = self.env.container.file("manifest-a-unspecified-size")
        file_item.write(
            json.dumps([{
                'size_bytes': None,
                'etag': hashlib.md5('a' * 1024 * 1024).hexdigest(),
                'path': '/%s/%s' % (self.env.container.name, 'seg_a')}]),
            parms={'multipart-manifest': 'put'})
        self.assert_status(201)

    def test_slo_missing_etag(self):
        file_item = self.env.container.file("manifest-a-missing-etag")
        try:
            file_item.write(
                json.dumps([{
                    'size_bytes': 1024 * 1024,
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a')}]),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_missing_size(self):
        file_item = self.env.container.file("manifest-a-missing-size")
        try:
            file_item.write(
                json.dumps([{
                    'etag': hashlib.md5('a' * 1024 * 1024).hexdigest(),
                    'path': '/%s/%s' % (self.env.container.name, 'seg_a')}]),
                parms={'multipart-manifest': 'put'})
        except ResponseError as err:
            self.assertEqual(400, err.status)
        else:
            self.fail("Expected ResponseError but didn't get it")

    def test_slo_overwrite_segment_with_manifest(self):
        file_item = self.env.container.file("seg_b")
        with self.assertRaises(ResponseError) as catcher:
            file_item.write(
                json.dumps([
                    {'size_bytes': 1024 * 1024,
                     'etag': hashlib.md5('a' * 1024 * 1024).hexdigest(),
                     'path': '/%s/%s' % (self.env.container.name, 'seg_a')},
                    {'size_bytes': 1024 * 1024,
                     'etag': hashlib.md5('b' * 1024 * 1024).hexdigest(),
                     'path': '/%s/%s' % (self.env.container.name, 'seg_b')},
                    {'size_bytes': 1024 * 1024,
                     'etag': hashlib.md5('c' * 1024 * 1024).hexdigest(),
                     'path': '/%s/%s' % (self.env.container.name, 'seg_c')}]),
                parms={'multipart-manifest': 'put'})
        self.assertEqual(400, catcher.exception.status)

    def test_slo_copy(self):
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy(self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

    def test_slo_copy_account(self):
        acct = self.env.conn.account_name
        # same account copy
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy_account(acct, self.env.container.name, "copied-abcde")

        copied = self.env.container.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

        # copy to different account
        acct = self.env.conn2.account_name
        dest_cont = self.env.account2.container(Utils.create_name())
        self.assertTrue(dest_cont.create(hdrs={
            'X-Container-Write': self.env.conn.user_acl
        }))
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy_account(acct, dest_cont, "copied-abcde")

        copied = dest_cont.file("copied-abcde")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        self.assertEqual(4 * 1024 * 1024 + 1, len(copied_contents))

    def test_slo_copy_the_manifest(self):
        source = self.env.container.file("manifest-abcde")
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        source.initialize()
        self.assertEqual('application/octet-stream', source.content_type)
        source.initialize(parms={'multipart-manifest': 'get'})
        source_hash = hashlib.md5()
        source_hash.update(source_contents)
        self.assertEqual(source_hash.hexdigest(), source.etag)

        self.assertTrue(source.copy(self.env.container.name,
                                    "copied-abcde-manifest-only",
                                    parms={'multipart-manifest': 'get'}))

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")
        self.assertEqual(source_json, copied_json)
        copied.initialize()
        self.assertEqual('application/octet-stream', copied.content_type)
        copied.initialize(parms={'multipart-manifest': 'get'})
        copied_hash = hashlib.md5()
        copied_hash.update(copied_contents)
        self.assertEqual(copied_hash.hexdigest(), copied.etag)

        # verify the listing metadata
        listing = self.env.container.files(parms={'format': 'json'})
        names = {}
        for f_dict in listing:
            if f_dict['name'] in ('manifest-abcde',
                                  'copied-abcde-manifest-only'):
                names[f_dict['name']] = f_dict

        self.assertIn('manifest-abcde', names)
        actual = names['manifest-abcde']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        self.assertEqual(source.etag, actual['hash'])

        self.assertIn('copied-abcde-manifest-only', names)
        actual = names['copied-abcde-manifest-only']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        self.assertEqual(copied.etag, actual['hash'])

    def test_slo_copy_the_manifest_updating_metadata(self):
        source = self.env.container.file("manifest-abcde")
        source.content_type = 'application/octet-stream'
        source.sync_metadata({'test': 'original'})
        source_contents = source.read(parms={'multipart-manifest': 'get'})
        source_json = json.loads(source_contents)
        source.initialize()
        self.assertEqual('application/octet-stream', source.content_type)
        source.initialize(parms={'multipart-manifest': 'get'})
        source_hash = hashlib.md5()
        source_hash.update(source_contents)
        self.assertEqual(source_hash.hexdigest(), source.etag)
        self.assertEqual(source.metadata['test'], 'original')

        self.assertTrue(
            source.copy(self.env.container.name, "copied-abcde-manifest-only",
                        parms={'multipart-manifest': 'get'},
                        hdrs={'Content-Type': 'image/jpeg',
                              'X-Object-Meta-Test': 'updated'}))

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            copied_json = json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")
        self.assertEqual(source_json, copied_json)
        copied.initialize()
        self.assertEqual('image/jpeg', copied.content_type)
        copied.initialize(parms={'multipart-manifest': 'get'})
        copied_hash = hashlib.md5()
        copied_hash.update(copied_contents)
        self.assertEqual(copied_hash.hexdigest(), copied.etag)
        self.assertEqual(copied.metadata['test'], 'updated')

        # verify the listing metadata
        listing = self.env.container.files(parms={'format': 'json'})
        names = {}
        for f_dict in listing:
            if f_dict['name'] in ('manifest-abcde',
                                  'copied-abcde-manifest-only'):
                names[f_dict['name']] = f_dict

        self.assertIn('manifest-abcde', names)
        actual = names['manifest-abcde']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('application/octet-stream', actual['content_type'])
        # the container listing should have the etag of the manifest contents
        self.assertEqual(source.etag, actual['hash'])

        self.assertIn('copied-abcde-manifest-only', names)
        actual = names['copied-abcde-manifest-only']
        self.assertEqual(4 * 1024 * 1024 + 1, actual['bytes'])
        self.assertEqual('image/jpeg', actual['content_type'])
        self.assertEqual(copied.etag, actual['hash'])

    def test_slo_copy_the_manifest_account(self):
        acct = self.env.conn.account_name
        # same account
        file_item = self.env.container.file("manifest-abcde")
        file_item.copy_account(acct,
                               self.env.container.name,
                               "copied-abcde-manifest-only",
                               parms={'multipart-manifest': 'get'})

        copied = self.env.container.file("copied-abcde-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")

        # different account
        acct = self.env.conn2.account_name
        dest_cont = self.env.account2.container(Utils.create_name())
        self.assertTrue(dest_cont.create(hdrs={
            'X-Container-Write': self.env.conn.user_acl
        }))

        # manifest copy will fail because there is no read access to segments
        # in destination account
        file_item.copy_account(
            acct, dest_cont, "copied-abcde-manifest-only",
            parms={'multipart-manifest': 'get'})
        self.assertEqual(400, file_item.conn.response.status)
        resp_body = file_item.conn.response.read()
        self.assertEqual(5, resp_body.count('403 Forbidden'),
                         'Unexpected response body %r' % resp_body)

        # create segments container in account2 with read access for account1
        segs_container = self.env.account2.container(self.env.container.name)
        self.assertTrue(segs_container.create(hdrs={
            'X-Container-Read': self.env.conn.user_acl
        }))

        # manifest copy will still fail because there are no segments in
        # destination account
        file_item.copy_account(
            acct, dest_cont, "copied-abcde-manifest-only",
            parms={'multipart-manifest': 'get'})
        self.assertEqual(400, file_item.conn.response.status)
        resp_body = file_item.conn.response.read()
        self.assertEqual(5, resp_body.count('404 Not Found'),
                         'Unexpected response body %r' % resp_body)

        # create segments in account2 container with same name as in account1,
        # manifest copy now succeeds
        self.env.create_segments(segs_container)

        self.assertTrue(file_item.copy_account(
            acct, dest_cont, "copied-abcde-manifest-only",
            parms={'multipart-manifest': 'get'}))

        copied = dest_cont.file("copied-abcde-manifest-only")
        copied_contents = copied.read(parms={'multipart-manifest': 'get'})
        try:
            json.loads(copied_contents)
        except ValueError:
            self.fail("COPY didn't copy the manifest (invalid json on GET)")

    def _make_manifest(self):
        file_item = self.env.container.file("manifest-post")
        seg_info = self.env.seg_info
        file_item.write(
            json.dumps([seg_info['seg_a'], seg_info['seg_b'],
                        seg_info['seg_c'], seg_info['seg_d'],
                        seg_info['seg_e']]),
            parms={'multipart-manifest': 'put'})
        return file_item

    def test_slo_post_the_manifest_metadata_update(self):
        file_item = self._make_manifest()
        # sanity check, check the object is an SLO manifest
        file_item.info()
        file_item.header_fields([('slo', 'x-static-large-object')])

        # POST a user metadata (i.e. x-object-meta-post)
        file_item.sync_metadata({'post': 'update'})

        updated = self.env.container.file("manifest-post")
        updated.info()
        updated.header_fields([('user-meta', 'x-object-meta-post')])  # sanity
        updated.header_fields([('slo', 'x-static-large-object')])
        updated_contents = updated.read(parms={'multipart-manifest': 'get'})
        try:
            json.loads(updated_contents)
        except ValueError:
            self.fail("Unexpected content on GET, expected a json body")

    def test_slo_post_the_manifest_metadata_update_with_qs(self):
        # multipart-manifest query should be ignored on post
        for verb in ('put', 'get', 'delete'):
            file_item = self._make_manifest()
            # sanity check, check the object is an SLO manifest
            file_item.info()
            file_item.header_fields([('slo', 'x-static-large-object')])
            # POST a user metadata (i.e. x-object-meta-post)
            file_item.sync_metadata(metadata={'post': 'update'},
                                    parms={'multipart-manifest': verb})
            updated = self.env.container.file("manifest-post")
            updated.info()
            updated.header_fields(
                [('user-meta', 'x-object-meta-post')])  # sanity
            updated.header_fields([('slo', 'x-static-large-object')])
            updated_contents = updated.read(
                parms={'multipart-manifest': 'get'})
            try:
                json.loads(updated_contents)
            except ValueError:
                self.fail(
                    "Unexpected content on GET, expected a json body")

    def test_slo_get_the_manifest(self):
        manifest = self.env.container.file("manifest-abcde")
        got_body = manifest.read(parms={'multipart-manifest': 'get'})

        self.assertEqual('application/json; charset=utf-8',
                         manifest.content_type)
        try:
            json.loads(got_body)
        except ValueError:
            self.fail("GET with multipart-manifest=get got invalid json")

    def test_slo_get_the_manifest_with_details_from_server(self):
        manifest = self.env.container.file("manifest-db")
        got_body = manifest.read(parms={'multipart-manifest': 'get'})

        self.assertEqual('application/json; charset=utf-8',
                         manifest.content_type)
        try:
            value = json.loads(got_body)
        except ValueError:
            self.fail("GET with multipart-manifest=get got invalid json")

        self.assertEqual(len(value), 2)
        self.assertEqual(value[0]['bytes'], 1024 * 1024)
        self.assertEqual(value[0]['hash'],
                         hashlib.md5('d' * 1024 * 1024).hexdigest())
        self.assertEqual(value[0]['name'],
                         '/%s/seg_d' % self.env.container.name.decode("utf-8"))

        self.assertEqual(value[1]['bytes'], 1024 * 1024)
        self.assertEqual(value[1]['hash'],
                         hashlib.md5('b' * 1024 * 1024).hexdigest())
        self.assertEqual(value[1]['name'],
                         '/%s/seg_b' % self.env.container.name.decode("utf-8"))

    def test_slo_get_raw_the_manifest_with_details_from_server(self):
        manifest = self.env.container.file("manifest-db")
        got_body = manifest.read(parms={'multipart-manifest': 'get',
                                        'format': 'raw'})

        # raw format should have the actual manifest object content-type
        self.assertEqual('application/octet-stream', manifest.content_type)
        try:
            value = json.loads(got_body)
        except ValueError:
            msg = "GET with multipart-manifest=get&format=raw got invalid json"
            self.fail(msg)

        self.assertEqual(
            set(value[0].keys()), set(('size_bytes', 'etag', 'path')))
        self.assertEqual(len(value), 2)
        self.assertEqual(value[0]['size_bytes'], 1024 * 1024)
        self.assertEqual(value[0]['etag'],
                         hashlib.md5('d' * 1024 * 1024).hexdigest())
        self.assertEqual(value[0]['path'],
                         '/%s/seg_d' % self.env.container.name.decode("utf-8"))
        self.assertEqual(value[1]['size_bytes'], 1024 * 1024)
        self.assertEqual(value[1]['etag'],
                         hashlib.md5('b' * 1024 * 1024).hexdigest())
        self.assertEqual(value[1]['path'],
                         '/%s/seg_b' % self.env.container.name.decode("utf-8"))

        file_item = self.env.container.file("manifest-from-get-raw")
        file_item.write(got_body, parms={'multipart-manifest': 'put'})

        file_contents = file_item.read()
        self.assertEqual(2 * 1024 * 1024, len(file_contents))

    def test_slo_head_the_manifest(self):
        manifest = self.env.container.file("manifest-abcde")
        got_info = manifest.info(parms={'multipart-manifest': 'get'})

        self.assertEqual('application/json; charset=utf-8',
                         got_info['content_type'])

    def test_slo_if_match_get(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.read(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_slo_if_none_match_put(self):
        file_item = self.env.container.file("manifest-if-none-match")
        manifest = json.dumps([{
            'size_bytes': 1024 * 1024,
            'etag': None,
            'path': '/%s/%s' % (self.env.container.name, 'seg_a')}])

        self.assertRaises(ResponseError, file_item.write, manifest,
                          parms={'multipart-manifest': 'put'},
                          hdrs={'If-None-Match': '"not-star"'})
        self.assert_status(400)

        file_item.write(manifest, parms={'multipart-manifest': 'put'},
                        hdrs={'If-None-Match': '*'})
        self.assert_status(201)

        self.assertRaises(ResponseError, file_item.write, manifest,
                          parms={'multipart-manifest': 'put'},
                          hdrs={'If-None-Match': '*'})
        self.assert_status(412)

    def test_slo_if_none_match_get(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.read,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.read(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_slo_if_match_head(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-Match': 'not-%s' % etag})
        self.assert_status(412)

        manifest.info(hdrs={'If-Match': etag})
        self.assert_status(200)

    def test_slo_if_none_match_head(self):
        manifest = self.env.container.file("manifest-abcde")
        etag = manifest.info()['etag']

        self.assertRaises(ResponseError, manifest.info,
                          hdrs={'If-None-Match': etag})
        self.assert_status(304)

        manifest.info(hdrs={'If-None-Match': "not-%s" % etag})
        self.assert_status(200)

    def test_slo_referer_on_segment_container(self):
        # First the account2 (test3) should fail
        headers = {'X-Auth-Token': self.env.conn3.storage_token,
                   'Referer': 'http://blah.example.com'}
        slo_file = self.env.container2.file('manifest-abcde')
        self.assertRaises(ResponseError, slo_file.read,
                          hdrs=headers)
        self.assert_status(403)

        # Now set the referer on the slo container only
        referer_metadata = {'X-Container-Read': '.r:*.example.com,.rlistings'}
        self.env.container2.update_metadata(referer_metadata)

        self.assertRaises(ResponseError, slo_file.read,
                          hdrs=headers)
        self.assert_status(409)

        # Finally set the referer on the segment container
        self.env.container.update_metadata(referer_metadata)
        contents = slo_file.read(hdrs=headers)
        self.assertEqual(4 * 1024 * 1024 + 1, len(contents))
        self.assertEqual('a', contents[0])
        self.assertEqual('a', contents[1024 * 1024 - 1])
        self.assertEqual('b', contents[1024 * 1024])
        self.assertEqual('d', contents[-2])
        self.assertEqual('e', contents[-1])


class TestSloUTF8(Base2, TestSlo):
    set_up = False


class TestObjectVersioningEnv(object):
    versioning_enabled = None  # tri-state: None initially, then True/False
    location_header_key = 'X-Versions-Location'

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.storage_url, cls.storage_token = cls.conn.authenticate()

        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))

        # Second connection for ACL tests
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.conn2.authenticate()

        # avoid getting a prefix that stops halfway through an encoded
        # character
        prefix = Utils.create_name().decode("utf-8")[:10].encode("utf-8")

        cls.versions_container = cls.account.container(prefix + "-versions")
        if not cls.versions_container.create():
            raise ResponseError(cls.conn.response)

        cls.container = cls.account.container(prefix + "-objs")
        container_headers = {
            cls.location_header_key: cls.versions_container.name}
        if not cls.container.create(hdrs=container_headers):
            if cls.conn.response.status == 412:
                cls.versioning_enabled = False
                return
            raise ResponseError(cls.conn.response)

        container_info = cls.container.info()
        # if versioning is off, then cls.location_header_key won't persist
        cls.versioning_enabled = 'versions' in container_info

        # setup another account to test ACLs
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.storage_url2, cls.storage_token2 = cls.conn2.authenticate()
        cls.account2 = cls.conn2.get_account()
        cls.account2.delete_containers()

        # setup another account with no access to anything to test ACLs
        config3 = deepcopy(tf.config)
        config3['account'] = tf.config['account']
        config3['username'] = tf.config['username3']
        config3['password'] = tf.config['password3']
        cls.conn3 = Connection(config3)
        cls.storage_url3, cls.storage_token3 = cls.conn3.authenticate()
        cls.account3 = cls.conn3.get_account()

    @classmethod
    def tearDown(cls):
        cls.account.delete_containers()
        cls.account2.delete_containers()


class TestCrossPolicyObjectVersioningEnv(object):
    # tri-state: None initially, then True/False
    versioning_enabled = None
    multiple_policies_enabled = None
    policies = None

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.multiple_policies_enabled is None:
            try:
                cls.policies = tf.FunctionalStoragePolicyCollection.from_info()
            except AssertionError:
                pass

        if cls.policies and len(cls.policies) > 1:
            cls.multiple_policies_enabled = True
        else:
            cls.multiple_policies_enabled = False
            cls.versioning_enabled = True
            # We don't actually know the state of versioning, but without
            # multiple policies the tests should be skipped anyway. Claiming
            # versioning support lets us report the right reason for skipping.
            return

        policy = cls.policies.select()
        version_policy = cls.policies.exclude(name=policy['name']).select()

        cls.account = Account(cls.conn, tf.config.get('account',
                                                      tf.config['username']))

        # Second connection for ACL tests
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.conn2.authenticate()

        # avoid getting a prefix that stops halfway through an encoded
        # character
        prefix = Utils.create_name().decode("utf-8")[:10].encode("utf-8")

        cls.versions_container = cls.account.container(prefix + "-versions")
        if not cls.versions_container.create(
                {'X-Storage-Policy': policy['name']}):
            raise ResponseError(cls.conn.response)

        cls.container = cls.account.container(prefix + "-objs")
        if not cls.container.create(
                hdrs={'X-Versions-Location': cls.versions_container.name,
                      'X-Storage-Policy': version_policy['name']}):
            if cls.conn.response.status == 412:
                cls.versioning_enabled = False
                return
            raise ResponseError(cls.conn.response)

        container_info = cls.container.info()
        # if versioning is off, then X-Versions-Location won't persist
        cls.versioning_enabled = 'versions' in container_info

        # setup another account to test ACLs
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.storage_url2, cls.storage_token2 = cls.conn2.authenticate()
        cls.account2 = cls.conn2.get_account()
        cls.account2.delete_containers()

        # setup another account with no access to anything to test ACLs
        config3 = deepcopy(tf.config)
        config3['account'] = tf.config['account']
        config3['username'] = tf.config['username3']
        config3['password'] = tf.config['password3']
        cls.conn3 = Connection(config3)
        cls.storage_url3, cls.storage_token3 = cls.conn3.authenticate()
        cls.account3 = cls.conn3.get_account()

    @classmethod
    def tearDown(cls):
        cls.account.delete_containers()
        cls.account2.delete_containers()


class TestObjectVersioningHistoryModeEnv(TestObjectVersioningEnv):
    location_header_key = 'X-History-Location'


class TestObjectVersioning(Base):
    env = TestObjectVersioningEnv
    set_up = False

    def setUp(self):
        super(TestObjectVersioning, self).setUp()
        if self.env.versioning_enabled is False:
            raise SkipTest("Object versioning not enabled")
        elif self.env.versioning_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected versioning_enabled to be True/False, got %r" %
                (self.env.versioning_enabled,))

    def _tear_down_files(self):
        try:
            # only delete files and not containers
            # as they were configured in self.env
            self.env.versions_container.delete_files()
            self.env.container.delete_files()
        except ResponseError:
            pass

    def tearDown(self):
        super(TestObjectVersioning, self).tearDown()
        self._tear_down_files()

    def test_clear_version_option(self):
        # sanity
        self.assertEqual(self.env.container.info()['versions'],
                         self.env.versions_container.name)
        self.env.container.update_metadata(
            hdrs={self.env.location_header_key: ''})
        self.assertIsNone(self.env.container.info().get('versions'))

        # set location back to the way it was
        self.env.container.update_metadata(
            hdrs={self.env.location_header_key:
                  self.env.versions_container.name})
        self.assertEqual(self.env.container.info()['versions'],
                         self.env.versions_container.name)

    def test_overwriting(self):
        container = self.env.container
        versions_container = self.env.versions_container
        cont_info = container.info()
        self.assertEqual(cont_info['versions'], versions_container.name)

        obj_name = Utils.create_name()

        versioned_obj = container.file(obj_name)
        put_headers = {'Content-Type': 'text/jibberish01',
                       'Content-Encoding': 'gzip',
                       'Content-Disposition': 'attachment; filename=myfile'}
        versioned_obj.write("aaaaa", hdrs=put_headers)
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])

        # the allowed headers are configurable in object server, so we cannot
        # assert that content-encoding or content-disposition get *copied* to
        # the object version unless they were set on the original PUT, so
        # populate expected_headers by making a HEAD on the original object
        resp_headers = dict(versioned_obj.conn.response.getheaders())
        expected_headers = {}
        for k, v in put_headers.items():
            if k.lower() in resp_headers:
                expected_headers[k] = v

        self.assertEqual(0, versions_container.info()['object_count'])
        versioned_obj.write("bbbbb", hdrs={'Content-Type': 'text/jibberish02',
                            'X-Object-Meta-Foo': 'Bar'})
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish02')
        self.assertEqual(versioned_obj.metadata['foo'], 'Bar')

        # the old version got saved off
        self.assertEqual(1, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[0]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual("aaaaa", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish01')

        resp_headers = dict(prev_version.conn.response.getheaders())
        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        # make sure the new obj metadata did not leak to the prev. version
        self.assertNotIn('foo', prev_version.metadata)

        # check that POST does not create a new version
        versioned_obj.sync_metadata(metadata={'fu': 'baz'})
        self.assertEqual(1, versions_container.info()['object_count'])

        # if we overwrite it again, there are two versions
        versioned_obj.write("ccccc")
        self.assertEqual(2, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[1]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual("bbbbb", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish02')
        self.assertNotIn('foo', prev_version.metadata)
        self.assertIn('fu', prev_version.metadata)

        # as we delete things, the old contents return
        self.assertEqual("ccccc", versioned_obj.read())

        # test copy from a different container
        src_container = self.env.account.container(Utils.create_name())
        self.assertTrue(src_container.create())
        src_name = Utils.create_name()
        src_obj = src_container.file(src_name)
        src_obj.write("ddddd", hdrs={'Content-Type': 'text/jibberish04'})
        src_obj.copy(container.name, obj_name)

        self.assertEqual("ddddd", versioned_obj.read())
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish04')

        # make sure versions container has the previous version
        self.assertEqual(3, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[2]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual("ccccc", prev_version.read())

        # test delete
        versioned_obj.delete()
        self.assertEqual("ccccc", versioned_obj.read())
        versioned_obj.delete()
        self.assertEqual("bbbbb", versioned_obj.read())
        versioned_obj.delete()
        self.assertEqual("aaaaa", versioned_obj.read())
        self.assertEqual(0, versions_container.info()['object_count'])

        # verify that all the original object headers have been copied back
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])
        resp_headers = dict(versioned_obj.conn.response.getheaders())
        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        versioned_obj.delete()
        self.assertRaises(ResponseError, versioned_obj.read)

    def test_versioning_dlo(self):
        container = self.env.container
        versions_container = self.env.versions_container
        obj_name = Utils.create_name()

        for i in ('1', '2', '3'):
            time.sleep(.01)  # guarantee that the timestamp changes
            obj_name_seg = obj_name + '/' + i
            versioned_obj = container.file(obj_name_seg)
            versioned_obj.write(i)
            versioned_obj.write(i + i)

        self.assertEqual(3, versions_container.info()['object_count'])

        man_file = container.file(obj_name)
        man_file.write('', hdrs={"X-Object-Manifest": "%s/%s/" %
                       (self.env.container.name, obj_name)})

        # guarantee that the timestamp changes
        time.sleep(.01)

        # write manifest file again
        man_file.write('', hdrs={"X-Object-Manifest": "%s/%s/" %
                       (self.env.container.name, obj_name)})

        self.assertEqual(3, versions_container.info()['object_count'])
        self.assertEqual("112233", man_file.read())

    def test_versioning_container_acl(self):
        # create versions container and DO NOT give write access to account2
        versions_container = self.env.account.container(Utils.create_name())
        self.assertTrue(versions_container.create(hdrs={
            'X-Container-Write': ''
        }))

        # check account2 cannot write to versions container
        fail_obj_name = Utils.create_name()
        fail_obj = versions_container.file(fail_obj_name)
        self.assertRaises(ResponseError, fail_obj.write, "should fail",
                          cfg={'use_token': self.env.storage_token2})

        # create container and give write access to account2
        # don't set X-Versions-Location just yet
        container = self.env.account.container(Utils.create_name())
        self.assertTrue(container.create(hdrs={
            'X-Container-Write': self.env.conn2.user_acl}))

        # check account2 cannot set X-Versions-Location on container
        self.assertRaises(ResponseError, container.update_metadata, hdrs={
            self.env.location_header_key: versions_container},
            cfg={'use_token': self.env.storage_token2})

        # good! now let admin set the X-Versions-Location
        # p.s.: sticking a 'x-remove' header here to test precedence
        # of both headers. Setting the location should succeed.
        self.assertTrue(container.update_metadata(hdrs={
            'X-Remove-' + self.env.location_header_key[len('X-'):]:
            versions_container,
            self.env.location_header_key: versions_container}))

        # write object twice to container and check version
        obj_name = Utils.create_name()
        versioned_obj = container.file(obj_name)
        self.assertTrue(versioned_obj.write("never argue with the data",
                        cfg={'use_token': self.env.storage_token2}))
        self.assertEqual(versioned_obj.read(), "never argue with the data")

        self.assertTrue(
            versioned_obj.write("we don't have no beer, just tequila",
                                cfg={'use_token': self.env.storage_token2}))
        self.assertEqual(versioned_obj.read(),
                         "we don't have no beer, just tequila")
        self.assertEqual(1, versions_container.info()['object_count'])

        # read the original uploaded object
        for filename in versions_container.files():
            backup_file = versions_container.file(filename)
            break
        self.assertEqual(backup_file.read(), "never argue with the data")

        # user3 (some random user with no access to anything)
        # tries to read from versioned container
        self.assertRaises(ResponseError, backup_file.read,
                          cfg={'use_token': self.env.storage_token3})

        # user3 cannot write or delete from source container either
        number_of_versions = versions_container.info()['object_count']
        self.assertRaises(ResponseError, versioned_obj.write,
                          "some random user trying to write data",
                          cfg={'use_token': self.env.storage_token3})
        self.assertEqual(number_of_versions,
                         versions_container.info()['object_count'])
        self.assertRaises(ResponseError, versioned_obj.delete,
                          cfg={'use_token': self.env.storage_token3})
        self.assertEqual(number_of_versions,
                         versions_container.info()['object_count'])

        # user2 can't read or delete from versions-location
        self.assertRaises(ResponseError, backup_file.read,
                          cfg={'use_token': self.env.storage_token2})
        self.assertRaises(ResponseError, backup_file.delete,
                          cfg={'use_token': self.env.storage_token2})

        # but is able to delete from the source container
        # this could be a helpful scenario for dev ops that want to setup
        # just one container to hold object versions of multiple containers
        # and each one of those containers are owned by different users
        self.assertTrue(versioned_obj.delete(
                        cfg={'use_token': self.env.storage_token2}))

        # tear-down since we create these containers here
        # and not in self.env
        versions_container.delete_recursive()
        container.delete_recursive()

    def test_versioning_check_acl(self):
        container = self.env.container
        versions_container = self.env.versions_container
        versions_container.create(hdrs={'X-Container-Read': '.r:*,.rlistings'})

        obj_name = Utils.create_name()
        versioned_obj = container.file(obj_name)
        versioned_obj.write("aaaaa")
        self.assertEqual("aaaaa", versioned_obj.read())

        versioned_obj.write("bbbbb")
        self.assertEqual("bbbbb", versioned_obj.read())

        # Use token from second account and try to delete the object
        org_token = self.env.account.conn.storage_token
        self.env.account.conn.storage_token = self.env.conn2.storage_token
        try:
            with self.assertRaises(ResponseError) as cm:
                versioned_obj.delete()
            self.assertEqual(403, cm.exception.status)
        finally:
            self.env.account.conn.storage_token = org_token

        # Verify with token from first account
        self.assertEqual("bbbbb", versioned_obj.read())

        versioned_obj.delete()
        self.assertEqual("aaaaa", versioned_obj.read())


class TestObjectVersioningUTF8(Base2, TestObjectVersioning):
    set_up = False

    def tearDown(self):
        self._tear_down_files()
        super(TestObjectVersioningUTF8, self).tearDown()


class TestCrossPolicyObjectVersioning(TestObjectVersioning):
    env = TestCrossPolicyObjectVersioningEnv
    set_up = False

    def setUp(self):
        super(TestCrossPolicyObjectVersioning, self).setUp()
        if self.env.multiple_policies_enabled is False:
            raise SkipTest('Cross policy test requires multiple policies')
        elif self.env.multiple_policies_enabled is not True:
            # just some sanity checking
            raise Exception("Expected multiple_policies_enabled "
                            "to be True/False, got %r" % (
                                self.env.versioning_enabled,))


class TestObjectVersioningHistoryMode(TestObjectVersioning):
    env = TestObjectVersioningHistoryModeEnv
    set_up = False

    # those override tests includes assertions for delete versioned objects
    # behaviors different from default object versioning using
    # x-versions-location.

    # The difference from the parent is since below delete
    def test_overwriting(self):
        container = self.env.container
        versions_container = self.env.versions_container
        cont_info = container.info()
        self.assertEqual(cont_info['versions'], versions_container.name)
        expected_content_types = []
        obj_name = Utils.create_name()

        versioned_obj = container.file(obj_name)
        put_headers = {'Content-Type': 'text/jibberish01',
                       'Content-Encoding': 'gzip',
                       'Content-Disposition': 'attachment; filename=myfile'}
        versioned_obj.write("aaaaa", hdrs=put_headers)
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])
        expected_content_types.append('text/jibberish01')

        # the allowed headers are configurable in object server, so we cannot
        # assert that content-encoding or content-disposition get *copied* to
        # the object version unless they were set on the original PUT, so
        # populate expected_headers by making a HEAD on the original object
        resp_headers = dict(versioned_obj.conn.response.getheaders())
        expected_headers = {}
        for k, v in put_headers.items():
            if k.lower() in resp_headers:
                expected_headers[k] = v

        self.assertEqual(0, versions_container.info()['object_count'])
        versioned_obj.write("bbbbb", hdrs={'Content-Type': 'text/jibberish02',
                            'X-Object-Meta-Foo': 'Bar'})
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish02')
        expected_content_types.append('text/jibberish02')
        self.assertEqual(versioned_obj.metadata['foo'], 'Bar')

        # the old version got saved off
        self.assertEqual(1, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[0]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual("aaaaa", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish01')

        resp_headers = dict(prev_version.conn.response.getheaders())
        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        # make sure the new obj metadata did not leak to the prev. version
        self.assertNotIn('foo', prev_version.metadata)

        # check that POST does not create a new version
        versioned_obj.sync_metadata(metadata={'fu': 'baz'})
        self.assertEqual(1, versions_container.info()['object_count'])
        expected_content_types.append('text/jibberish02')

        # if we overwrite it again, there are two versions
        versioned_obj.write("ccccc")
        self.assertEqual(2, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[1]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual("bbbbb", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish02')
        self.assertNotIn('foo', prev_version.metadata)
        self.assertIn('fu', prev_version.metadata)

        # versioned_obj keeps the newest content
        self.assertEqual("ccccc", versioned_obj.read())

        # test copy from a different container
        src_container = self.env.account.container(Utils.create_name())
        self.assertTrue(src_container.create())
        src_name = Utils.create_name()
        src_obj = src_container.file(src_name)
        src_obj.write("ddddd", hdrs={'Content-Type': 'text/jibberish04'})
        src_obj.copy(container.name, obj_name)

        self.assertEqual("ddddd", versioned_obj.read())
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish04')
        expected_content_types.append('text/jibberish04')

        # make sure versions container has the previous version
        self.assertEqual(3, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[2]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual("ccccc", prev_version.read())

        # test delete
        # at first, delete will succeed with 204
        versioned_obj.delete()
        expected_content_types.append(
            'application/x-deleted;swift_versions_deleted=1')
        # after that, any time the delete doesn't restore the old version
        # and we will get 404 NotFound
        for x in range(3):
            with self.assertRaises(ResponseError) as cm:
                versioned_obj.delete()
            self.assertEqual(404, cm.exception.status)
            expected_content_types.append(
                'application/x-deleted;swift_versions_deleted=1')
        # finally, we have 4 versioned items and 4 delete markers total in
        # the versions container
        self.assertEqual(8, versions_container.info()['object_count'])
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        # update versioned_obj
        versioned_obj.write("eeee", hdrs={'Content-Type': 'text/thanksgiving',
                            'X-Object-Meta-Bar': 'foo'})
        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/thanksgiving', obj_info['content_type'])

        # we still have delete-marker there
        self.assertEqual(8, versions_container.info()['object_count'])

        # update versioned_obj
        versioned_obj.write("ffff", hdrs={'Content-Type': 'text/teriyaki',
                            'X-Object-Meta-Food': 'chickin'})
        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/teriyaki', obj_info['content_type'])

        # new obj will be inserted after delete-marker there
        self.assertEqual(9, versions_container.info()['object_count'])

        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.read()
        self.assertEqual(404, cm.exception.status)
        self.assertEqual(11, versions_container.info()['object_count'])

    # the difference from the parent is since below delete
    def test_versioning_check_acl(self):
        container = self.env.container
        versions_container = self.env.versions_container
        versions_container.create(hdrs={'X-Container-Read': '.r:*,.rlistings'})

        obj_name = Utils.create_name()
        versioned_obj = container.file(obj_name)
        versioned_obj.write("aaaaa")
        self.assertEqual("aaaaa", versioned_obj.read())

        versioned_obj.write("bbbbb")
        self.assertEqual("bbbbb", versioned_obj.read())

        # Use token from second account and try to delete the object
        org_token = self.env.account.conn.storage_token
        self.env.account.conn.storage_token = self.env.conn2.storage_token
        try:
            with self.assertRaises(ResponseError) as cm:
                versioned_obj.delete()
            self.assertEqual(403, cm.exception.status)
        finally:
            self.env.account.conn.storage_token = org_token

        # Verify with token from first account
        self.assertEqual("bbbbb", versioned_obj.read())

        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.read()
        self.assertEqual(404, cm.exception.status)

        # we have 3 objects in the versions_container, 'aaaaa', 'bbbbb'
        # and delete-marker with empty content
        self.assertEqual(3, versions_container.info()['object_count'])
        files = versions_container.files()
        for actual, expected in zip(files, ['aaaaa', 'bbbbb', '']):
            prev_version = versions_container.file(actual)
            self.assertEqual(expected, prev_version.read())


class TestSloWithVersioning(Base):

    def setUp(self):
        if 'slo' not in cluster_info:
            raise SkipTest("SLO not enabled")

        self.conn = Connection(tf.config)
        self.conn.authenticate()
        self.account = Account(
            self.conn, tf.config.get('account', tf.config['username']))
        self.account.delete_containers()

        # create a container with versioning
        self.versions_container = self.account.container(Utils.create_name())
        self.container = self.account.container(Utils.create_name())
        self.segments_container = self.account.container(Utils.create_name())
        if not self.container.create(
                hdrs={'X-Versions-Location': self.versions_container.name}):
            raise ResponseError(self.conn.response)
        if 'versions' not in self.container.info():
            raise SkipTest("Object versioning not enabled")

        for cont in (self.versions_container, self.segments_container):
            if not cont.create():
                raise ResponseError(self.conn.response)

        # create some segments
        self.seg_info = {}
        for letter, size in (('a', 1024 * 1024),
                             ('b', 1024 * 1024)):
            seg_name = letter
            file_item = self.segments_container.file(seg_name)
            file_item.write(letter * size)
            self.seg_info[seg_name] = {
                'size_bytes': size,
                'etag': file_item.md5,
                'path': '/%s/%s' % (self.segments_container.name, seg_name)}

    def _create_manifest(self, seg_name):
        # create a manifest in the versioning container
        file_item = self.container.file("my-slo-manifest")
        file_item.write(
            json.dumps([self.seg_info[seg_name]]),
            parms={'multipart-manifest': 'put'})
        return file_item

    def _assert_is_manifest(self, file_item, seg_name):
        manifest_body = file_item.read(parms={'multipart-manifest': 'get'})
        resp_headers = dict(file_item.conn.response.getheaders())
        self.assertIn('x-static-large-object', resp_headers)
        self.assertEqual('application/json; charset=utf-8',
                         file_item.content_type)
        try:
            manifest = json.loads(manifest_body)
        except ValueError:
            self.fail("GET with multipart-manifest=get got invalid json")

        self.assertEqual(1, len(manifest))
        key_map = {'etag': 'hash', 'size_bytes': 'bytes', 'path': 'name'}
        for k_client, k_slo in key_map.items():
            self.assertEqual(self.seg_info[seg_name][k_client],
                             manifest[0][k_slo])

    def _assert_is_object(self, file_item, seg_name):
        file_contents = file_item.read()
        self.assertEqual(1024 * 1024, len(file_contents))
        self.assertEqual(seg_name, file_contents[0])
        self.assertEqual(seg_name, file_contents[-1])

    def tearDown(self):
        # remove versioning to allow simple container delete
        self.container.update_metadata(hdrs={'X-Versions-Location': ''})
        self.account.delete_containers()

    def test_slo_manifest_version(self):
        file_item = self._create_manifest('a')
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, 'a')

        # upload new manifest
        file_item = self._create_manifest('b')
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'b')
        self._assert_is_object(file_item, 'b')

        versions_list = self.versions_container.files()
        self.assertEqual(1, len(versions_list))
        version_file = self.versions_container.file(versions_list[0])
        # check the version is still a manifest
        self._assert_is_manifest(version_file, 'a')
        self._assert_is_object(version_file, 'a')

        # delete the newest manifest
        file_item.delete()

        # expect the original manifest file to be restored
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, 'a')


class TestTempurlEnv(object):
    tempurl_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.tempurl_enabled is None:
            cls.tempurl_enabled = 'tempurl' in cluster_info
            if not cls.tempurl_enabled:
                return

        cls.tempurl_key = Utils.create_name()
        cls.tempurl_key2 = Utils.create_name()

        cls.account = Account(
            cls.conn, tf.config.get('account', tf.config['username']))
        cls.account.delete_containers()
        cls.account.update_metadata({
            'temp-url-key': cls.tempurl_key,
            'temp-url-key-2': cls.tempurl_key2
        })

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.obj = cls.container.file(Utils.create_name())
        cls.obj.write("obj contents")
        cls.other_obj = cls.container.file(Utils.create_name())
        cls.other_obj.write("other obj contents")


class TestTempurl(Base):
    env = TestTempurlEnv
    set_up = False

    def setUp(self):
        super(TestTempurl, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        self.obj_tempurl_parms = {'temp_url_sig': sig,
                                  'temp_url_expires': str(expires)}

    def tempurl_sig(self, method, expires, path, key):
        return hmac.new(
            key,
            '%s\n%s\n%s' % (method, expires, urllib.parse.unquote(path)),
            hashlib.sha1).hexdigest()

    def test_GET(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        # GET tempurls also allow HEAD requests
        self.assertTrue(self.env.obj.info(parms=self.obj_tempurl_parms,
                                          cfg={'no_auth_token': True}))

    def test_GET_with_key_2(self):
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key2)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = self.env.obj.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

    def test_GET_DLO_inside_container(self):
        seg1 = self.env.container.file(
            "get-dlo-inside-seg1" + Utils.create_name())
        seg2 = self.env.container.file(
            "get-dlo-inside-seg2" + Utils.create_name())
        seg1.write("one fish two fish ")
        seg2.write("red fish blue fish")

        manifest = self.env.container.file("manifest" + Utils.create_name())
        manifest.write(
            '',
            hdrs={"X-Object-Manifest": "%s/get-dlo-inside-seg" %
                  (self.env.container.name,)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = manifest.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, "one fish two fish red fish blue fish")

    def test_GET_DLO_outside_container(self):
        seg1 = self.env.container.file(
            "get-dlo-outside-seg1" + Utils.create_name())
        seg2 = self.env.container.file(
            "get-dlo-outside-seg2" + Utils.create_name())
        seg1.write("one fish two fish ")
        seg2.write("red fish blue fish")

        container2 = self.env.account.container(Utils.create_name())
        container2.create()

        manifest = container2.file("manifest" + Utils.create_name())
        manifest.write(
            '',
            hdrs={"X-Object-Manifest": "%s/get-dlo-outside-seg" %
                  (self.env.container.name,)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        # cross container tempurl works fine for account tempurl key
        contents = manifest.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, "one fish two fish red fish blue fish")
        self.assert_status([200])

    def test_PUT(self):
        new_obj = self.env.container.file(Utils.create_name())

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'PUT', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        put_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        new_obj.write('new obj contents',
                      parms=put_parms, cfg={'no_auth_token': True})
        self.assertEqual(new_obj.read(), "new obj contents")

        # PUT tempurls also allow HEAD requests
        self.assertTrue(new_obj.info(parms=put_parms,
                                     cfg={'no_auth_token': True}))

    def test_PUT_manifest_access(self):
        new_obj = self.env.container.file(Utils.create_name())

        # give out a signature which allows a PUT to new_obj
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'PUT', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        put_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        # try to create manifest pointing to some random container
        try:
            new_obj.write('', {
                'x-object-manifest': '%s/foo' % 'some_random_container'
            }, parms=put_parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

        # create some other container
        other_container = self.env.account.container(Utils.create_name())
        if not other_container.create():
            raise ResponseError(self.conn.response)

        # try to create manifest pointing to new container
        try:
            new_obj.write('', {
                'x-object-manifest': '%s/foo' % other_container
            }, parms=put_parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

        # try again using a tempurl POST to an already created object
        new_obj.write('', {}, parms=put_parms, cfg={'no_auth_token': True})
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'POST', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        post_parms = {'temp_url_sig': sig,
                      'temp_url_expires': str(expires)}
        try:
            new_obj.post({'x-object-manifest': '%s/foo' % other_container},
                         parms=post_parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

    def test_HEAD(self):
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'HEAD', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        head_parms = {'temp_url_sig': sig,
                      'temp_url_expires': str(expires)}

        self.assertTrue(self.env.obj.info(parms=head_parms,
                                          cfg={'no_auth_token': True}))
        # HEAD tempurls don't allow PUT or GET requests, despite the fact that
        # PUT and GET tempurls both allow HEAD requests
        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

        self.assertRaises(ResponseError, self.env.other_obj.write,
                          'new contents',
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_different_object(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_changing_sig(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_sig'][0] == 'a':
            parms['temp_url_sig'] = 'b' + parms['temp_url_sig'][1:]
        else:
            parms['temp_url_sig'] = 'a' + parms['temp_url_sig'][1:]

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    def test_changing_expires(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_expires'][-1] == '0':
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '1'
        else:
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '0'

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])


class TestTempurlUTF8(Base2, TestTempurl):
    set_up = False


class TestContainerTempurlEnv(object):
    tempurl_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.tempurl_enabled is None:
            cls.tempurl_enabled = 'tempurl' in cluster_info
            if not cls.tempurl_enabled:
                return

        cls.tempurl_key = Utils.create_name()
        cls.tempurl_key2 = Utils.create_name()

        cls.account = Account(
            cls.conn, tf.config.get('account', tf.config['username']))
        cls.account.delete_containers()

        # creating another account and connection
        # for ACL tests
        config2 = deepcopy(tf.config)
        config2['account'] = tf.config['account2']
        config2['username'] = tf.config['username2']
        config2['password'] = tf.config['password2']
        cls.conn2 = Connection(config2)
        cls.conn2.authenticate()
        cls.account2 = Account(
            cls.conn2, config2.get('account', config2['username']))
        cls.account2 = cls.conn2.get_account()

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create({
                'x-container-meta-temp-url-key': cls.tempurl_key,
                'x-container-meta-temp-url-key-2': cls.tempurl_key2,
                'x-container-read': cls.account2.name}):
            raise ResponseError(cls.conn.response)

        cls.obj = cls.container.file(Utils.create_name())
        cls.obj.write("obj contents")
        cls.other_obj = cls.container.file(Utils.create_name())
        cls.other_obj.write("other obj contents")


class TestContainerTempurl(Base):
    env = TestContainerTempurlEnv
    set_up = False

    def setUp(self):
        super(TestContainerTempurl, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        self.obj_tempurl_parms = {'temp_url_sig': sig,
                                  'temp_url_expires': str(expires)}

    def tempurl_sig(self, method, expires, path, key):
        return hmac.new(
            key,
            '%s\n%s\n%s' % (method, expires, urllib.parse.unquote(path)),
            hashlib.sha1).hexdigest()

    def test_GET(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        # GET tempurls also allow HEAD requests
        self.assertTrue(self.env.obj.info(parms=self.obj_tempurl_parms,
                                          cfg={'no_auth_token': True}))

    def test_GET_with_key_2(self):
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key2)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = self.env.obj.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

    def test_PUT(self):
        new_obj = self.env.container.file(Utils.create_name())

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'PUT', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        put_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        new_obj.write('new obj contents',
                      parms=put_parms, cfg={'no_auth_token': True})
        self.assertEqual(new_obj.read(), "new obj contents")

        # PUT tempurls also allow HEAD requests
        self.assertTrue(new_obj.info(parms=put_parms,
                                     cfg={'no_auth_token': True}))

    def test_HEAD(self):
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'HEAD', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        head_parms = {'temp_url_sig': sig,
                      'temp_url_expires': str(expires)}

        self.assertTrue(self.env.obj.info(parms=head_parms,
                                          cfg={'no_auth_token': True}))
        # HEAD tempurls don't allow PUT or GET requests, despite the fact that
        # PUT and GET tempurls both allow HEAD requests
        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

        self.assertRaises(ResponseError, self.env.other_obj.write,
                          'new contents',
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_different_object(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_changing_sig(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_sig'][0] == 'a':
            parms['temp_url_sig'] = 'b' + parms['temp_url_sig'][1:]
        else:
            parms['temp_url_sig'] = 'a' + parms['temp_url_sig'][1:]

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    def test_changing_expires(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, "obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_expires'][-1] == '0':
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '1'
        else:
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '0'

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    @requires_acls
    def test_tempurl_keys_visible_to_account_owner(self):
        if not tf.cluster_info.get('tempauth'):
            raise SkipTest('TEMP AUTH SPECIFIC TEST')
        metadata = self.env.container.info()
        self.assertEqual(metadata.get('tempurl_key'), self.env.tempurl_key)
        self.assertEqual(metadata.get('tempurl_key2'), self.env.tempurl_key2)

    @requires_acls
    def test_tempurl_keys_hidden_from_acl_readonly(self):
        if not tf.cluster_info.get('tempauth'):
            raise SkipTest('TEMP AUTH SPECIFIC TEST')
        original_token = self.env.container.conn.storage_token
        self.env.container.conn.storage_token = self.env.conn2.storage_token
        metadata = self.env.container.info()
        self.env.container.conn.storage_token = original_token

        self.assertNotIn(
            'tempurl_key', metadata,
            'Container TempURL key found, should not be visible '
            'to readonly ACLs')
        self.assertNotIn(
            'tempurl_key2', metadata,
            'Container TempURL key-2 found, should not be visible '
            'to readonly ACLs')

    def test_GET_DLO_inside_container(self):
        seg1 = self.env.container.file(
            "get-dlo-inside-seg1" + Utils.create_name())
        seg2 = self.env.container.file(
            "get-dlo-inside-seg2" + Utils.create_name())
        seg1.write("one fish two fish ")
        seg2.write("red fish blue fish")

        manifest = self.env.container.file("manifest" + Utils.create_name())
        manifest.write(
            '',
            hdrs={"X-Object-Manifest": "%s/get-dlo-inside-seg" %
                  (self.env.container.name,)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = manifest.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, "one fish two fish red fish blue fish")

    def test_GET_DLO_outside_container(self):
        container2 = self.env.account.container(Utils.create_name())
        container2.create()
        seg1 = container2.file(
            "get-dlo-outside-seg1" + Utils.create_name())
        seg2 = container2.file(
            "get-dlo-outside-seg2" + Utils.create_name())
        seg1.write("one fish two fish ")
        seg2.write("red fish blue fish")

        manifest = self.env.container.file("manifest" + Utils.create_name())
        manifest.write(
            '',
            hdrs={"X-Object-Manifest": "%s/get-dlo-outside-seg" %
                  (container2.name,)})

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        # cross container tempurl does not work for container tempurl key
        try:
            manifest.read(parms=parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 401)
        else:
            self.fail('request did not error')
        try:
            manifest.info(parms=parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 401)
        else:
            self.fail('request did not error')


class TestContainerTempurlUTF8(Base2, TestContainerTempurl):
    set_up = False


class TestSloTempurlEnv(object):
    enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.enabled is None:
            cls.enabled = 'tempurl' in cluster_info and 'slo' in cluster_info

        cls.tempurl_key = Utils.create_name()

        cls.account = Account(
            cls.conn, tf.config.get('account', tf.config['username']))
        cls.account.delete_containers()
        cls.account.update_metadata({'temp-url-key': cls.tempurl_key})

        cls.manifest_container = cls.account.container(Utils.create_name())
        cls.segments_container = cls.account.container(Utils.create_name())
        if not cls.manifest_container.create():
            raise ResponseError(cls.conn.response)
        if not cls.segments_container.create():
            raise ResponseError(cls.conn.response)

        seg1 = cls.segments_container.file(Utils.create_name())
        seg1.write('1' * 1024 * 1024)

        seg2 = cls.segments_container.file(Utils.create_name())
        seg2.write('2' * 1024 * 1024)

        cls.manifest_data = [{'size_bytes': 1024 * 1024,
                              'etag': seg1.md5,
                              'path': '/%s/%s' % (cls.segments_container.name,
                                                  seg1.name)},
                             {'size_bytes': 1024 * 1024,
                              'etag': seg2.md5,
                              'path': '/%s/%s' % (cls.segments_container.name,
                                                  seg2.name)}]

        cls.manifest = cls.manifest_container.file(Utils.create_name())
        cls.manifest.write(
            json.dumps(cls.manifest_data),
            parms={'multipart-manifest': 'put'})


class TestSloTempurl(Base):
    env = TestSloTempurlEnv
    set_up = False

    def setUp(self):
        super(TestSloTempurl, self).setUp()
        if self.env.enabled is False:
            raise SkipTest("TempURL and SLO not both enabled")
        elif self.env.enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected enabled to be True/False, got %r" %
                (self.env.enabled,))

    def tempurl_sig(self, method, expires, path, key):
        return hmac.new(
            key,
            '%s\n%s\n%s' % (method, expires, urllib.parse.unquote(path)),
            hashlib.sha1).hexdigest()

    def test_GET(self):
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig, 'temp_url_expires': str(expires)}

        contents = self.env.manifest.read(
            parms=parms,
            cfg={'no_auth_token': True})
        self.assertEqual(len(contents), 2 * 1024 * 1024)

        # GET tempurls also allow HEAD requests
        self.assertTrue(self.env.manifest.info(
            parms=parms, cfg={'no_auth_token': True}))


class TestSloTempurlUTF8(Base2, TestSloTempurl):
    set_up = False


class TestServiceToken(unittest2.TestCase):

    def setUp(self):
        if tf.skip_service_tokens:
            raise SkipTest

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
