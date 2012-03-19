#!/usr/bin/python -u
# Copyright (c) 2010-2012 OpenStack, LLC.
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
from datetime import datetime
import locale
import os
import os.path
import random
import StringIO
import sys
import time
import threading
import uuid
import unittest
import urllib

from test import get_config
from swift import Account, AuthenticationFailed, Connection, Container, \
     File, ResponseError

config = get_config()

locale.setlocale(locale.LC_COLLATE, config.get('collate', 'C'))


class Base:
    pass

def chunks(s, length=3):
    i, j = 0, length
    while i < len(s):
        yield s[i:j]
        i, j = j, j + length

def timeout(seconds, method, *args, **kwargs):
    class TimeoutThread(threading.Thread):
        def __init__ (self, method, *args, **kwargs):
            threading.Thread.__init__(self)

            self.method = method
            self.args = args
            self.kwargs = kwargs
            self.exception = None

        def run(self):
            try:
                self.method(*self.args, **self.kwargs)
            except Exception, e:
                self.exception = e

    t = TimeoutThread(method, *args, **kwargs)
    t.start()
    t.join(seconds)

    if t.exception:
        raise t.exception

    if t.isAlive():
        t._Thread__stop()
        return True
    return False

class Utils:
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
        return ''.join([random.choice(utf8_chars) for x in \
            xrange(length)]).encode('utf-8')

    create_name = create_ascii_name

class Base(unittest.TestCase):
    def setUp(self):
        cls = type(self)
        if not cls.set_up:
            cls.env.setUp()
            cls.set_up = True

    def assert_body(self, body):
        response_body = self.env.conn.response.read()
        self.assert_(response_body == body,
           'Body returned: %s' % (response_body))

    def assert_status(self, status_or_statuses):
        self.assert_(self.env.conn.response.status == status_or_statuses or
                     (hasattr(status_or_statuses, '__iter__') and
                      self.env.conn.response.status in status_or_statuses),
           'Status returned: %d Expected: %s' %
           (self.env.conn.response.status, status_or_statuses))

class Base2(object):
    def setUp(self):
        Utils.create_name = Utils.create_utf8_name
        super(Base2, self).setUp()

    def tearDown(self):
        Utils.create_name = Utils.create_ascii_name

class TestAccountEnv:
    @classmethod
    def setUp(cls):
        cls.conn = Connection(config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, config.get('account',
                                                   config['username']))
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
            cfg={'no_auth_token':True})
        self.assert_status([401, 412])

        self.assertRaises(ResponseError, self.env.account.containers,
            cfg={'no_auth_token':True})
        self.assert_status([401, 412])

    def testInvalidUTF8Path(self):
        invalid_utf8 = Utils.create_utf8_name()[::-1]
        container = self.env.account.container(invalid_utf8)
        self.assert_(not container.create(cfg={'no_path_quote':True}))
        self.assert_status(412)
        self.assert_body('Invalid UTF8')

    def testVersionOnlyPath(self):
        self.env.account.conn.make_request('PUT',
            cfg={'version_only_path':True})
        self.assert_status(412)
        self.assert_body('Bad URL')

    def testInvalidPath(self):
        was_url = self.env.account.conn.storage_url
        self.env.account.conn.storage_url = "/%s" % was_url
        self.env.account.conn.make_request('GET')
        try:
            self.assert_status(404)
        finally:
            self.env.account.conn.storage_url = was_url

    def testPUT(self):
        self.env.account.conn.make_request('PUT')
        self.assert_status([403, 405])

    def testAccountHead(self):
        try_count = 0
        while try_count < 5:
            try_count += 1

            info = self.env.account.info()
            for field in ['object_count', 'container_count', 'bytes_used']:
                self.assert_(info[field] >= 0)

            if info['container_count'] == len(self.env.containers):
                break

            if try_count < 5:
                time.sleep(1)

        self.assertEquals(info['container_count'], len(self.env.containers))
        self.assert_status(204)

    def testContainerSerializedInfo(self):
        container_info = {}
        for container in self.env.containers:
            info = {'bytes': 0}
            info['count'] = random.randint(10, 30)
            for i in range(info['count']):
                file = container.file(Utils.create_name())
                bytes = random.randint(1, 32768)
                file.write_random(bytes)
                info['bytes'] += bytes

            container_info[container.name] = info

        for format in ['json', 'xml']:
            for a in self.env.account.containers(
                parms={'format':format}):

                self.assert_(a['count'] >= 0)
                self.assert_(a['bytes'] >= 0)

            headers = dict(self.env.conn.response.getheaders())
            if format == 'json':
                self.assertEquals(headers['content-type'],
                    'application/json; charset=utf-8')
            elif format == 'xml':
                self.assertEquals(headers['content-type'],
                    'application/xml; charset=utf-8')

    def testListingLimit(self):
        limit = 10000

        for l in (1, 100, limit/2, limit-1, limit, limit+1, limit*2):
            p = {'limit':l}

            if l <= limit:
                self.assert_(len(self.env.account.containers(parms=p)) <= l)
                self.assert_status(200)
            else:
                self.assertRaises(ResponseError,
                    self.env.account.containers, parms=p)
                self.assert_status(412)

    def testContainerListing(self):
        a = sorted([c.name for c in self.env.containers])

        for format in [None, 'json', 'xml']:
            b = self.env.account.containers(parms={'format':format})

            if isinstance(b[0], dict):
                b = [x['name'] for x in b]

            self.assertEquals(a, b)

    def testInvalidAuthToken(self):
        hdrs = {'X-Auth-Token': 'bogus_auth_token'}
        self.assertRaises(ResponseError, self.env.account.info, hdrs=hdrs)
        self.assert_status(401)

    def testLastContainerMarker(self):
        for format in [None, 'json', 'xml']:
            containers = self.env.account.containers({'format':format})
            self.assertEquals(len(containers), len(self.env.containers))
            self.assert_status(200)

            containers = self.env.account.containers(
                parms={'format':format,'marker':containers[-1]})
            self.assertEquals(len(containers), 0)
            if format is None:
                self.assert_status(204)
            else:
                self.assert_status(200)

    def testMarkerLimitContainerList(self):
        for format in [None, 'json', 'xml']:
            for marker in ['0', 'A', 'I', 'R', 'Z', 'a', 'i', 'r', 'z', \
                'abc123', 'mnop', 'xyz']:

                limit = random.randint(2, 9)
                containers = self.env.account.containers(
                    parms={'format':format, 'marker':marker, 'limit':limit})
                self.assert_(len(containers) <= limit)
                if containers:
                   if isinstance(containers[0], dict):
                       containers = [x['name'] for x in containers]
                   self.assert_(locale.strcoll(containers[0], marker) > 0)

    def testContainersOrderedByName(self):
        for format in [None, 'json', 'xml']:
            containers = self.env.account.containers(
                parms={'format':format})
            if isinstance(containers[0], dict):
                containers = [x['name'] for x in containers]
            self.assertEquals(sorted(containers, cmp=locale.strcoll),
                containers)

class TestAccountUTF8(Base2, TestAccount):
    set_up = False

class TestAccountNoContainersEnv:
    @classmethod
    def setUp(cls):
        cls.conn = Connection(config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, config.get('account',
                                                   config['username']))
        cls.account.delete_containers()

class TestAccountNoContainers(Base):
    env = TestAccountNoContainersEnv
    set_up = False

    def testGetRequest(self):
        for format in [None, 'json', 'xml']:
            self.assert_(not self.env.account.containers(
                parms={'format':format}))

            if format is None:
                self.assert_status(204)
            else:
                self.assert_status(200)

class TestAccountNoContainersUTF8(Base2, TestAccountNoContainers):
    set_up = False

class TestContainerEnv:
    @classmethod
    def setUp(cls):
        cls.conn = Connection(config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, config.get('account',
                                                   config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_count = 10
        cls.file_size = 128
        cls.files = list()
        for x in range(cls.file_count):
            file = cls.container.file(Utils.create_name())
            file.write_random(cls.file_size)
            cls.files.append(file.name)

class TestContainerDev(Base):
    env = TestContainerEnv
    set_up = False

class TestContainerDevUTF8(Base2, TestContainerDev):
    set_up = False

class TestContainer(Base):
    env = TestContainerEnv
    set_up = False

    def testContainerNameLimit(self):
        limit = 256

        for l in (limit-100, limit-10, limit-1, limit,
            limit+1, limit+10, limit+100):

            cont = self.env.account.container('a'*l)
            if l <= limit:
                self.assert_(cont.create())
                self.assert_status(201)
            else:
                self.assert_(not cont.create())
                self.assert_status(400)

    def testFileThenContainerDelete(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())
        file = cont.file(Utils.create_name())
        self.assert_(file.write_random())

        self.assert_(file.delete())
        self.assert_status(204)
        self.assert_(file.name not in cont.files())

        self.assert_(cont.delete())
        self.assert_status(204)
        self.assert_(cont.name not in self.env.account.containers())

    def testFileListingLimitMarkerPrefix(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())

        files = sorted([Utils.create_name() for x in xrange(10)])
        for f in files:
            file = cont.file(f)
            self.assert_(file.write_random())

        for i in xrange(len(files)):
            f = files[i]
            for j in xrange(1, len(files)-i):
                self.assert_(cont.files(parms={'limit':j, 'marker':f}) == files[i+1:i+j+1])
            self.assert_(cont.files(parms={'marker':f}) == files[i+1:])
            self.assert_(cont.files(parms={'marker': f, 'prefix':f}) == [])
            self.assert_(cont.files(parms={'prefix': f}) == [f])

    def testPrefixAndLimit(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())

        prefix_file_count = 10
        limit_count = 2
        prefixs = ['alpha/', 'beta/', 'kappa/']
        prefix_files = {}

        all_files = []
        for prefix in prefixs:
            prefix_files[prefix] = []

            for i in range(prefix_file_count):
                file = cont.file(prefix + Utils.create_name())
                file.write()
                prefix_files[prefix].append(file.name)

        for format in [None, 'json', 'xml']:
            for prefix in prefixs:
                files = cont.files(parms={'prefix':prefix})
                self.assertEquals(files, sorted(prefix_files[prefix]))

        for format in [None, 'json', 'xml']:
            for prefix in prefixs:
                files = cont.files(parms={'limit':limit_count,
                    'prefix':prefix})
                self.assertEquals(len(files), limit_count)

                for file in files:
                    self.assert_(file.startswith(prefix))

    def testCreate(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())
        self.assert_status(201)
        self.assert_(cont.name in self.env.account.containers())

    def testContainerFileListOnContainerThatDoesNotExist(self):
        for format in [None, 'json', 'xml']:
            container = self.env.account.container(Utils.create_name())
            self.assertRaises(ResponseError, container.files,
                parms={'format':format})
            self.assert_status(404)

    def testUtf8Container(self):
        valid_utf8 = Utils.create_utf8_name()
        invalid_utf8 = valid_utf8[::-1]
        container = self.env.account.container(valid_utf8)
        self.assert_(container.create(cfg={'no_path_quote':True}))
        self.assert_(container.name in self.env.account.containers())
        self.assertEquals(container.files(), [])
        self.assert_(container.delete())

        container = self.env.account.container(invalid_utf8)
        self.assert_(not container.create(cfg={'no_path_quote':True}))
        self.assert_status(412)
        self.assertRaises(ResponseError, container.files,
            cfg={'no_path_quote':True})
        self.assert_status(412)

    def testCreateOnExisting(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())
        self.assert_status(201)
        self.assert_(cont.create())
        self.assert_status(202)

    def testSlashInName(self):
        if Utils.create_name == Utils.create_utf8_name:
            cont_name = list(unicode(Utils.create_name(), 'utf-8'))
        else:
            cont_name = list(Utils.create_name())

        cont_name[random.randint(2, len(cont_name)-2)] = '/'
        cont_name = ''.join(cont_name)

        if Utils.create_name == Utils.create_utf8_name:
            cont_name = cont_name.encode('utf-8')

        cont = self.env.account.container(cont_name)
        self.assert_(not cont.create(cfg={'no_path_quote':True}),
            'created container with name %s' % (cont_name))
        self.assert_status(404)
        self.assert_(cont.name not in self.env.account.containers())

    def testDelete(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())
        self.assert_status(201)
        self.assert_(cont.delete())
        self.assert_status(204)
        self.assert_(cont.name not in self.env.account.containers())

    def testDeleteOnContainerThatDoesNotExist(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(not cont.delete())
        self.assert_status(404)

    def testDeleteOnContainerWithFiles(self):
        cont = self.env.account.container(Utils.create_name())
        self.assert_(cont.create())
        file = cont.file(Utils.create_name())
        file.write_random(self.env.file_size)
        self.assert_(file.name in cont.files())
        self.assert_(not cont.delete())
        self.assert_status(409)

    def testFileCreateInContainerThatDoesNotExist(self):
        file = File(self.env.conn, self.env.account, Utils.create_name(),
            Utils.create_name())
        self.assertRaises(ResponseError, file.write)
        self.assert_status(404)

    def testLastFileMarker(self):
        for format in [None, 'json', 'xml']:
            files = self.env.container.files({'format':format})
            self.assertEquals(len(files), len(self.env.files))
            self.assert_status(200)

            files = self.env.container.files(
                parms={'format':format,'marker':files[-1]})
            self.assertEquals(len(files), 0)

            if format is None:
                self.assert_status(204)
            else:
                self.assert_status(200)

    def testContainerFileList(self):
        for format in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format':format})
            self.assert_status(200)
            if isinstance(files[0], dict):
                files = [x['name'] for x in files]

            for file in self.env.files:
                self.assert_(file in files)

            for file in files:
                self.assert_(file in self.env.files)

    def testMarkerLimitFileList(self):
        for format in [None, 'json', 'xml']:
            for marker in ['0', 'A', 'I', 'R', 'Z', 'a', 'i', 'r', 'z', \
                'abc123', 'mnop', 'xyz']:

                limit = random.randint(2, self.env.file_count-1)
                files = self.env.container.files(parms={'format':format, \
                    'marker':marker, 'limit':limit})

                if not files:
                    continue

                if isinstance(files[0], dict):
                    files = [x['name'] for x in files]
                
                self.assert_(len(files) <= limit)
                if files:
                    if isinstance(files[0], dict):
                        files = [x['name'] for x in files]
                    self.assert_(locale.strcoll(files[0], marker) > 0)

    def testFileOrder(self):
        for format in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format':format})
            if isinstance(files[0], dict):
                files = [x['name'] for x in files]
            self.assertEquals(sorted(files, cmp=locale.strcoll), files)

    def testContainerInfo(self):
        info = self.env.container.info()
        self.assert_status(204)
        self.assertEquals(info['object_count'], self.env.file_count)
        self.assertEquals(info['bytes_used'],
            self.env.file_count*self.env.file_size)

    def testContainerInfoOnContainerThatDoesNotExist(self):
        container = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, container.info)
        self.assert_status(404)

    def testContainerFileListWithLimit(self):
        for format in [None, 'json', 'xml']:
            files = self.env.container.files(parms={'format':format,
                'limit':2})
            self.assertEquals(len(files), 2)

    def testTooLongName(self):
        cont = self.env.account.container('x'*257)
        self.assert_(not cont.create(), 'created container with name %s' % \
            (cont.name))
        self.assert_status(400)

    def testContainerExistenceCachingProblem(self):
        cont = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, cont.files)
        self.assert_(cont.create())
        cont.files()

        cont = self.env.account.container(Utils.create_name())
        self.assertRaises(ResponseError, cont.files)
        self.assert_(cont.create())
        file = cont.file(Utils.create_name())
        file.write_random()

class TestContainerUTF8(Base2, TestContainer):
    set_up = False

class TestContainerPathsEnv:
    @classmethod
    def setUp(cls):
        cls.conn = Connection(config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, config.get('account',
                                                   config['username']))
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

        for f in cls.files:
            file = cls.container.file(f)
            if f.endswith('/'):
                file.write(hdrs={'content-type': 'application/directory'})
            else:
                file.write_random(cls.file_size, hdrs={'content-type': \
                    'application/directory'})

class TestContainerPaths(Base):
    env = TestContainerPathsEnv
    set_up = False

    def testTraverseContainer(self):
        found_files = []
        found_dirs = []
        def recurse_path(path, count=0):
            if count > 10:
                raise ValueError('too deep recursion')

            for file in self.env.container.files(parms={'path':path}):
                self.assert_(file.startswith(path))
                if file.endswith('/'):
                    recurse_path(file, count + 1)
                    found_dirs.append(file)
                else:
                    found_files.append(file)
        recurse_path('')
        for file in self.env.files:
            if file.startswith('/'):
                self.assert_(file not in found_dirs)
                self.assert_(file not in found_files)
            elif file.endswith('/'):
                self.assert_(file in found_dirs)
                self.assert_(file not in found_files)
            else:
                self.assert_(file in found_files)
                self.assert_(file not in found_dirs)
        found_files = []
        found_dirs = []
        recurse_path('/')
        for file in self.env.files:
            if not file.startswith('/'):
                self.assert_(file not in found_dirs)
                self.assert_(file not in found_files)
            elif file.endswith('/'):
                self.assert_(file in found_dirs)
                self.assert_(file not in found_files)
            else:
                self.assert_(file in found_files)
                self.assert_(file not in found_dirs)

    def testContainerListing(self):
        for format in (None, 'json', 'xml'):
            files = self.env.container.files(parms={'format':format})

            if isinstance(files[0], dict):
                files = [str(x['name']) for x in files]

            self.assertEquals(files, sorted(self.env.files))

        for format in ('json', 'xml'):
            for file in self.env.container.files(parms={'format':format}):
                self.assert_(int(file['bytes']) >= 0)
                self.assert_(file.has_key('last_modified'))
                if file['name'].endswith('/'):
                    self.assertEquals(file['content_type'],
                        'application/directory')

    def testStructure(self):
        def assert_listing(path, list):
            files = self.env.container.files(parms={'path':path})
            self.assertEquals(sorted(list, cmp=locale.strcoll), files)

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

class TestFileEnv:
    @classmethod
    def setUp(cls):
        cls.conn = Connection(config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, config.get('account',
                                                   config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_size = 128

class TestFileDev(Base):
    env = TestFileEnv
    set_up = False

class TestFileDevUTF8(Base2, TestFileDev):
    set_up = False

class TestFile(Base):
    env = TestFileEnv
    set_up = False

    def testCopy(self):
        # makes sure to test encoded characters"
        source_filename = 'dealde%2Fl04 011e%204c8df/flash.png'
        file = self.env.container.file(source_filename)

        metadata = {}
        for i in range(1):
            metadata[Utils.create_name()] = Utils.create_name()

        data = file.write_random()
        file.sync_metadata(metadata)

        dest_cont = self.env.account.container(Utils.create_name())
        self.assert_(dest_cont.create())

        # copy both from within and across containers
        for cont in (self.env.container, dest_cont):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file = self.env.container.file(source_filename)
                file.copy('%s%s' % (prefix, cont), dest_filename)

                self.assert_(dest_filename in cont.files())

                file = cont.file(dest_filename)

                self.assert_(data == file.read())
                self.assert_(file.initialize())
                self.assert_(metadata == file.metadata)

    def testCopy404s(self):
        source_filename = Utils.create_name()
        file = self.env.container.file(source_filename)
        file.write_random()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assert_(dest_cont.create())

        for prefix in ('', '/'):
            # invalid source container
            source_cont = self.env.account.container(Utils.create_name())
            file = source_cont.file(source_filename)
            self.assert_(not file.copy('%s%s' % (prefix, self.env.container),
                Utils.create_name()))
            self.assert_status(404)

            self.assert_(not file.copy('%s%s' % (prefix, dest_cont),
                Utils.create_name()))
            self.assert_status(404)

            # invalid source object
            file = self.env.container.file(Utils.create_name())
            self.assert_(not file.copy('%s%s' % (prefix, self.env.container),
                Utils.create_name()))
            self.assert_status(404)

            self.assert_(not file.copy('%s%s' % (prefix, dest_cont),
                Utils.create_name()))
            self.assert_status(404)
           
            # invalid destination container
            file = self.env.container.file(source_filename)
            self.assert_(not file.copy('%s%s' % (prefix, Utils.create_name()),
                Utils.create_name()))

    def testCopyNoDestinationHeader(self):
        source_filename = Utils.create_name()
        file = self.env.container.file(source_filename)
        file.write_random()

        file = self.env.container.file(source_filename)
        self.assert_(not file.copy(Utils.create_name(), Utils.create_name(),
            cfg={'no_destination': True}))
        self.assert_status(412)

    def testCopyDestinationSlashProblems(self):
        source_filename = Utils.create_name()
        file = self.env.container.file(source_filename)
        file.write_random()

        # no slash
        self.assert_(not file.copy(Utils.create_name(), Utils.create_name(),
            cfg={'destination': Utils.create_name()}))
        self.assert_status(412)

    def testCopyFromHeader(self):
        source_filename = Utils.create_name()
        file = self.env.container.file(source_filename)

        metadata = {}
        for i in range(1):
            metadata[Utils.create_name()] = Utils.create_name()
        file.metadata = metadata

        data = file.write_random()

        dest_cont = self.env.account.container(Utils.create_name())
        self.assert_(dest_cont.create())

        # copy both from within and across containers
        for cont in (self.env.container, dest_cont):
            # copy both with and without initial slash
            for prefix in ('', '/'):
                dest_filename = Utils.create_name()

                file = cont.file(dest_filename)
                file.write(hdrs={'X-Copy-From': '%s%s/%s' % (prefix,
                    self.env.container.name, source_filename)})

                self.assert_(dest_filename in cont.files())

                file = cont.file(dest_filename)

                self.assert_(data == file.read())
                self.assert_(file.initialize())
                self.assert_(metadata == file.metadata)

    def testCopyFromHeader404s(self):
        source_filename = Utils.create_name()
        file = self.env.container.file(source_filename)
        file.write_random()

        for prefix in ('', '/'):
            # invalid source container
            file = self.env.container.file(Utils.create_name())
            self.assertRaises(ResponseError, file.write,
                hdrs={'X-Copy-From': '%s%s/%s' % (prefix,
                Utils.create_name(), source_filename)})
            self.assert_status(404)

            # invalid source object
            file = self.env.container.file(Utils.create_name())
            self.assertRaises(ResponseError, file.write,
                hdrs={'X-Copy-From': '%s%s/%s' % (prefix,
                self.env.container.name, Utils.create_name())})
            self.assert_status(404)

            # invalid destination container
            dest_cont = self.env.account.container(Utils.create_name())
            file = dest_cont.file(Utils.create_name())
            self.assertRaises(ResponseError, file.write,
                hdrs={'X-Copy-From': '%s%s/%s' % (prefix,
                self.env.container.name, source_filename)})
            self.assert_status(404)

    def testNameLimit(self):
        limit = 1024

        for l in (1, 10, limit/2, limit-1, limit, limit+1, limit*2):
            file = self.env.container.file('a'*l)

            if l <= limit:
                self.assert_(file.write())
                self.assert_status(201)
            else:
                self.assertRaises(ResponseError, file.write)
                self.assert_status(400)

    def testQuestionMarkInName(self):
        if Utils.create_name == Utils.create_ascii_name:
            file_name = list(Utils.create_name())
            file_name[random.randint(2, len(file_name)-2)] = '?'
            file_name = "".join(file_name)
        else:
            file_name = Utils.create_name(6) + '?' + Utils.create_name(6)

        file = self.env.container.file(file_name)
        self.assert_(file.write(cfg={'no_path_quote':True}))
        self.assert_(file_name not in self.env.container.files())
        self.assert_(file_name.split('?')[0] in self.env.container.files())

    def testDeleteThen404s(self):
        file = self.env.container.file(Utils.create_name())
        self.assert_(file.write_random())
        self.assert_status(201)

        self.assert_(file.delete())
        self.assert_status(204)

        file.metadata = {Utils.create_name(): Utils.create_name()}

        for method in (file.info, file.read, file.sync_metadata, \
            file.delete):

            self.assertRaises(ResponseError, method)
            self.assert_status(404)

    def testBlankMetadataName(self):
        file = self.env.container.file(Utils.create_name())
        file.metadata = {'': Utils.create_name()}
        self.assertRaises(ResponseError, file.write_random)
        self.assert_status(400)

    def testMetadataNumberLimit(self):
        number_limit = 90

        for i in (number_limit-10, number_limit-1, number_limit,
            number_limit+1, number_limit+10, number_limit+100):

            size_limit = 4096

            j = size_limit/(i * 2)

            size = 0
            metadata = {}
            while len(metadata.keys()) < i:
                key = Utils.create_name()
                val = Utils.create_name()

                if len(key) > j:
                    key = key[:j]
                    val = val[:j]

                size += len(key) + len(val)
                metadata[key] = val

            file = self.env.container.file(Utils.create_name())
            file.metadata = metadata

            if i <= number_limit:
                self.assert_(file.write())
                self.assert_status(201)
                self.assert_(file.sync_metadata())
                self.assert_status((201, 202))
            else:
                self.assertRaises(ResponseError, file.write)
                self.assert_status(400)
                file.metadata = {}
                self.assert_(file.write())
                self.assert_status(201)
                file.metadata = metadata
                self.assertRaises(ResponseError, file.sync_metadata)
                self.assert_status(400)

    def testContentTypeGuessing(self):
        file_types = {'wav': 'audio/x-wav', 'txt': 'text/plain',
                      'zip': 'application/zip'}

        container = self.env.account.container(Utils.create_name())
        self.assert_(container.create())

        for i in file_types.keys():
            file = container.file(Utils.create_name() + '.' + i)
            file.write('', cfg={'no_content_type':True})

        file_types_read = {}
        for i in container.files(parms={'format': 'json'}):
            file_types_read[i['name'].split('.')[1]] = i['content_type']

        self.assertEquals(file_types, file_types_read)

    def testRangedGets(self):
        file_length = 10000
        range_size = file_length/10
        file = self.env.container.file(Utils.create_name())
        data = file.write_random(file_length)

        for i in range(0, file_length, range_size):
            range_string = 'bytes=%d-%d' % (i, i+range_size-1)
            hdrs = {'Range': range_string}
            self.assert_(data[i:i+range_size] == file.read(hdrs=hdrs),
                range_string)

            range_string = 'bytes=-%d' % (i)
            hdrs = {'Range': range_string}
            self.assert_(file.read(hdrs=hdrs) == data[-i:], range_string)

            range_string = 'bytes=%d-' % (i)
            hdrs = {'Range': range_string}
            self.assert_(file.read(hdrs=hdrs) == data[i-file_length:],
                range_string)

        range_string = 'bytes=%d-%d' % (file_length+1000, file_length+2000)
        hdrs = {'Range': range_string}
        self.assertRaises(ResponseError, file.read, hdrs=hdrs)
        self.assert_status(416)

        range_string = 'bytes=%d-%d' % (file_length-1000, file_length+2000)
        hdrs = {'Range': range_string}
        self.assert_(file.read(hdrs=hdrs) == data[-1000:], range_string)

        hdrs = {'Range': '0-4'}
        self.assert_(file.read(hdrs=hdrs) == data, range_string)

        for r in ('BYTES=0-999', 'bytes = 0-999', 'BYTES = 0 - 999',
            'bytes = 0 - 999', 'bytes=0 - 999', 'bytes=0-999 '):

            self.assert_(file.read(hdrs={'Range': r}) == data[0:1000])

    def testFileSizeLimit(self):
        limit = 5*2**30 + 2
        tsecs = 3

        for i in (limit-100, limit-10, limit-1, limit, limit+1, limit+10,
            limit+100):

            file = self.env.container.file(Utils.create_name())

            if i <= limit:
                self.assert_(timeout(tsecs, file.write,
                    cfg={'set_content_length':i}))
            else:
                self.assertRaises(ResponseError, timeout, tsecs,
                    file.write, cfg={'set_content_length':i})

    def testNoContentLengthForPut(self):
        file = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.write, 'testing',
            cfg={'no_content_length':True})
        self.assert_status(411)

    def testDelete(self):
        file = self.env.container.file(Utils.create_name())
        file.write_random(self.env.file_size)

        self.assert_(file.name in self.env.container.files())
        self.assert_(file.delete())
        self.assert_(file.name not in self.env.container.files())

    def testBadHeaders(self):
        file_length = 100

        # no content type on puts should be ok
        file = self.env.container.file(Utils.create_name())
        file.write_random(file_length, cfg={'no_content_type':True})
        self.assert_status(201)

        # content length x
        self.assertRaises(ResponseError, file.write_random, file_length,
            hdrs={'Content-Length':'X'}, cfg={'no_content_length':True})
        self.assert_status(400)

        # bad request types
        #for req in ('LICK', 'GETorHEAD_base', 'container_info', 'best_response'):
        for req in ('LICK', 'GETorHEAD_base'):
            self.env.account.conn.make_request(req)
            self.assert_status(405)

        # bad range headers
        self.assert_(len(file.read(hdrs={'Range':'parsecs=8-12'})) == \
            file_length)
        self.assert_status(200)

    def testMetadataLengthLimits(self):
        key_limit, value_limit = 128, 256
        lengths = [[key_limit, value_limit], [key_limit, value_limit+1], \
            [key_limit+1, value_limit], [key_limit, 0], \
            [key_limit, value_limit*10], [key_limit*10, value_limit]] 

        for l in lengths:
            metadata = {'a'*l[0]: 'b'*l[1]}
            file = self.env.container.file(Utils.create_name())
            file.metadata = metadata

            if l[0] <= key_limit and l[1] <= value_limit:
                self.assert_(file.write())
                self.assert_status(201)
                self.assert_(file.sync_metadata())
            else:
                self.assertRaises(ResponseError, file.write)
                self.assert_status(400)
                file.metadata = {}
                self.assert_(file.write())
                self.assert_status(201)
                file.metadata = metadata
                self.assertRaises(ResponseError, file.sync_metadata)
                self.assert_status(400)

    def testEtagWayoff(self):
        file = self.env.container.file(Utils.create_name())
        hdrs = {'etag': 'reallylonganddefinitelynotavalidetagvalue'}
        self.assertRaises(ResponseError, file.write_random, hdrs=hdrs) 
        self.assert_status(422)

    def testFileCreate(self):
        for i in range(10):
            file = self.env.container.file(Utils.create_name())
            data = file.write_random()
            self.assert_status(201)
            self.assert_(data == file.read())
            self.assert_status(200)

    def testHead(self):
        file_name = Utils.create_name()
        content_type = Utils.create_name()

        file = self.env.container.file(file_name)
        file.content_type = content_type
        file.write_random(self.env.file_size)

        md5 = file.md5

        file = self.env.container.file(file_name)
        info = file.info()

        self.assert_status(200)
        self.assertEquals(info['content_length'], self.env.file_size)
        self.assertEquals(info['etag'], md5)
        self.assertEquals(info['content_type'], content_type)
        self.assert_(info.has_key('last_modified'))

    def testDeleteOfFileThatDoesNotExist(self):
        # in container that exists
        file = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.delete)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file = container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.delete)
        self.assert_status(404)

    def testHeadOnFileThatDoesNotExist(self):
        # in container that exists
        file = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.info)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file = container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.info)
        self.assert_status(404)

    def testMetadataOnPost(self):
        file = self.env.container.file(Utils.create_name())
        file.write_random(self.env.file_size)

        for i in range(10):
            metadata = {}
            for i in range(10):
                metadata[Utils.create_name()] = Utils.create_name()

            file.metadata = metadata
            self.assert_(file.sync_metadata())
            self.assert_status((201, 202))

            file = self.env.container.file(file.name)
            self.assert_(file.initialize())
            self.assert_status(200)
            self.assertEquals(file.metadata, metadata)

    def testGetContentType(self):
        file_name = Utils.create_name()
        content_type = Utils.create_name()

        file = self.env.container.file(file_name)
        file.content_type = content_type
        file.write_random()

        file = self.env.container.file(file_name)
        file.read()

        self.assertEquals(content_type, file.content_type)

    def testGetOnFileThatDoesNotExist(self):
        # in container that exists
        file = self.env.container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.read)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file = container.file(Utils.create_name())
        self.assertRaises(ResponseError, file.read)
        self.assert_status(404)

    def testPostOnFileThatDoesNotExist(self):
        # in container that exists
        file = self.env.container.file(Utils.create_name())
        file.metadata['Field'] = 'Value'
        self.assertRaises(ResponseError, file.sync_metadata)
        self.assert_status(404)

        # in container that does not exist
        container = self.env.account.container(Utils.create_name())
        file = container.file(Utils.create_name())
        file.metadata['Field'] = 'Value'
        self.assertRaises(ResponseError, file.sync_metadata)
        self.assert_status(404)

    def testMetadataOnPut(self):
        for i in range(10):
            metadata = {}
            for j in range(10):
                metadata[Utils.create_name()] = Utils.create_name()

            file = self.env.container.file(Utils.create_name())
            file.metadata = metadata
            file.write_random(self.env.file_size)

            file = self.env.container.file(file.name)
            self.assert_(file.initialize())
            self.assert_status(200)
            self.assertEquals(file.metadata, metadata)

    def testSerialization(self):
        container = self.env.account.container(Utils.create_name())
        self.assert_(container.create())

        files = []
        for i in (0, 1, 10, 100, 1000, 10000):
            files.append({'name': Utils.create_name(), \
                'content_type': Utils.create_name(), 'bytes':i})

        write_time = time.time()
        for f in files:
            file = container.file(f['name'])
            file.content_type = f['content_type']
            file.write_random(f['bytes'])

            f['hash'] = file.md5
            f['json'] = False
            f['xml'] = False
        write_time = time.time() - write_time

        for format in ['json', 'xml']:
            for file in container.files(parms={'format': format}):
                found = False
                for f in files:
                    if f['name'] != file['name']:
                        continue

                    self.assertEquals(file['content_type'],
                        f['content_type'])
                    self.assertEquals(int(file['bytes']), f['bytes'])

                    d = datetime.strptime(file['last_modified'].\
                        split('.')[0], "%Y-%m-%dT%H:%M:%S")
                    lm = time.mktime(d.timetuple())

                    if f.has_key('last_modified'):
                        self.assertEquals(f['last_modified'], lm)
                    else:
                       f['last_modified'] = lm

                    f[format] = True
                    found = True

                self.assert_(found, 'Unexpected file %s found in ' \
                    '%s listing' % (file['name'], format))

            headers = dict(self.env.conn.response.getheaders())
            if format == 'json':
                self.assertEquals(headers['content-type'],
                    'application/json; charset=utf-8')
            elif format == 'xml':
                self.assertEquals(headers['content-type'],
                    'application/xml; charset=utf-8')

        lm_diff = max([f['last_modified'] for f in files]) - \
            min([f['last_modified'] for f in files])
        self.assert_(lm_diff < write_time + 1, 'Diff in last ' + \
            'modified times should be less than time to write files')

        for f in files:
            for format in ['json', 'xml']:
                self.assert_(f[format], 'File %s not found in %s listing' \
                    % (f['name'], format))

    def testStackedOverwrite(self):
        file = self.env.container.file(Utils.create_name())

        for i in range(1, 11):
            data = file.write_random(512)
            file.write(data)

        self.assert_(file.read() == data)

    def testTooLongName(self):
        file = self.env.container.file('x'*1025)
        self.assertRaises(ResponseError, file.write) 
        self.assert_status(400) 

    def testZeroByteFile(self):
        file = self.env.container.file(Utils.create_name())

        self.assert_(file.write(''))
        self.assert_(file.name in self.env.container.files())
        self.assert_(file.read() == '')

    def testEtagResponse(self):
        file = self.env.container.file(Utils.create_name())

        data = StringIO.StringIO(file.write_random(512))
        etag = File.compute_md5sum(data)

        headers = dict(self.env.conn.response.getheaders())
        self.assert_('etag' in headers.keys())

        header_etag = headers['etag'].strip('"')
        self.assertEquals(etag, header_etag)

    def testChunkedPut(self):
        data = File.random_data(10000)
        etag = File.compute_md5sum(data)

        for i in (1, 10, 100, 1000):
            file = self.env.container.file(Utils.create_name())

            for j in chunks(data, i):
                file.chunked_write(j)

            self.assert_(file.chunked_write())
            self.assert_(data == file.read())

            info = file.info()
            self.assertEquals(etag, info['etag'])

class TestFileUTF8(Base2, TestFile):
    set_up = False

class TestFileComparisonEnv:
    @classmethod
    def setUp(cls):
        cls.conn = Connection(config)
        cls.conn.authenticate()
        cls.account = Account(cls.conn, config.get('account',
                                                   config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())

        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.file_count = 20
        cls.file_size = 128
        cls.files = list()
        for x in range(cls.file_count):
            file = cls.container.file(Utils.create_name())
            file.write_random(cls.file_size)
            cls.files.append(file)

        cls.time_old = time.asctime(time.localtime(time.time()-86400))
        cls.time_new = time.asctime(time.localtime(time.time()+86400))

class TestFileComparison(Base):
    env = TestFileComparisonEnv
    set_up = False

    def testIfMatch(self):
        for file in self.env.files:
            hdrs = {'If-Match': file.md5}
            self.assert_(file.read(hdrs=hdrs))

            hdrs = {'If-Match': 'bogus'}
            self.assertRaises(ResponseError, file.read, hdrs=hdrs)
            self.assert_status(412)

    def testIfNoneMatch(self):
        for file in self.env.files:
            hdrs = {'If-None-Match': 'bogus'}
            self.assert_(file.read(hdrs=hdrs))

            hdrs = {'If-None-Match': file.md5}
            self.assertRaises(ResponseError, file.read, hdrs=hdrs)
            self.assert_status(304)

    def testIfModifiedSince(self):
        for file in self.env.files:
            hdrs = {'If-Modified-Since': self.env.time_old}
            self.assert_(file.read(hdrs=hdrs))

            hdrs = {'If-Modified-Since': self.env.time_new}
            self.assertRaises(ResponseError, file.read, hdrs=hdrs)
            self.assert_status(304)

    def testIfUnmodifiedSince(self):
        for file in self.env.files:
            hdrs = {'If-Unmodified-Since': self.env.time_new}
            self.assert_(file.read(hdrs=hdrs))

            hdrs = {'If-Unmodified-Since': self.env.time_old}
            self.assertRaises(ResponseError, file.read, hdrs=hdrs)
            self.assert_status(412)

    def testIfMatchAndUnmodified(self):
        for file in self.env.files:
            hdrs = {'If-Match': file.md5, 'If-Unmodified-Since': \
                self.env.time_new}
            self.assert_(file.read(hdrs=hdrs))

            hdrs = {'If-Match': 'bogus', 'If-Unmodified-Since': \
                self.env.time_new}
            self.assertRaises(ResponseError, file.read, hdrs=hdrs)
            self.assert_status(412)

            hdrs = {'If-Match': file.md5, 'If-Unmodified-Since': \
                self.env.time_old}
            self.assertRaises(ResponseError, file.read, hdrs=hdrs)
            self.assert_status(412)

class TestFileComparisonUTF8(Base2, TestFileComparison):
    set_up = False

if __name__ == '__main__':
    unittest.main()
