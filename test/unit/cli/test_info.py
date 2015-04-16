# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Tests for swift.cli.info"""

import os
import unittest
import mock
from cStringIO import StringIO
from shutil import rmtree
from tempfile import mkdtemp

from test.unit import patch_policies, write_fake_ring

from swift.common import ring, utils
from swift.common.swob import Request
from swift.common.storage_policy import StoragePolicy, POLICIES
from swift.cli.info import print_db_info_metadata, print_ring_locations, \
    print_info, print_obj_metadata, print_obj, InfoSystemExit
from swift.account.server import AccountController
from swift.container.server import ContainerController
from swift.obj.diskfile import write_metadata


@patch_policies([StoragePolicy(0, 'zero', True),
                 StoragePolicy(1, 'one', False),
                 StoragePolicy(2, 'two', False)])
class TestCliInfoBase(unittest.TestCase):
    def setUp(self):
        self.orig_hp = utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX
        utils.HASH_PATH_PREFIX = 'info'
        utils.HASH_PATH_SUFFIX = 'info'
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_cli_info')
        utils.mkdirs(self.testdir)
        rmtree(self.testdir)
        utils.mkdirs(os.path.join(self.testdir, 'sda1'))
        utils.mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        utils.mkdirs(os.path.join(self.testdir, 'sdb1'))
        utils.mkdirs(os.path.join(self.testdir, 'sdb1', 'tmp'))
        self.account_ring_path = os.path.join(self.testdir, 'account.ring.gz')
        account_devs = [
            {'ip': '127.0.0.1', 'port': 42},
            {'ip': '127.0.0.2', 'port': 43},
        ]
        write_fake_ring(self.account_ring_path, *account_devs)
        self.container_ring_path = os.path.join(self.testdir,
                                                'container.ring.gz')
        container_devs = [
            {'ip': '127.0.0.3', 'port': 42},
            {'ip': '127.0.0.4', 'port': 43},
        ]
        write_fake_ring(self.container_ring_path, *container_devs)
        self.object_ring_path = os.path.join(self.testdir, 'object.ring.gz')
        object_devs = [
            {'ip': '127.0.0.3', 'port': 42},
            {'ip': '127.0.0.4', 'port': 43},
        ]
        write_fake_ring(self.object_ring_path, *object_devs)
        # another ring for policy 1
        self.one_ring_path = os.path.join(self.testdir, 'object-1.ring.gz')
        write_fake_ring(self.one_ring_path, *object_devs)
        # ... and another for policy 2
        self.two_ring_path = os.path.join(self.testdir, 'object-2.ring.gz')
        write_fake_ring(self.two_ring_path, *object_devs)

    def tearDown(self):
        utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX = self.orig_hp
        rmtree(os.path.dirname(self.testdir))

    def assertRaisesMessage(self, exc, msg, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            self.assertTrue(msg in str(e),
                            "Expected %r in %r" % (msg, str(e)))
            self.assertTrue(isinstance(e, exc),
                            "Expected %s, got %s" % (exc, type(e)))


class TestCliInfo(TestCliInfoBase):
    def test_print_db_info_metadata(self):
        self.assertRaisesMessage(ValueError, 'Wrong DB type',
                                 print_db_info_metadata, 't', {}, {})
        self.assertRaisesMessage(ValueError, 'DB info is None',
                                 print_db_info_metadata, 'container', None, {})
        self.assertRaisesMessage(ValueError, 'Info is incomplete',
                                 print_db_info_metadata, 'container', {}, {})

        info = dict(
            account='acct',
            created_at=100.1,
            put_timestamp=106.3,
            delete_timestamp=107.9,
            status_changed_at=108.3,
            container_count='3',
            object_count='20',
            bytes_used='42')
        info['hash'] = 'abaddeadbeefcafe'
        info['id'] = 'abadf100d0ddba11'
        md = {'x-account-meta-mydata': ('swift', '0000000000.00000'),
              'x-other-something': ('boo', '0000000000.00000')}
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_db_info_metadata('account', info, md)
        exp_out = '''Path: /acct
  Account: acct
  Account Hash: dc5be2aa4347a22a0fee6bc7de505b47
Metadata:
  Created at: 1970-01-01T00:01:40.100000 (100.1)
  Put Timestamp: 1970-01-01T00:01:46.300000 (106.3)
  Delete Timestamp: 1970-01-01T00:01:47.900000 (107.9)
  Status Timestamp: 1970-01-01T00:01:48.300000 (108.3)
  Container Count: 3
  Object Count: 20
  Bytes Used: 42
  Chexor: abaddeadbeefcafe
  UUID: abadf100d0ddba11
  X-Other-Something: boo
No system metadata found in db file
  User Metadata: {'mydata': 'swift'}'''

        self.assertEquals(sorted(out.getvalue().strip().split('\n')),
                          sorted(exp_out.split('\n')))

        info = dict(
            account='acct',
            container='cont',
            storage_policy_index=0,
            created_at='0000000100.10000',
            put_timestamp='0000000106.30000',
            delete_timestamp='0000000107.90000',
            status_changed_at='0000000108.30000',
            object_count='20',
            bytes_used='42',
            reported_put_timestamp='0000010106.30000',
            reported_delete_timestamp='0000010107.90000',
            reported_object_count='20',
            reported_bytes_used='42',
            x_container_foo='bar',
            x_container_bar='goo')
        info['hash'] = 'abaddeadbeefcafe'
        info['id'] = 'abadf100d0ddba11'
        md = {'x-container-sysmeta-mydata': ('swift', '0000000000.00000')}
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_db_info_metadata('container', info, md)
        exp_out = '''Path: /acct/cont
  Account: acct
  Container: cont
  Container Hash: d49d0ecbb53be1fcc49624f2f7c7ccae
Metadata:
  Created at: 1970-01-01T00:01:40.100000 (0000000100.10000)
  Put Timestamp: 1970-01-01T00:01:46.300000 (0000000106.30000)
  Delete Timestamp: 1970-01-01T00:01:47.900000 (0000000107.90000)
  Status Timestamp: 1970-01-01T00:01:48.300000 (0000000108.30000)
  Object Count: 20
  Bytes Used: 42
  Storage Policy: %s (0)
  Reported Put Timestamp: 1970-01-01T02:48:26.300000 (0000010106.30000)
  Reported Delete Timestamp: 1970-01-01T02:48:27.900000 (0000010107.90000)
  Reported Object Count: 20
  Reported Bytes Used: 42
  Chexor: abaddeadbeefcafe
  UUID: abadf100d0ddba11
  X-Container-Bar: goo
  X-Container-Foo: bar
  System Metadata: {'mydata': 'swift'}
No user metadata found in db file''' % POLICIES[0].name
        self.assertEquals(sorted(out.getvalue().strip().split('\n')),
                          sorted(exp_out.split('\n')))

    def test_print_ring_locations_invalid_args(self):
        self.assertRaises(ValueError, print_ring_locations,
                          None, 'dir', 'acct')
        self.assertRaises(ValueError, print_ring_locations,
                          [], None, 'acct')
        self.assertRaises(ValueError, print_ring_locations,
                          [], 'dir', None)
        self.assertRaises(ValueError, print_ring_locations,
                          [], 'dir', 'acct', 'con')
        self.assertRaises(ValueError, print_ring_locations,
                          [], 'dir', 'acct', obj='o')

    def test_print_ring_locations_account(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            acctring = ring.Ring(self.testdir, ring_name='account')
            print_ring_locations(acctring, 'dir', 'acct')
        exp_db = os.path.join('${DEVICE:-/srv/node*}', 'sdb1', 'dir', '3',
                              'b47', 'dc5be2aa4347a22a0fee6bc7de505b47')
        self.assertTrue(exp_db in out.getvalue())
        self.assertTrue('127.0.0.1' in out.getvalue())
        self.assertTrue('127.0.0.2' in out.getvalue())

    def test_print_ring_locations_container(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            contring = ring.Ring(self.testdir, ring_name='container')
            print_ring_locations(contring, 'dir', 'acct', 'con')
        exp_db = os.path.join('${DEVICE:-/srv/node*}', 'sdb1', 'dir', '1',
                              'fe6', '63e70955d78dfc62821edc07d6ec1fe6')
        self.assertTrue(exp_db in out.getvalue())

    def test_print_ring_locations_obj(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            objring = ring.Ring(self.testdir, ring_name='object')
            print_ring_locations(objring, 'dir', 'acct', 'con', 'obj')
        exp_obj = os.path.join('${DEVICE:-/srv/node*}', 'sda1', 'dir', '1',
                               '117', '4a16154fc15c75e26ba6afadf5b1c117')
        self.assertTrue(exp_obj in out.getvalue())

    def test_print_ring_locations_partition_number(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            objring = ring.Ring(self.testdir, ring_name='object')
            print_ring_locations(objring, 'objects', None, tpart='1')
        exp_obj1 = os.path.join('${DEVICE:-/srv/node*}', 'sda1',
                                'objects', '1')
        exp_obj2 = os.path.join('${DEVICE:-/srv/node*}', 'sdb1',
                                'objects', '1')
        self.assertTrue(exp_obj1 in out.getvalue())
        self.assertTrue(exp_obj2 in out.getvalue())

    def test_print_info(self):
        db_file = 'foo'
        self.assertRaises(InfoSystemExit, print_info, 'object', db_file)
        db_file = os.path.join(self.testdir, './acct.db')
        self.assertRaises(InfoSystemExit, print_info, 'account', db_file)

        controller = AccountController(
            {'devices': self.testdir, 'mount_check': 'false'})
        req = Request.blank('/sda1/1/acct', environ={'REQUEST_METHOD': 'PUT',
                                                     'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(controller)
        self.assertEqual(resp.status_int, 201)
        out = StringIO()
        exp_raised = False
        with mock.patch('sys.stdout', out):
            db_file = os.path.join(self.testdir, 'sda1', 'accounts',
                                   '1', 'b47',
                                   'dc5be2aa4347a22a0fee6bc7de505b47',
                                   'dc5be2aa4347a22a0fee6bc7de505b47.db')
            try:
                print_info('account', db_file, swift_dir=self.testdir)
            except Exception:
                exp_raised = True
        if exp_raised:
            self.fail("Unexpected exception raised")
        else:
            self.assertTrue(len(out.getvalue().strip()) > 800)

        controller = ContainerController(
            {'devices': self.testdir, 'mount_check': 'false'})
        req = Request.blank('/sda1/1/acct/cont',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(controller)
        self.assertEqual(resp.status_int, 201)
        out = StringIO()
        exp_raised = False
        with mock.patch('sys.stdout', out):
            db_file = os.path.join(self.testdir, 'sda1', 'containers',
                                   '1', 'cae',
                                   'd49d0ecbb53be1fcc49624f2f7c7ccae',
                                   'd49d0ecbb53be1fcc49624f2f7c7ccae.db')
            orig_cwd = os.getcwd()
            try:
                os.chdir(os.path.dirname(db_file))
                print_info('container', os.path.basename(db_file),
                           swift_dir='/dev/null')
            except Exception:
                exp_raised = True
            finally:
                os.chdir(orig_cwd)
        if exp_raised:
            self.fail("Unexpected exception raised")
        else:
            self.assertTrue(len(out.getvalue().strip()) > 600)

        out = StringIO()
        exp_raised = False
        with mock.patch('sys.stdout', out):
            db_file = os.path.join(self.testdir, 'sda1', 'containers',
                                   '1', 'cae',
                                   'd49d0ecbb53be1fcc49624f2f7c7ccae',
                                   'd49d0ecbb53be1fcc49624f2f7c7ccae.db')
            orig_cwd = os.getcwd()
            try:
                os.chdir(os.path.dirname(db_file))
                print_info('account', os.path.basename(db_file),
                           swift_dir='/dev/null')
            except InfoSystemExit:
                exp_raised = True
            finally:
                os.chdir(orig_cwd)
        if exp_raised:
            exp_out = 'Does not appear to be a DB of type "account":' \
                ' ./d49d0ecbb53be1fcc49624f2f7c7ccae.db'
            self.assertEquals(out.getvalue().strip(), exp_out)
        else:
            self.fail("Expected an InfoSystemExit exception to be raised")


class TestPrintObj(TestCliInfoBase):

    def setUp(self):
        super(TestPrintObj, self).setUp()
        self.datafile = os.path.join(self.testdir,
                                     '1402017432.46642.data')
        with open(self.datafile, 'wb') as fp:
            md = {'name': '/AUTH_admin/c/obj',
                  'Content-Type': 'application/octet-stream'}
            write_metadata(fp, md)

    def test_print_obj_invalid(self):
        datafile = '1402017324.68634.data'
        self.assertRaises(InfoSystemExit, print_obj, datafile)
        datafile = os.path.join(self.testdir, './1234.data')
        self.assertRaises(InfoSystemExit, print_obj, datafile)

        with open(datafile, 'wb') as fp:
            fp.write('1234')

        out = StringIO()
        with mock.patch('sys.stdout', out):
            self.assertRaises(InfoSystemExit, print_obj, datafile)
            self.assertEquals(out.getvalue().strip(),
                              'Invalid metadata')

    def test_print_obj_valid(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=self.testdir)
        etag_msg = 'ETag: Not found in metadata'
        length_msg = 'Content-Length: Not found in metadata'
        self.assertTrue(etag_msg in out.getvalue())
        self.assertTrue(length_msg in out.getvalue())

    def test_print_obj_with_policy(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=self.testdir, policy_name='one')
        etag_msg = 'ETag: Not found in metadata'
        length_msg = 'Content-Length: Not found in metadata'
        ring_loc_msg = 'ls -lah'
        self.assertTrue(etag_msg in out.getvalue())
        self.assertTrue(length_msg in out.getvalue())
        self.assertTrue(ring_loc_msg in out.getvalue())

    def test_missing_etag(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile)
        self.assertTrue('ETag: Not found in metadata' in out.getvalue())


class TestPrintObjFullMeta(TestCliInfoBase):
    def setUp(self):
        super(TestPrintObjFullMeta, self).setUp()
        self.datafile = os.path.join(self.testdir,
                                     'sda', 'objects-1',
                                     '1', 'ea8',
                                     'db4449e025aca992307c7c804a67eea8',
                                     '1402017884.18202.data')
        utils.mkdirs(os.path.dirname(self.datafile))
        with open(self.datafile, 'wb') as fp:
            md = {'name': '/AUTH_admin/c/obj',
                  'Content-Type': 'application/octet-stream',
                  'ETag': 'd41d8cd98f00b204e9800998ecf8427e',
                  'Content-Length': 0}
            write_metadata(fp, md)

    def test_print_obj(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=self.testdir)
        self.assertTrue('/objects-1/' in out.getvalue())

    def test_print_obj_meta_and_ts_files(self):
        # verify that print_obj will also read from meta and ts files
        base = os.path.splitext(self.datafile)[0]
        for ext in ('.meta', '.ts'):
            test_file = '%s%s' % (base, ext)
            os.link(self.datafile, test_file)
            out = StringIO()
            with mock.patch('sys.stdout', out):
                print_obj(test_file, swift_dir=self.testdir)
            self.assertTrue('/objects-1/' in out.getvalue())

    def test_print_obj_no_ring(self):
        no_rings_dir = os.path.join(self.testdir, 'no_rings_here')
        os.mkdir(no_rings_dir)

        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=no_rings_dir)
        self.assertTrue('d41d8cd98f00b204e9800998ecf8427e' in out.getvalue())
        self.assertTrue('Partition' not in out.getvalue())

    def test_print_obj_policy_name_mismatch(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, policy_name='two', swift_dir=self.testdir)
        ring_alert_msg = 'Attention: Ring does not match policy'
        self.assertTrue(ring_alert_msg in out.getvalue())

    def test_valid_etag(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile)
        self.assertTrue('ETag: d41d8cd98f00b204e9800998ecf8427e (valid)'
                        in out.getvalue())

    def test_invalid_etag(self):
        with open(self.datafile, 'wb') as fp:
            md = {'name': '/AUTH_admin/c/obj',
                  'Content-Type': 'application/octet-stream',
                  'ETag': 'badetag',
                  'Content-Length': 0}
            write_metadata(fp, md)

        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile)
        self.assertTrue('ETag: badetag doesn\'t match file hash'
                        in out.getvalue())

    def test_unchecked_etag(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, check_etag=False)
        self.assertTrue('ETag: d41d8cd98f00b204e9800998ecf8427e (not checked)'
                        in out.getvalue())

    def test_print_obj_metadata(self):
        self.assertRaisesMessage(ValueError, 'Metadata is None',
                                 print_obj_metadata, [])

        def get_metadata(items):
            md = dict(name='/AUTH_admin/c/dummy')
            md['Content-Type'] = 'application/octet-stream'
            md['X-Timestamp'] = 106.3
            md.update(items)
            return md

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEquals(out.getvalue().strip(), exp_out)

        metadata = get_metadata({
            'X-Object-Sysmeta-Mtime': '107.3',
            'X-Object-Sysmeta-Name': 'Obj name',
        })
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  X-Object-Sysmeta-Mtime: 107.3
  X-Object-Sysmeta-Name: Obj name
User Metadata:
  No metadata found
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEquals(out.getvalue().strip(), exp_out)

        metadata = get_metadata({
            'X-Object-Meta-Mtime': '107.3',
            'X-Object-Sysmeta-Mtime': '107.3',
            'X-Object-Mtime': '107.3',
        })
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  X-Object-Sysmeta-Mtime: 107.3
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  X-Object-Mtime: 107.3''' % (
            utils.Timestamp(106.3).internal)

        self.assertEquals(out.getvalue().strip(), exp_out)

        metadata = get_metadata({})
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  No metadata found
User Metadata:
  No metadata found
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEquals(out.getvalue().strip(), exp_out)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        metadata['name'] = '/a-s'
        self.assertRaisesMessage(ValueError, 'Path is invalid',
                                 print_obj_metadata, metadata)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        del metadata['name']
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: Not found in metadata
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEquals(out.getvalue().strip(), exp_out)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        del metadata['Content-Type']
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: Not found in metadata
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEquals(out.getvalue().strip(), exp_out)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        del metadata['X-Timestamp']
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: Not found in metadata
System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  No metadata found'''

        self.assertEquals(out.getvalue().strip(), exp_out)
