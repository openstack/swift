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

from test.unit import write_fake_ring
from swift.common import ring, utils
from swift.common.swob import Request
from swift.cli.info import print_db_info_metadata, print_ring_locations, \
    print_info, InfoSystemExit
from swift.account.server import AccountController
from swift.container.server import ContainerController


class TestCliInfo(unittest.TestCase):

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
        account_devs = [{'ip': '127.0.0.1', 'port': 42},
                        {'ip': '127.0.0.2', 'port': 43}]
        write_fake_ring(self.account_ring_path, *account_devs)
        self.container_ring_path = os.path.join(self.testdir,
                                                'container.ring.gz')
        container_devs = [{'ip': '127.0.0.3', 'port': 42},
                          {'ip': '127.0.0.4', 'port': 43}]
        write_fake_ring(self.container_ring_path, *container_devs)

    def tearDown(self):
        utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX = self.orig_hp
        rmtree(os.path.dirname(self.testdir))

    def assertRaisesMessage(self, exc, msg, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception, e:
            self.assertEqual(msg, str(e))
            self.assertTrue(isinstance(e, exc),
                            "Expected %s, got %s" % (exc, type(e)))

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
  Created at: 1970-01-01 00:01:40.100000 (100.1)
  Put Timestamp: 1970-01-01 00:01:46.300000 (106.3)
  Delete Timestamp: 1970-01-01 00:01:47.900000 (107.9)
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
            created_at='0000000100.10000',
            put_timestamp='0000000106.30000',
            delete_timestamp='0000000107.90000',
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
  Created at: 1970-01-01 00:01:40.100000 (0000000100.10000)
  Put Timestamp: 1970-01-01 00:01:46.300000 (0000000106.30000)
  Delete Timestamp: 1970-01-01 00:01:47.900000 (0000000107.90000)
  Object Count: 20
  Bytes Used: 42
  Reported Put Timestamp: 1970-01-01 02:48:26.300000 (0000010106.30000)
  Reported Delete Timestamp: 1970-01-01 02:48:27.900000 (0000010107.90000)
  Reported Object Count: 20
  Reported Bytes Used: 42
  Chexor: abaddeadbeefcafe
  UUID: abadf100d0ddba11
  X-Container-Bar: goo
  X-Container-Foo: bar
  System Metadata: {'mydata': 'swift'}
No user metadata found in db file'''
        self.assertEquals(sorted(out.getvalue().strip().split('\n')),
                          sorted(exp_out.split('\n')))

    def test_print_ring_locations(self):
        self.assertRaisesMessage(ValueError, 'None type', print_ring_locations,
                                 None, 'dir', 'acct')
        self.assertRaisesMessage(ValueError, 'None type', print_ring_locations,
                                 [], None, 'acct')
        self.assertRaisesMessage(ValueError, 'None type', print_ring_locations,
                                 [], 'dir', None)
        self.assertRaisesMessage(ValueError, 'Ring error',
                                 print_ring_locations,
                                 [], 'dir', 'acct', 'con')

        out = StringIO()
        with mock.patch('sys.stdout', out):
            acctring = ring.Ring(self.testdir, ring_name='account')
            print_ring_locations(acctring, 'dir', 'acct')
        exp_db2 = os.path.join('/srv', 'node', 'sdb1', 'dir', '3', 'b47',
                               'dc5be2aa4347a22a0fee6bc7de505b47',
                               'dc5be2aa4347a22a0fee6bc7de505b47.db')
        exp_db1 = os.path.join('/srv', 'node', 'sda1', 'dir', '3', 'b47',
                               'dc5be2aa4347a22a0fee6bc7de505b47',
                               'dc5be2aa4347a22a0fee6bc7de505b47.db')
        exp_out = ('Ring locations:\n  127.0.0.2:43 - %s\n'
                   '  127.0.0.1:42 - %s\n'
                   '\nnote: /srv/node is used as default value of `devices`,'
                   ' the real value is set in the account config file on'
                   ' each storage node.' % (exp_db2, exp_db1))
        self.assertEquals(out.getvalue().strip(), exp_out)

        out = StringIO()
        with mock.patch('sys.stdout', out):
            contring = ring.Ring(self.testdir, ring_name='container')
            print_ring_locations(contring, 'dir', 'acct', 'con')
        exp_db4 = os.path.join('/srv', 'node', 'sdb1', 'dir', '1', 'fe6',
                               '63e70955d78dfc62821edc07d6ec1fe6',
                               '63e70955d78dfc62821edc07d6ec1fe6.db')
        exp_db3 = os.path.join('/srv', 'node', 'sda1', 'dir', '1', 'fe6',
                               '63e70955d78dfc62821edc07d6ec1fe6',
                               '63e70955d78dfc62821edc07d6ec1fe6.db')
        exp_out = ('Ring locations:\n  127.0.0.4:43 - %s\n'
                   '  127.0.0.3:42 - %s\n'
                   '\nnote: /srv/node is used as default value of `devices`,'
                   ' the real value is set in the container config file on'
                   ' each storage node.' % (exp_db4, exp_db3))
        self.assertEquals(out.getvalue().strip(), exp_out)

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
