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

from argparse import Namespace
import os
import unittest
import mock
from shutil import rmtree
from tempfile import mkdtemp

import six
from six.moves import cStringIO as StringIO
from test.unit import patch_policies, write_fake_ring, skip_if_no_xattrs

from swift.common import ring, utils
from swift.common.swob import Request
from swift.common.storage_policy import StoragePolicy, POLICIES
from swift.cli.info import (print_db_info_metadata, print_ring_locations,
                            print_info, print_obj_metadata, print_obj,
                            InfoSystemExit, print_item_locations,
                            parse_get_node_args)
from swift.account.server import AccountController
from swift.container.server import ContainerController
from swift.container.backend import UNSHARDED, SHARDED
from swift.obj.diskfile import write_metadata


@patch_policies([StoragePolicy(0, 'zero', True),
                 StoragePolicy(1, 'one', False),
                 StoragePolicy(2, 'two', False),
                 StoragePolicy(3, 'three', False)])
class TestCliInfoBase(unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()
        self.orig_hp = utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX
        utils.HASH_PATH_PREFIX = b'info'
        utils.HASH_PATH_SUFFIX = b'info'
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
        # ... and one for policy 3 with some v6 IPs in it
        object_devs_ipv6 = [
            {'ip': 'feed:face::dead:beef', 'port': 42},
            {'ip': 'deca:fc0f:feeb:ad11::1', 'port': 43}
        ]
        self.three_ring_path = os.path.join(self.testdir, 'object-3.ring.gz')
        write_fake_ring(self.three_ring_path, *object_devs_ipv6)

    def tearDown(self):
        utils.HASH_PATH_PREFIX, utils.HASH_PATH_SUFFIX = self.orig_hp
        rmtree(os.path.dirname(self.testdir))

    def assertRaisesMessage(self, exc, msg, func, *args, **kwargs):
        with self.assertRaises(exc) as ctx:
            func(*args, **kwargs)
        self.assertIn(msg, str(ctx.exception))


class TestCliInfo(TestCliInfoBase):
    def test_print_db_info_metadata(self):
        self.assertRaisesMessage(ValueError, 'Wrong DB type',
                                 print_db_info_metadata, 't', {}, {})
        self.assertRaisesMessage(ValueError, 'DB info is None',
                                 print_db_info_metadata, 'container', None, {})
        self.assertRaisesMessage(ValueError, 'Info is incomplete',
                                 print_db_info_metadata, 'container', {}, {})

        info = {
            'account': 'acct',
            'created_at': 100.1,
            'put_timestamp': 106.3,
            'delete_timestamp': 107.9,
            'status_changed_at': 108.3,
            'container_count': '3',
            'object_count': '20',
            'bytes_used': '42',
            'hash': 'abaddeadbeefcafe',
            'id': 'abadf100d0ddba11',
        }
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
  User Metadata: {'x-account-meta-mydata': 'swift'}'''

        self.assertEqual(sorted(out.getvalue().strip().split('\n')),
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
            x_container_bar='goo',
            db_state=UNSHARDED,
            is_root=True)
        info['hash'] = 'abaddeadbeefcafe'
        info['id'] = 'abadf100d0ddba11'
        md = {'x-container-sysmeta-mydata': ('swift', '0000000000.00000')}
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_db_info_metadata('container', info, md, True)
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
No user metadata found in db file
Sharding Metadata:
  Type: root
  State: unsharded''' % POLICIES[0].name
        self.assertEqual(sorted(out.getvalue().strip().split('\n')),
                         sorted(exp_out.split('\n')))

    def test_print_db_info_metadata_with_shard_ranges(self):

        shard_ranges = [utils.ShardRange(
            name='.sharded_a/shard_range_%s' % i,
            timestamp=utils.Timestamp(i), lower='%da' % i,
            upper='%dz' % i, object_count=i, bytes_used=i,
            meta_timestamp=utils.Timestamp(i)) for i in range(1, 4)]
        shard_ranges[0].state = utils.ShardRange.CLEAVED
        shard_ranges[1].state = utils.ShardRange.CREATED

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
            db_state=SHARDED,
            is_root=True,
            shard_ranges=shard_ranges)
        info['hash'] = 'abaddeadbeefcafe'
        info['id'] = 'abadf100d0ddba11'
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_db_info_metadata('container', info, {})
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
No system metadata found in db file
No user metadata found in db file
Sharding Metadata:
  Type: root
  State: sharded
Shard Ranges (3):
  Name: .sharded_a/shard_range_1
    lower: '1a', upper: '1z'
    Object Count: 1, Bytes Used: 1, State: cleaved (30)
    Created at: 1970-01-01T00:00:01.000000 (0000000001.00000)
    Meta Timestamp: 1970-01-01T00:00:01.000000 (0000000001.00000)
  Name: .sharded_a/shard_range_2
    lower: '2a', upper: '2z'
    Object Count: 2, Bytes Used: 2, State: created (20)
    Created at: 1970-01-01T00:00:02.000000 (0000000002.00000)
    Meta Timestamp: 1970-01-01T00:00:02.000000 (0000000002.00000)
  Name: .sharded_a/shard_range_3
    lower: '3a', upper: '3z'
    Object Count: 3, Bytes Used: 3, State: found (10)
    Created at: 1970-01-01T00:00:03.000000 (0000000003.00000)
    Meta Timestamp: 1970-01-01T00:00:03.000000 (0000000003.00000)''' %\
                  POLICIES[0].name
        self.assertEqual(sorted(out.getvalue().strip().split('\n')),
                         sorted(exp_out.strip().split('\n')))

    def test_print_db_info_metadata_with_shard_ranges_bis(self):

        shard_ranges = [utils.ShardRange(
            name='.sharded_a/shard_range_%s' % i,
            timestamp=utils.Timestamp(i), lower=u'%d\u30a2' % i,
            upper=u'%d\u30e4' % i, object_count=i, bytes_used=i,
            meta_timestamp=utils.Timestamp(i)) for i in range(1, 4)]
        shard_ranges[0].state = utils.ShardRange.CLEAVED
        shard_ranges[1].state = utils.ShardRange.CREATED

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
            db_state=SHARDED,
            is_root=True,
            shard_ranges=shard_ranges)
        info['hash'] = 'abaddeadbeefcafe'
        info['id'] = 'abadf100d0ddba11'
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_db_info_metadata('container', info, {})
        if six.PY2:
            s_a = '\\xe3\\x82\\xa2'
            s_ya = '\\xe3\\x83\\xa4'
        else:
            s_a = '\u30a2'
            s_ya = '\u30e4'
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
No system metadata found in db file
No user metadata found in db file
Sharding Metadata:
  Type: root
  State: sharded
Shard Ranges (3):
  Name: .sharded_a/shard_range_1
    lower: '1%s', upper: '1%s'
    Object Count: 1, Bytes Used: 1, State: cleaved (30)
    Created at: 1970-01-01T00:00:01.000000 (0000000001.00000)
    Meta Timestamp: 1970-01-01T00:00:01.000000 (0000000001.00000)
  Name: .sharded_a/shard_range_2
    lower: '2%s', upper: '2%s'
    Object Count: 2, Bytes Used: 2, State: created (20)
    Created at: 1970-01-01T00:00:02.000000 (0000000002.00000)
    Meta Timestamp: 1970-01-01T00:00:02.000000 (0000000002.00000)
  Name: .sharded_a/shard_range_3
    lower: '3%s', upper: '3%s'
    Object Count: 3, Bytes Used: 3, State: found (10)
    Created at: 1970-01-01T00:00:03.000000 (0000000003.00000)
    Meta Timestamp: 1970-01-01T00:00:03.000000 (0000000003.00000)''' %\
                  (POLICIES[0].name, s_a, s_ya, s_a, s_ya, s_a, s_ya)
        self.assertEqual(out.getvalue().strip().split('\n'),
                         exp_out.strip().split('\n'))

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
        self.assertIn(exp_db, out.getvalue())
        self.assertIn('127.0.0.1', out.getvalue())
        self.assertIn('127.0.0.2', out.getvalue())

    def test_print_ring_locations_container(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            contring = ring.Ring(self.testdir, ring_name='container')
            print_ring_locations(contring, 'dir', 'acct', 'con')
        exp_db = os.path.join('${DEVICE:-/srv/node*}', 'sdb1', 'dir', '1',
                              'fe6', '63e70955d78dfc62821edc07d6ec1fe6')
        self.assertIn(exp_db, out.getvalue())

    def test_print_ring_locations_obj(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            objring = ring.Ring(self.testdir, ring_name='object')
            print_ring_locations(objring, 'dir', 'acct', 'con', 'obj')
        exp_obj = os.path.join('${DEVICE:-/srv/node*}', 'sda1', 'dir', '1',
                               '117', '4a16154fc15c75e26ba6afadf5b1c117')
        self.assertIn(exp_obj, out.getvalue())

    def test_print_ring_locations_partition_number(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            objring = ring.Ring(self.testdir, ring_name='object')
            print_ring_locations(objring, 'objects', None, tpart='1')
        exp_obj1 = os.path.join('${DEVICE:-/srv/node*}', 'sda1',
                                'objects', '1')
        exp_obj2 = os.path.join('${DEVICE:-/srv/node*}', 'sdb1',
                                'objects', '1')
        self.assertIn(exp_obj1, out.getvalue())
        self.assertIn(exp_obj2, out.getvalue())

    def test_print_item_locations_invalid_args(self):
        # No target specified
        self.assertRaises(InfoSystemExit, print_item_locations,
                          None)
        # Need a ring or policy
        self.assertRaises(InfoSystemExit, print_item_locations,
                          None, account='account', obj='object')
        # No account specified
        self.assertRaises(InfoSystemExit, print_item_locations,
                          None, container='con')
        # No policy named 'xyz' (unrecognized policy)
        self.assertRaises(InfoSystemExit, print_item_locations,
                          None, obj='object', policy_name='xyz')
        # No container specified
        objring = ring.Ring(self.testdir, ring_name='object')
        self.assertRaises(InfoSystemExit, print_item_locations,
                          objring, account='account', obj='object')

    def test_print_item_locations_ring_policy_mismatch_no_target(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            objring = ring.Ring(self.testdir, ring_name='object')
            # Test mismatch of ring and policy name (valid policy)
            self.assertRaises(InfoSystemExit, print_item_locations,
                              objring, policy_name='zero')
        self.assertIn('Warning: mismatch between ring and policy name!',
                      out.getvalue())
        self.assertIn('No target specified', out.getvalue())

    def test_print_item_locations_invalid_policy_no_target(self):
        out = StringIO()
        policy_name = 'nineteen'
        with mock.patch('sys.stdout', out):
            objring = ring.Ring(self.testdir, ring_name='object')
            self.assertRaises(InfoSystemExit, print_item_locations,
                              objring, policy_name=policy_name)
        exp_msg = 'Warning: Policy %s is not valid' % policy_name
        self.assertIn(exp_msg, out.getvalue())
        self.assertIn('No target specified', out.getvalue())

    def test_print_item_locations_policy_object(self):
        out = StringIO()
        part = '1'
        with mock.patch('sys.stdout', out):
            print_item_locations(None, partition=part, policy_name='zero',
                                 swift_dir=self.testdir)
        exp_part_msg = 'Partition\t%s' % part
        exp_acct_msg = 'Account  \tNone'
        exp_cont_msg = 'Container\tNone'
        exp_obj_msg = 'Object   \tNone'
        self.assertIn(exp_part_msg, out.getvalue())
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_dashed_ring_name_partition(self):
        out = StringIO()
        part = '1'
        with mock.patch('sys.stdout', out):
            print_item_locations(None, policy_name='one',
                                 ring_name='foo-bar', partition=part,
                                 swift_dir=self.testdir)
        exp_part_msg = 'Partition\t%s' % part
        exp_acct_msg = 'Account  \tNone'
        exp_cont_msg = 'Container\tNone'
        exp_obj_msg = 'Object   \tNone'
        self.assertIn(exp_part_msg, out.getvalue())
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_account_with_ring(self):
        out = StringIO()
        account = 'account'
        with mock.patch('sys.stdout', out):
            account_ring = ring.Ring(self.testdir, ring_name=account)
            print_item_locations(account_ring, account=account)
        exp_msg = 'Account  \t%s' % account
        self.assertIn(exp_msg, out.getvalue())
        exp_warning = 'Warning: account specified ' + \
                      'but ring not named "account"'
        self.assertIn(exp_warning, out.getvalue())
        exp_acct_msg = 'Account  \t%s' % account
        exp_cont_msg = 'Container\tNone'
        exp_obj_msg = 'Object   \tNone'
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_account_no_ring(self):
        out = StringIO()
        account = 'account'
        with mock.patch('sys.stdout', out):
            print_item_locations(None, account=account,
                                 swift_dir=self.testdir)
        exp_acct_msg = 'Account  \t%s' % account
        exp_cont_msg = 'Container\tNone'
        exp_obj_msg = 'Object   \tNone'
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_account_container_ring(self):
        out = StringIO()
        account = 'account'
        container = 'container'
        with mock.patch('sys.stdout', out):
            container_ring = ring.Ring(self.testdir, ring_name='container')
            print_item_locations(container_ring, account=account,
                                 container=container)
        exp_acct_msg = 'Account  \t%s' % account
        exp_cont_msg = 'Container\t%s' % container
        exp_obj_msg = 'Object   \tNone'
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_account_container_no_ring(self):
        out = StringIO()
        account = 'account'
        container = 'container'
        with mock.patch('sys.stdout', out):
            print_item_locations(None, account=account,
                                 container=container, swift_dir=self.testdir)
        exp_acct_msg = 'Account  \t%s' % account
        exp_cont_msg = 'Container\t%s' % container
        exp_obj_msg = 'Object   \tNone'
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_account_container_object_ring(self):
        out = StringIO()
        account = 'account'
        container = 'container'
        obj = 'object'
        with mock.patch('sys.stdout', out):
            object_ring = ring.Ring(self.testdir, ring_name='object')
            print_item_locations(object_ring, ring_name='object',
                                 account=account, container=container,
                                 obj=obj)
        exp_acct_msg = 'Account  \t%s' % account
        exp_cont_msg = 'Container\t%s' % container
        exp_obj_msg = 'Object   \t%s' % obj
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

    def test_print_item_locations_account_container_object_dashed_ring(self):
        out = StringIO()
        account = 'account'
        container = 'container'
        obj = 'object'
        with mock.patch('sys.stdout', out):
            object_ring = ring.Ring(self.testdir, ring_name='object-1')
            print_item_locations(object_ring, ring_name='object-1',
                                 account=account, container=container,
                                 obj=obj)
        exp_acct_msg = 'Account  \t%s' % account
        exp_cont_msg = 'Container\t%s' % container
        exp_obj_msg = 'Object   \t%s' % obj
        self.assertIn(exp_acct_msg, out.getvalue())
        self.assertIn(exp_cont_msg, out.getvalue())
        self.assertIn(exp_obj_msg, out.getvalue())

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
            print_info('account', db_file, swift_dir=self.testdir)
        self.assertGreater(len(out.getvalue().strip()), 800)

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
            self.assertGreater(len(out.getvalue().strip()), 600)

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
            self.assertEqual(out.getvalue().strip(), exp_out)
        else:
            self.fail("Expected an InfoSystemExit exception to be raised")

    def test_parse_get_node_args(self):
        # Capture error messages
        # (without any parameters)
        options = Namespace(policy_name=None, partition=None)
        args = ''
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())
        # a
        options = Namespace(policy_name=None, partition=None)
        args = 'a'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())
        # a c
        options = Namespace(policy_name=None, partition=None)
        args = 'a c'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())
        # a c o
        options = Namespace(policy_name=None, partition=None)
        args = 'a c o'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())

        # a/c
        options = Namespace(policy_name=None, partition=None)
        args = 'a/c'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())
        # a/c/o
        options = Namespace(policy_name=None, partition=None)
        args = 'a/c/o'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())

        # account container junk/test.ring.gz
        options = Namespace(policy_name=None, partition=None)
        args = 'account container junk/test.ring.gz'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())

        # account container object junk/test.ring.gz
        options = Namespace(policy_name=None, partition=None)
        args = 'account container object junk/test.ring.gz'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Need to specify policy_name or <ring.gz>',
                                 parse_get_node_args, options, args.split())

        # object.ring.gz(without any arguments i.e. a c o)
        options = Namespace(policy_name=None, partition=None)
        args = 'object.ring.gz'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Ring file does not exist',
                                 parse_get_node_args, options, args.split())

        # Valid policy
        # -P zero
        options = Namespace(policy_name='zero', partition=None)
        args = ''
        self.assertRaisesMessage(InfoSystemExit,
                                 'No target specified',
                                 parse_get_node_args, options, args.split())
        # -P one a/c/o
        options = Namespace(policy_name='one', partition=None)
        args = 'a/c/o'
        ring_path, args = parse_get_node_args(options, args.split())
        self.assertIsNone(ring_path)
        self.assertEqual(args, ['a', 'c', 'o'])
        # -P one account container photos/cat.jpg
        options = Namespace(policy_name='one', partition=None)
        args = 'account container photos/cat.jpg'
        ring_path, args = parse_get_node_args(options, args.split())
        self.assertIsNone(ring_path)
        self.assertEqual(args, ['account', 'container', 'photos/cat.jpg'])
        # -P one account/container/photos/cat.jpg
        options = Namespace(policy_name='one', partition=None)
        args = 'account/container/photos/cat.jpg'
        ring_path, args = parse_get_node_args(options, args.split())
        self.assertIsNone(ring_path)
        self.assertEqual(args, ['account', 'container', 'photos/cat.jpg'])
        # -P one account/container/junk/test.ring.gz(object endswith 'ring.gz')
        options = Namespace(policy_name='one', partition=None)
        args = 'account/container/junk/test.ring.gz'
        ring_path, args = parse_get_node_args(options, args.split())
        self.assertIsNone(ring_path)
        self.assertEqual(args, ['account', 'container', 'junk/test.ring.gz'])
        # -P two a c o hooya
        options = Namespace(policy_name='two', partition=None)
        args = 'a c o hooya'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Invalid arguments',
                                 parse_get_node_args, options, args.split())
        # -P zero -p 1
        options = Namespace(policy_name='zero', partition='1')
        args = ''
        ring_path, args = parse_get_node_args(options, args.split())
        self.assertIsNone(ring_path)
        self.assertFalse(args)
        # -P one -p 1 a/c/o
        options = Namespace(policy_name='one', partition='1')
        args = 'a/c/o'
        ring_path, args = parse_get_node_args(options, args.split())
        self.assertIsNone(ring_path)
        self.assertEqual(args, ['a', 'c', 'o'])
        # -P two -p 1 a c o hooya
        options = Namespace(policy_name='two', partition='1')
        args = 'a c o hooya'
        self.assertRaisesMessage(InfoSystemExit,
                                 'Invalid arguments',
                                 parse_get_node_args, options, args.split())

        # Invalid policy
        # -P undefined
        options = Namespace(policy_name='undefined')
        args = ''
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args.split())
        # -P undefined -p 1
        options = Namespace(policy_name='undefined', partition='1')
        args = ''
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args.split())
        # -P undefined a
        options = Namespace(policy_name='undefined')
        args = 'a'
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args.split())
        # -P undefined a c
        options = Namespace(policy_name='undefined')
        args = 'a c'
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args.split())
        # -P undefined a c o
        options = Namespace(policy_name='undefined')
        args = 'a c o'
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args.split())
        # -P undefined a/c
        options = Namespace(policy_name='undefined')
        args = 'a/c'
        # ring_path, args = parse_get_node_args(options, args.split())
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args)
        # -P undefined a/c/o
        options = Namespace(policy_name='undefined')
        args = 'a/c/o'
        # ring_path, args = parse_get_node_args(options, args.split())
        self.assertRaisesMessage(InfoSystemExit,
                                 "No policy named 'undefined'",
                                 parse_get_node_args, options, args)

        # Mock tests
        # /etc/swift/object.ring.gz(without any arguments i.e. a c o)
        options = Namespace(policy_name=None, partition=None)
        args = '/etc/swift/object.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            self.assertRaisesMessage(
                InfoSystemExit,
                'No target specified',
                parse_get_node_args, options, args.split())
        # Similar ring_path and arguments
        # /etc/swift/object.ring.gz /etc/swift/object.ring.gz
        options = Namespace(policy_name=None, partition=None)
        args = '/etc/swift/object.ring.gz /etc/swift/object.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, '/etc/swift/object.ring.gz')
        self.assertEqual(args, ['etc', 'swift', 'object.ring.gz'])
        # /etc/swift/object.ring.gz a/c/etc/swift/object.ring.gz
        options = Namespace(policy_name=None, partition=None)
        args = '/etc/swift/object.ring.gz a/c/etc/swift/object.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, '/etc/swift/object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'etc/swift/object.ring.gz'])
        # Invalid path as mentioned in BUG#1539275
        # /etc/swift/object.tar.gz account container object
        options = Namespace(policy_name=None, partition=None)
        args = '/etc/swift/object.tar.gz account container object'
        self.assertRaisesMessage(
            InfoSystemExit,
            'Need to specify policy_name or <ring.gz>',
            parse_get_node_args, options, args.split())

        # object.ring.gz a/
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a/'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a'])
        # object.ring.gz a/c
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a/c'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c'])
        # object.ring.gz a/c/o
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a/c/o'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o'])
        # object.ring.gz a/c/o/junk/test.ring.gz
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a/c/o/junk/test.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o/junk/test.ring.gz'])
        # object.ring.gz a
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a'])
        # object.ring.gz a c
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a c'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c'])
        # object.ring.gz a c o
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a c o'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o'])
        # object.ring.gz a c o blah blah
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a c o blah blah'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            self.assertRaisesMessage(
                InfoSystemExit,
                'Invalid arguments',
                parse_get_node_args, options, args.split())
        # object.ring.gz a/c/o/blah/blah
        options = Namespace(policy_name=None)
        args = 'object.ring.gz a/c/o/blah/blah'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o/blah/blah'])

        # object.ring.gz -p 1
        options = Namespace(policy_name=None, partition='1')
        args = 'object.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertFalse(args)
        # object.ring.gz -p 1 a c o
        options = Namespace(policy_name=None, partition='1')
        args = 'object.ring.gz a c o'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o'])
        # object.ring.gz -p 1 a c o forth_arg
        options = Namespace(policy_name=None, partition='1')
        args = 'object.ring.gz a c o forth_arg'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            self.assertRaisesMessage(
                InfoSystemExit,
                'Invalid arguments',
                parse_get_node_args, options, args.split())
        # object.ring.gz -p 1 a/c/o
        options = Namespace(policy_name=None, partition='1')
        args = 'object.ring.gz a/c/o'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o'])
        # object.ring.gz -p 1 a/c/junk/test.ring.gz
        options = Namespace(policy_name=None, partition='1')
        args = 'object.ring.gz a/c/junk/test.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'junk/test.ring.gz'])
        # object.ring.gz -p 1 a/c/photos/cat.jpg
        options = Namespace(policy_name=None, partition='1')
        args = 'object.ring.gz a/c/photos/cat.jpg'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'photos/cat.jpg'])

        # --all object.ring.gz a
        options = Namespace(all=True, policy_name=None)
        args = 'object.ring.gz a'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a'])
        # --all object.ring.gz a c
        options = Namespace(all=True, policy_name=None)
        args = 'object.ring.gz a c'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c'])
        # --all object.ring.gz a c o
        options = Namespace(all=True, policy_name=None)
        args = 'object.ring.gz a c o'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['a', 'c', 'o'])
        # object.ring.gz account container photos/cat.jpg
        options = Namespace(policy_name=None, partition=None)
        args = 'object.ring.gz account container photos/cat.jpg'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['account', 'container', 'photos/cat.jpg'])
        # object.ring.gz /account/container/photos/cat.jpg
        options = Namespace(policy_name=None, partition=None)
        args = 'object.ring.gz account/container/photos/cat.jpg'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['account', 'container', 'photos/cat.jpg'])
        # Object name ends with 'ring.gz'
        # object.ring.gz /account/container/junk/test.ring.gz
        options = Namespace(policy_name=None, partition=None)
        args = 'object.ring.gz account/container/junk/test.ring.gz'
        with mock.patch('swift.cli.info.os.path.exists') as exists:
            exists.return_value = True
            ring_path, args = parse_get_node_args(options, args.split())
        self.assertEqual(ring_path, 'object.ring.gz')
        self.assertEqual(args, ['account', 'container', 'junk/test.ring.gz'])


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
            fp.write(b'1234')

        out = StringIO()
        with mock.patch('sys.stdout', out):
            self.assertRaises(InfoSystemExit, print_obj, datafile)
            self.assertEqual(out.getvalue().strip(),
                             'Invalid metadata')

    def test_print_obj_valid(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=self.testdir)
        etag_msg = 'ETag: Not found in metadata'
        length_msg = 'Content-Length: Not found in metadata'
        self.assertIn(etag_msg, out.getvalue())
        self.assertIn(length_msg, out.getvalue())

    def test_print_obj_with_policy(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=self.testdir, policy_name='one')
        etag_msg = 'ETag: Not found in metadata'
        length_msg = 'Content-Length: Not found in metadata'
        ring_loc_msg = 'ls -lah'
        self.assertIn(etag_msg, out.getvalue())
        self.assertIn(length_msg, out.getvalue())
        self.assertIn(ring_loc_msg, out.getvalue())

    def test_missing_etag(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile)
        self.assertIn('ETag: Not found in metadata', out.getvalue())


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
        self.assertIn('/objects-1/', out.getvalue())

    def test_print_obj_policy_index(self):
        # Check an output of policy index when current directory is in
        # object-* directory
        out = StringIO()
        hash_dir = os.path.dirname(self.datafile)
        file_name = os.path.basename(self.datafile)

        # Change working directory to object hash dir
        cwd = os.getcwd()
        try:
            os.chdir(hash_dir)
            with mock.patch('sys.stdout', out):
                print_obj(file_name, swift_dir=self.testdir)
        finally:
            os.chdir(cwd)
        self.assertIn('X-Backend-Storage-Policy-Index: 1', out.getvalue())

    def test_print_obj_curl_command_ipv4(self):
        # Note: policy 2 has IPv4 addresses in its ring
        datafile2 = os.path.join(
            self.testdir,
            'sda', 'objects-2', '1', 'ea8',
            'db4449e025aca992307c7c804a67eea8', '1402017884.18202.data')
        utils.mkdirs(os.path.dirname(datafile2))
        with open(datafile2, 'wb') as fp:
            md = {'name': '/AUTH_admin/c/obj',
                  'Content-Type': 'application/octet-stream',
                  'ETag': 'd41d8cd98f00b204e9800998ecf8427e',
                  'Content-Length': 0}
            write_metadata(fp, md)

        object_ring = ring.Ring(self.testdir, ring_name='object-2')
        part, nodes = object_ring.get_nodes('AUTH_admin', 'c', 'obj')
        node = nodes[0]

        out = StringIO()
        hash_dir = os.path.dirname(datafile2)
        file_name = os.path.basename(datafile2)

        # Change working directory to object hash dir
        cwd = os.getcwd()
        try:
            os.chdir(hash_dir)
            with mock.patch('sys.stdout', out):
                print_obj(file_name, swift_dir=self.testdir)
        finally:
            os.chdir(cwd)

        exp_curl = (
            'curl -g -I -XHEAD '
            '"http://{host}:{port}/{device}/{part}/AUTH_admin/c/obj" '
            '-H "X-Backend-Storage-Policy-Index: 2"').format(
                host=node['ip'],
                port=node['port'],
                device=node['device'],
                part=part)
        self.assertIn(exp_curl, out.getvalue())

    def test_print_obj_curl_command_ipv6(self):
        # Note: policy 3 has IPv6 addresses in its ring
        datafile3 = os.path.join(
            self.testdir,
            'sda', 'objects-3', '1', 'ea8',
            'db4449e025aca992307c7c804a67eea8', '1402017884.18202.data')
        utils.mkdirs(os.path.dirname(datafile3))
        with open(datafile3, 'wb') as fp:
            md = {'name': '/AUTH_admin/c/obj',
                  'Content-Type': 'application/octet-stream',
                  'ETag': 'd41d8cd98f00b204e9800998ecf8427e',
                  'Content-Length': 0}
            write_metadata(fp, md)

        object_ring = ring.Ring(self.testdir, ring_name='object-3')
        part, nodes = object_ring.get_nodes('AUTH_admin', 'c', 'obj')
        node = nodes[0]

        out = StringIO()
        hash_dir = os.path.dirname(datafile3)
        file_name = os.path.basename(datafile3)

        # Change working directory to object hash dir
        cwd = os.getcwd()
        try:
            os.chdir(hash_dir)
            with mock.patch('sys.stdout', out):
                print_obj(file_name, swift_dir=self.testdir)
        finally:
            os.chdir(cwd)

        exp_curl = (
            'curl -g -I -XHEAD '
            '"http://[{host}]:{port}'
            '/{device}/{part}/AUTH_admin/c/obj" ').format(
                host=node['ip'],
                port=node['port'],
                device=node['device'],
                part=part)
        self.assertIn(exp_curl, out.getvalue())

    def test_print_obj_meta_and_ts_files(self):
        # verify that print_obj will also read from meta and ts files
        base = os.path.splitext(self.datafile)[0]
        for ext in ('.meta', '.ts'):
            test_file = '%s%s' % (base, ext)
            os.link(self.datafile, test_file)
            out = StringIO()
            with mock.patch('sys.stdout', out):
                print_obj(test_file, swift_dir=self.testdir)
            self.assertIn('/objects-1/', out.getvalue())

    def test_print_obj_no_ring(self):
        no_rings_dir = os.path.join(self.testdir, 'no_rings_here')
        os.mkdir(no_rings_dir)

        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, swift_dir=no_rings_dir)
        self.assertIn('d41d8cd98f00b204e9800998ecf8427e', out.getvalue())
        self.assertNotIn('Partition', out.getvalue())

    def test_print_obj_policy_name_mismatch(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, policy_name='two', swift_dir=self.testdir)
        ring_alert_msg = 'Warning: Ring does not match policy!'
        self.assertIn(ring_alert_msg, out.getvalue())

    def test_valid_etag(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile)
        self.assertIn('ETag: d41d8cd98f00b204e9800998ecf8427e (valid)',
                      out.getvalue())

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
        self.assertIn('ETag: badetag doesn\'t match file hash',
                      out.getvalue())

    def test_unchecked_etag(self):
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj(self.datafile, check_etag=False)
        self.assertIn('ETag: d41d8cd98f00b204e9800998ecf8427e (not checked)',
                      out.getvalue())

    def test_print_obj_metadata(self):
        self.assertRaisesMessage(ValueError, 'Metadata is None',
                                 print_obj_metadata, [])

        def get_metadata(items):
            md = {
                'name': '/AUTH_admin/c/dummy',
                'Content-Type': 'application/octet-stream',
                'X-Timestamp': 106.3,
            }
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
Transient System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

        metadata = get_metadata({
            'X-Object-Sysmeta-Mtime': '107.3',
            'X-Object-Sysmeta-Name': 'Obj name',
        })
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata, True)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  Mtime: 107.3
  Name: Obj name
Transient System Metadata:
  No metadata found
User Metadata:
  No metadata found
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

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
Transient System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  X-Object-Mtime: 107.3''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

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
Transient System Metadata:
  No metadata found
User Metadata:
  No metadata found
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        metadata['name'] = '/a-s'
        self.assertRaisesMessage(ValueError, 'Path is invalid',
                                 print_obj_metadata, metadata)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        del metadata['name']
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata, True)
        exp_out = '''Path: Not found in metadata
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  No metadata found
Transient System Metadata:
  No metadata found
User Metadata:
  Mtime: 107.3
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

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
Transient System Metadata:
  No metadata found
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  No metadata found''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

        metadata = get_metadata({'X-Object-Meta-Mtime': '107.3'})
        del metadata['X-Timestamp']
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata, True)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: Not found in metadata
System Metadata:
  No metadata found
Transient System Metadata:
  No metadata found
User Metadata:
  Mtime: 107.3
Other Metadata:
  No metadata found'''

        self.assertEqual(out.getvalue().strip(), exp_out)

        metadata = get_metadata({
            'X-Object-Meta-Mtime': '107.3',
            'X-Object-Sysmeta-Mtime': '106.3',
            'X-Object-Transient-Sysmeta-Mtime': '105.3',
            'X-Object-Mtime': '104.3',
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
  X-Object-Sysmeta-Mtime: 106.3
Transient System Metadata:
  X-Object-Transient-Sysmeta-Mtime: 105.3
User Metadata:
  X-Object-Meta-Mtime: 107.3
Other Metadata:
  X-Object-Mtime: 104.3''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

        metadata = get_metadata({
            'X-Object-Meta-Mtime': '107.3',
            'X-Object-Sysmeta-Mtime': '106.3',
            'X-Object-Transient-Sysmeta-Mtime': '105.3',
            'X-Object-Mtime': '104.3',
        })
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(metadata, True)
        exp_out = '''Path: /AUTH_admin/c/dummy
  Account: AUTH_admin
  Container: c
  Object: dummy
  Object hash: 128fdf98bddd1b1e8695f4340e67a67a
Content-Type: application/octet-stream
Timestamp: 1970-01-01T00:01:46.300000 (%s)
System Metadata:
  Mtime: 106.3
Transient System Metadata:
  Mtime: 105.3
User Metadata:
  Mtime: 107.3
Other Metadata:
  X-Object-Mtime: 104.3''' % (
            utils.Timestamp(106.3).internal)

        self.assertEqual(out.getvalue().strip(), exp_out)

    def test_print_obj_crypto_metadata(self):
        cryto_body_meta = '%7B%22body_key%22%3A+%7B%22iv%22%3A+%22HmpwLDjlo' \
            '6JxFvOOCVyT6Q%3D%3D%22%2C+%22key%22%3A+%22dEox1dyZJPCs4mtmiQDg' \
            'u%2Fv1RTointi%2FUhm2y%2BgB3F8%3D%22%7D%2C+%22cipher%22%3A+%22A' \
            'ES_CTR_256%22%2C+%22iv%22%3A+%22l3W0NZekjt4PFkAJXubVYQ%3D%3D%2' \
            '2%2C+%22key_id%22%3A+%7B%22path%22%3A+%22%2FAUTH_test%2Ftest%2' \
            'Ftest%22%2C+%22secret_id%22%3A+%222018%22%2C+%22v%22%3A+%221%2' \
            '2%7D%7D'

        crypto_meta_meta = '%7B%22cipher%22%3A+%22AES_CTR_256%22%2C+%22key_' \
            'id%22%3A+%7B%22path%22%3A+%22%2FAUTH_test%2Ftest%2Ftest%22%2C+' \
            '%22secret_id%22%3A+%222018%22%2C+%22v%22%3A+%221%22%7D%7D'

        stub_metadata = {
            'name': '/AUTH_test/test/test',
            'Content-Type': 'application/sekret',
            'X-Timestamp': '1549899598.237075',
            'X-Object-Sysmeta-Crypto-Body-Meta': cryto_body_meta,
            'X-Object-Transient-Sysmeta-Crypto-Meta': crypto_meta_meta,
        }
        out = StringIO()
        with mock.patch('sys.stdout', out):
            print_obj_metadata(stub_metadata)
        exp_out = '''Path: /AUTH_test/test/test
  Account: AUTH_test
  Container: test
  Object: test
  Object hash: dc3a7d53522b9392b0d19571a752fdfb
Content-Type: application/sekret
Timestamp: 2019-02-11T15:39:58.237080 (1549899598.23708)
System Metadata:
  X-Object-Sysmeta-Crypto-Body-Meta: %s
Transient System Metadata:
  X-Object-Transient-Sysmeta-Crypto-Meta: %s
User Metadata:
  No metadata found
Other Metadata:
  No metadata found
Data crypto details: {
  "body_key": {
    "iv": "HmpwLDjlo6JxFvOOCVyT6Q==",
    "key": "dEox1dyZJPCs4mtmiQDgu/v1RTointi/Uhm2y+gB3F8="
  },
  "cipher": "AES_CTR_256",
  "iv": "l3W0NZekjt4PFkAJXubVYQ==",
  "key_id": {
    "path": "/AUTH_test/test/test",
    "secret_id": "2018",
    "v": "1"
  }
}
Metadata crypto details: {
  "cipher": "AES_CTR_256",
  "key_id": {
    "path": "/AUTH_test/test/test",
    "secret_id": "2018",
    "v": "1"
  }
}''' % (cryto_body_meta, crypto_meta_meta)

        self.maxDiff = None
        self.assertMultiLineEqual(out.getvalue().strip(), exp_out)


class TestPrintObjWeirdPath(TestPrintObjFullMeta):
    def setUp(self):
        super(TestPrintObjWeirdPath, self).setUp()
        # device name is objects-0 instead of sda, this is weird.
        self.datafile = os.path.join(self.testdir,
                                     'objects-0', 'objects-1',
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
