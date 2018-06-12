#!/usr/bin/python -u
# Copyright (c) 2010-2016 OpenStack Foundation
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

from unittest2 import SkipTest

import test.functional as tf
from test.functional import cluster_info
from test.functional.tests import Utils, Base, BaseEnv
from test.functional.swift_test_client import Account, Connection, \
    ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestDomainRemapEnv(BaseEnv):
    domain_remap_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.domain_remap_enabled is None:
            cls.domain_remap_enabled = 'domain_remap' in cluster_info
            if not cls.domain_remap_enabled:
                return

        cls.account = Account(
            cls.conn, tf.config.get('account', tf.config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.obj = cls.container.file(Utils.create_name())
        cls.obj.write('obj contents')

        cls.obj_slash = cls.container.file('/v1')
        cls.obj_slash.write('obj contents')


class TestDomainRemap(Base):
    env = TestDomainRemapEnv
    set_up = False

    def setUp(self):
        super(TestDomainRemap, self).setUp()
        if self.env.domain_remap_enabled is False:
            raise SkipTest("Domain Remap is not enabled")
        elif self.env.domain_remap_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected domain_remap_enabled to be True/False, got %r" %
                (self.env.domain_remap_enabled,))
        # domain_remap middleware does not advertise its storage_domain values
        # in swift /info responses so a storage_domain must be configured in
        # test.conf for these tests to succeed
        if not tf.config.get('storage_domain'):
            raise SkipTest('Domain Remap storage_domain not configured in %s' %
                           tf.config['__file__'])

        storage_domain = tf.config.get('storage_domain')

        self.acct_domain_dash = '%s.%s' % (self.env.account.conn.account_name,
                                           storage_domain)
        self.acct_domain_underscore = '%s.%s' % (
            self.env.account.conn.account_name.replace('_', '-'),
            storage_domain)

        self.cont_domain_dash = '%s.%s.%s' % (
            self.env.container.name,
            self.env.account.conn.account_name,
            storage_domain)
        self.cont_domain_underscore = '%s.%s.%s' % (
            self.env.container.name,
            self.env.account.conn.account_name.replace('_', '-'),
            storage_domain)

    def test_GET_remapped_account(self):
        for domain in (self.acct_domain_dash, self.acct_domain_underscore):
            self.env.account.conn.make_request('GET', '/',
                                               hdrs={'Host': domain},
                                               cfg={'absolute_path': True})
            self.assert_status(200)
            body = self.env.account.conn.response.read()
            self.assertIn(self.env.container.name, body)

            path = '/'.join(['', self.env.container.name])
            self.env.account.conn.make_request('GET', path,
                                               hdrs={'Host': domain},
                                               cfg={'absolute_path': True})
            self.assert_status(200)
            body = self.env.account.conn.response.read()
            self.assertIn(self.env.obj.name, body)
            self.assertIn(self.env.obj_slash.name, body)

            for obj in (self.env.obj, self.env.obj_slash):
                path = '/'.join(['', self.env.container.name, obj.name])
                self.env.account.conn.make_request('GET', path,
                                                   hdrs={'Host': domain},
                                                   cfg={'absolute_path': True})
                self.assert_status(200)
                self.assert_body('obj contents')

    def test_PUT_remapped_account(self):
        for domain in (self.acct_domain_dash, self.acct_domain_underscore):
            # Create a container
            new_container_name = Utils.create_name()
            path = '/'.join(['', new_container_name])
            self.env.account.conn.make_request('PUT', path,
                                               data='new obj contents',
                                               hdrs={'Host': domain},
                                               cfg={'absolute_path': True})
            self.assert_status(201)
            self.assertIn(new_container_name, self.env.account.containers())

            # Create an object
            new_obj_name = Utils.create_name()
            path = '/'.join(['', self.env.container.name, new_obj_name])
            self.env.account.conn.make_request('PUT', path,
                                               data='new obj contents',
                                               hdrs={'Host': domain},
                                               cfg={'absolute_path': True})
            self.assert_status(201)
            new_obj = self.env.container.file(new_obj_name)
            self.assertEqual(new_obj.read(), 'new obj contents')

    def test_GET_remapped_container(self):
        for domain in (self.cont_domain_dash, self.cont_domain_underscore):
            self.env.account.conn.make_request('GET', '/',
                                               hdrs={'Host': domain},
                                               cfg={'absolute_path': True})
            self.assert_status(200)
            body = self.env.account.conn.response.read()
            self.assertIn(self.env.obj.name, body)
            self.assertIn(self.env.obj_slash.name, body)

            for obj in (self.env.obj, self.env.obj_slash):
                path = '/'.join(['', obj.name])
                self.env.account.conn.make_request('GET', path,
                                                   hdrs={'Host': domain},
                                                   cfg={'absolute_path': True})
                self.assert_status(200)
                self.assert_body('obj contents')

    def test_PUT_remapped_container(self):
        for domain in (self.cont_domain_dash, self.cont_domain_underscore):
            new_obj_name = Utils.create_name()
            path = '/'.join(['', new_obj_name])
            self.env.account.conn.make_request('PUT', path,
                                               data='new obj contents',
                                               hdrs={'Host': domain},
                                               cfg={'absolute_path': True})
            self.assert_status(201)

            new_obj = self.env.container.file(new_obj_name)
            self.assertEqual(new_obj.read(), 'new obj contents')
