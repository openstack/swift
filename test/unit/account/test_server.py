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

import errno
import os
import mock
import unittest
from tempfile import mkdtemp
from shutil import rmtree
from StringIO import StringIO
from time import gmtime
from test.unit import FakeLogger
import itertools
import random

import simplejson
import xml.dom.minidom

from swift import __version__ as swift_version
from swift.common.swob import Request
from swift.common import constraints
from swift.account.server import AccountController
from swift.common.utils import normalize_timestamp, replication, public
from swift.common.request_helpers import get_sys_meta_prefix
from test.unit import patch_policies
from swift.common.storage_policy import StoragePolicy, POLICIES


@patch_policies
class TestAccountController(unittest.TestCase):
    """Test swift.account.server.AccountController"""
    def setUp(self):
        """Set up for testing swift.account.server.AccountController"""
        self.testdir_base = mkdtemp()
        self.testdir = os.path.join(self.testdir_base, 'account_server')
        self.controller = AccountController(
            {'devices': self.testdir, 'mount_check': 'false'})

    def tearDown(self):
        """Tear down for testing swift.account.server.AccountController"""
        try:
            rmtree(self.testdir_base)
        except OSError as err:
            if err.errno != errno.ENOENT:
                raise

    def test_OPTIONS(self):
        server_handler = AccountController(
            {'devices': self.testdir, 'mount_check': 'false'})
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0
        resp = server_handler.OPTIONS(req)
        self.assertEquals(200, resp.status_int)
        for verb in 'OPTIONS GET POST PUT DELETE HEAD REPLICATE'.split():
            self.assertTrue(
                verb in resp.headers['Allow'].split(', '))
        self.assertEquals(len(resp.headers['Allow'].split(', ')), 7)
        self.assertEquals(resp.headers['Server'],
                          (server_handler.server_type + '/' + swift_version))

    def test_DELETE_not_found(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

    def test_DELETE_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_DELETE_not_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        # We now allow deleting non-empty accounts
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_DELETE_now_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank(
            '/sda1/p/a/c1',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Put-Timestamp': '1',
                     'X-Delete-Timestamp': '2',
                     'X-Object-Count': '0',
                     'X-Bytes-Used': '0',
                     'X-Timestamp': normalize_timestamp(0)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_DELETE_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_DELETE_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_DELETE_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank(
            '/sda-null/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                      'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 507)

    def test_HEAD_not_found(self):
        # Test the case in which account does not exist (can be recreated)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

        # Test the case in which account was deleted but not yet reaped
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_HEAD_empty_account(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['x-account-container-count'], '0')
        self.assertEqual(resp.headers['x-account-object-count'], '0')
        self.assertEqual(resp.headers['x-account-bytes-used'], '0')

    def test_HEAD_with_containers(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['x-account-container-count'], '2')
        self.assertEqual(resp.headers['x-account-object-count'], '0')
        self.assertEqual(resp.headers['x-account-bytes-used'], '0')
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD',
                                                  'HTTP_X_TIMESTAMP': '5'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['x-account-container-count'], '2')
        self.assertEqual(resp.headers['x-account-object-count'], '4')
        self.assertEqual(resp.headers['x-account-bytes-used'], '6')

    def test_HEAD_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'HEAD',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_HEAD_invalid_content_type(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Accept': 'application/plain'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 406)

    def test_HEAD_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'HEAD',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 507)

    def test_HEAD_invalid_format(self):
        format = '%D1%BD%8A9'  # invalid UTF-8; should be %E1%BD%8A9 (E -> D)
        req = Request.blank('/sda1/p/a?format=' + format,
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_not_found(self):
        req = Request.blank(
            '/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-PUT-Timestamp': normalize_timestamp(1),
                     'X-DELETE-Timestamp': normalize_timestamp(0),
                     'X-Object-Count': '1',
                     'X-Bytes-Used': '1',
                     'X-Timestamp': normalize_timestamp(0)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

    def test_PUT(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)

    def test_PUT_simulated_create_race(self):
        state = ['initial']

        from swift.account.backend import AccountBroker as OrigAcBr

        class InterceptedAcBr(OrigAcBr):

            def __init__(self, *args, **kwargs):
                super(InterceptedAcBr, self).__init__(*args, **kwargs)
                if state[0] == 'initial':
                    # Do nothing initially
                    pass
                elif state[0] == 'race':
                    # Save the original db_file attribute value
                    self._saved_db_file = self.db_file
                    self.db_file += '.doesnotexist'

            def initialize(self, *args, **kwargs):
                if state[0] == 'initial':
                    # Do nothing initially
                    pass
                elif state[0] == 'race':
                    # Restore the original db_file attribute to get the race
                    # behavior
                    self.db_file = self._saved_db_file
                return super(InterceptedAcBr, self).initialize(*args, **kwargs)

        with mock.patch("swift.account.server.AccountBroker", InterceptedAcBr):
            req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                      'HTTP_X_TIMESTAMP': '0'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)
            state[0] = "race"
            req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                      'HTTP_X_TIMESTAMP': '1'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 202)

    def test_PUT_after_DELETE(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': normalize_timestamp(2)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 403)
        self.assertEqual(resp.body, 'Recently deleted')
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_PUT_GET_metadata(self):
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Account-Meta-Test': 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Account-Meta-Test2': 'Value2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'Value')
        self.assertEqual(resp.headers.get('x-account-meta-test2'), 'Value2')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Account-Meta-Test': 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Account-Meta-Test': 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Account-Meta-Test': ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assert_('x-account-meta-test' not in resp.headers)

    def test_PUT_GET_sys_metadata(self):
        prefix = get_sys_meta_prefix('account')
        hdr = '%stest' % prefix
        hdr2 = '%stest2' % prefix
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     hdr.title(): 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     hdr2.title(): 'Value2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'Value')
        self.assertEqual(resp.headers.get(hdr2), 'Value2')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     hdr.title(): 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     hdr.title(): 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     hdr.title(): ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 202)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assert_(hdr not in resp.headers)

    def test_PUT_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_PUT_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 507)

    def test_POST_HEAD_metadata(self):
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Account-Meta-Test': 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'Value')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Account-Meta-Test': 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Account-Meta-Test': 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get('x-account-meta-test'), 'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Account-Meta-Test': ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assert_('x-account-meta-test' not in resp.headers)

    def test_POST_HEAD_sys_metadata(self):
        prefix = get_sys_meta_prefix('account')
        hdr = '%stest' % prefix
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)
        # Set metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     hdr.title(): 'Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'Value')
        # Update metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     hdr.title(): 'New Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'New Value')
        # Send old update to metadata header
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     hdr.title(): 'Old Value'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers.get(hdr), 'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank(
            '/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     hdr.title(): ''})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assert_(hdr not in resp.headers)

    def test_POST_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'POST',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST',
                                                  'HTTP_X_TIMESTAMP': '0'},
                            headers={'X-Timestamp': 'not-float'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 400)

    def test_POST_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'POST',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 507)

    def test_POST_after_DELETE_not_found(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST',
                                                  'HTTP_X_TIMESTAMP': '2'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_GET_not_found_plain(self):
        # Test the case in which account does not exist (can be recreated)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

        # Test the case in which account was deleted but not yet reaped
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['X-Account-Status'], 'Deleted')

    def test_GET_not_found_json(self):
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_GET_not_found_xml(self):
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_GET_empty_account_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(resp.headers['Content-Type'],
                         'text/plain; charset=utf-8')

    def test_GET_empty_account_json(self):
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['Content-Type'],
                         'application/json; charset=utf-8')

    def test_GET_empty_account_xml(self):
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['Content-Type'],
                         'application/xml; charset=utf-8')

    def test_GET_over_limit(self):
        req = Request.blank(
            '/sda1/p/a?limit=%d' % (constraints.ACCOUNT_LISTING_LIMIT + 1),
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 412)

    def test_GET_with_containers_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'), ['c1', 'c2'])
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'), ['c1', 'c2'])
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(resp.charset, 'utf-8')

        # test unknown format uses default plain
        req = Request.blank('/sda1/p/a?format=somethinglese',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'), ['c1', 'c2'])
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(resp.charset, 'utf-8')

    def test_GET_with_containers_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(simplejson.loads(resp.body),
                         [{'count': 0, 'bytes': 0, 'name': 'c1'},
                          {'count': 0, 'bytes': 0, 'name': 'c2'}])
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(simplejson.loads(resp.body),
                         [{'count': 1, 'bytes': 2, 'name': 'c1'},
                          {'count': 3, 'bytes': 4, 'name': 'c2'}])
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(resp.charset, 'utf-8')

    def test_GET_with_containers_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEqual(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEqual(len(listing), 2)
        self.assertEqual(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c1')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '0')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '0')
        self.assertEqual(listing[-1].nodeName, 'container')
        container = \
            [n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c2')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '0')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '0')
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEqual(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEqual(len(listing), 2)
        self.assertEqual(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c1')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '1')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '2')
        self.assertEqual(listing[-1].nodeName, 'container')
        container = [
            n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c2')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '3')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '4')
        self.assertEqual(resp.charset, 'utf-8')

    def test_GET_xml_escapes_account_name(self):
        req = Request.blank(
            '/sda1/p/%22%27',   # "'
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)

        req = Request.blank(
            '/sda1/p/%22%27?format=xml',
            environ={'REQUEST_METHOD': 'GET', 'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)

        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEqual(dom.firstChild.attributes['name'].value, '"\'')

    def test_GET_xml_escapes_container_name(self):
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)

        req = Request.blank(
            '/sda1/p/a/%22%3Cword',  # "<word
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                     'HTTP_X_PUT_TIMESTAMP': '1', 'HTTP_X_OBJECT_COUNT': '0',
                     'HTTP_X_DELETE_TIMESTAMP': '0', 'HTTP_X_BYTES_USED': '1'})
        req.get_response(self.controller)

        req = Request.blank(
            '/sda1/p/a?format=xml',
            environ={'REQUEST_METHOD': 'GET', 'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        dom = xml.dom.minidom.parseString(resp.body)

        self.assertEqual(
            dom.firstChild.firstChild.nextSibling.firstChild.firstChild.data,
            '"<word')

    def test_GET_xml_escapes_container_name_as_subdir(self):
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)

        req = Request.blank(
            '/sda1/p/a/%22%3Cword-test',  # "<word-test
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                     'HTTP_X_PUT_TIMESTAMP': '1', 'HTTP_X_OBJECT_COUNT': '0',
                     'HTTP_X_DELETE_TIMESTAMP': '0', 'HTTP_X_BYTES_USED': '1'})
        req.get_response(self.controller)

        req = Request.blank(
            '/sda1/p/a?format=xml&delimiter=-',
            environ={'REQUEST_METHOD': 'GET', 'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        dom = xml.dom.minidom.parseString(resp.body)

        self.assertEqual(
            dom.firstChild.firstChild.nextSibling.attributes['name'].value,
            '"<word-')

    def test_GET_limit_marker_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        for c in xrange(5):
            req = Request.blank(
                '/sda1/p/a/c%d' % c,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': str(c + 1),
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '2',
                         'X-Bytes-Used': '3',
                         'X-Timestamp': normalize_timestamp(0)})
            req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?limit=3',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'), ['c0', 'c1', 'c2'])
        req = Request.blank('/sda1/p/a?limit=3&marker=c2',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'), ['c3', 'c4'])

    def test_GET_limit_marker_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        for c in xrange(5):
            req = Request.blank(
                '/sda1/p/a/c%d' % c,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': str(c + 1),
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '2',
                         'X-Bytes-Used': '3',
                         'X-Timestamp': normalize_timestamp(0)})
            req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?limit=3&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(simplejson.loads(resp.body),
                         [{'count': 2, 'bytes': 3, 'name': 'c0'},
                          {'count': 2, 'bytes': 3, 'name': 'c1'},
                          {'count': 2, 'bytes': 3, 'name': 'c2'}])
        req = Request.blank('/sda1/p/a?limit=3&marker=c2&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(simplejson.loads(resp.body),
                         [{'count': 2, 'bytes': 3, 'name': 'c3'},
                          {'count': 2, 'bytes': 3, 'name': 'c4'}])

    def test_GET_limit_marker_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        for c in xrange(5):
            req = Request.blank(
                '/sda1/p/a/c%d' % c,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': str(c + 1),
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '2',
                         'X-Bytes-Used': '3',
                         'X-Timestamp': normalize_timestamp(c)})
            req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?limit=3&format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEqual(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEqual(len(listing), 3)
        self.assertEqual(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c0')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '3')
        self.assertEqual(listing[-1].nodeName, 'container')
        container = [
            n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c2')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '3')
        req = Request.blank('/sda1/p/a?limit=3&marker=c2&format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEqual(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEqual(len(listing), 2)
        self.assertEqual(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c3')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '3')
        self.assertEqual(listing[-1].nodeName, 'container')
        container = [
            n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEqual(sorted([n.nodeName for n in container]),
                         ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEqual(node.firstChild.nodeValue, 'c4')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEqual(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEqual(node.firstChild.nodeValue, '3')

    def test_GET_accept_wildcard(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = '*/*'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, 'c1\n')

    def test_GET_accept_application_wildcard(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        resp = req.get_response(self.controller)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/*'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(len(simplejson.loads(resp.body)), 1)

    def test_GET_accept_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(len(simplejson.loads(resp.body)), 1)

    def test_GET_accept_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/xml'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEqual(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEqual(len(listing), 1)

    def test_GET_accept_conflicting(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?format=plain',
                            environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, 'c1\n')

    def test_GET_accept_not_valid(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        req.get_response(self.controller)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/xml*'
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 406)

    def test_GET_delimiter_too_long(self):
        req = Request.blank('/sda1/p/a?delimiter=xx',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 412)

    def test_GET_prefix_delimiter_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for first in range(3):
            req = Request.blank(
                '/sda1/p/a/sub.%s' % first,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': '1',
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '0',
                         'X-Bytes-Used': '0',
                         'X-Timestamp': normalize_timestamp(0)})
            req.get_response(self.controller)
            for second in range(3):
                req = Request.blank(
                    '/sda1/p/a/sub.%s.%s' % (first, second),
                    environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Put-Timestamp': '1',
                             'X-Delete-Timestamp': '0',
                             'X-Object-Count': '0',
                             'X-Bytes-Used': '0',
                             'X-Timestamp': normalize_timestamp(0)})
                req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?delimiter=.',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'), ['sub.'])
        req = Request.blank('/sda1/p/a?prefix=sub.&delimiter=.',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(
            resp.body.strip().split('\n'),
            ['sub.0', 'sub.0.', 'sub.1', 'sub.1.', 'sub.2', 'sub.2.'])
        req = Request.blank('/sda1/p/a?prefix=sub.1.&delimiter=.',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body.strip().split('\n'),
                         ['sub.1.0', 'sub.1.1', 'sub.1.2'])

    def test_GET_prefix_delimiter_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for first in range(3):
            req = Request.blank(
                '/sda1/p/a/sub.%s' % first,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': '1',
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '0',
                         'X-Bytes-Used': '0',
                         'X-Timestamp': normalize_timestamp(0)})
            req.get_response(self.controller)
            for second in range(3):
                req = Request.blank(
                    '/sda1/p/a/sub.%s.%s' % (first, second),
                    environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Put-Timestamp': '1',
                             'X-Delete-Timestamp': '0',
                             'X-Object-Count': '0',
                             'X-Bytes-Used': '0',
                             'X-Timestamp': normalize_timestamp(0)})
                req.get_response(self.controller)
        req = Request.blank('/sda1/p/a?delimiter=.&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual([n.get('name', 's:' + n.get('subdir', 'error'))
                          for n in simplejson.loads(resp.body)], ['s:sub.'])
        req = Request.blank('/sda1/p/a?prefix=sub.&delimiter=.&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(
            [n.get('name', 's:' + n.get('subdir', 'error'))
             for n in simplejson.loads(resp.body)],
            ['sub.0', 's:sub.0.', 'sub.1', 's:sub.1.', 'sub.2', 's:sub.2.'])
        req = Request.blank('/sda1/p/a?prefix=sub.1.&delimiter=.&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(
            [n.get('name', 's:' + n.get('subdir', 'error'))
             for n in simplejson.loads(resp.body)],
            ['sub.1.0', 'sub.1.1', 'sub.1.2'])

    def test_GET_prefix_delimiter_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        resp = req.get_response(self.controller)
        for first in range(3):
            req = Request.blank(
                '/sda1/p/a/sub.%s' % first,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': '1',
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '0',
                         'X-Bytes-Used': '0',
                         'X-Timestamp': normalize_timestamp(0)})
            req.get_response(self.controller)
            for second in range(3):
                req = Request.blank(
                    '/sda1/p/a/sub.%s.%s' % (first, second),
                    environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Put-Timestamp': '1',
                             'X-Delete-Timestamp': '0',
                             'X-Object-Count': '0',
                             'X-Bytes-Used': '0',
                             'X-Timestamp': normalize_timestamp(0)})
                req.get_response(self.controller)
        req = Request.blank(
            '/sda1/p/a?delimiter=.&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        listing = []
        for node1 in dom.firstChild.childNodes:
            if node1.nodeName == 'subdir':
                listing.append('s:' + node1.attributes['name'].value)
            elif node1.nodeName == 'container':
                for node2 in node1.childNodes:
                    if node2.nodeName == 'name':
                        listing.append(node2.firstChild.nodeValue)
        self.assertEqual(listing, ['s:sub.'])
        req = Request.blank(
            '/sda1/p/a?prefix=sub.&delimiter=.&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        listing = []
        for node1 in dom.firstChild.childNodes:
            if node1.nodeName == 'subdir':
                listing.append('s:' + node1.attributes['name'].value)
            elif node1.nodeName == 'container':
                for node2 in node1.childNodes:
                    if node2.nodeName == 'name':
                        listing.append(node2.firstChild.nodeValue)
        self.assertEqual(
            listing,
            ['sub.0', 's:sub.0.', 'sub.1', 's:sub.1.', 'sub.2', 's:sub.2.'])
        req = Request.blank(
            '/sda1/p/a?prefix=sub.1.&delimiter=.&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        listing = []
        for node1 in dom.firstChild.childNodes:
            if node1.nodeName == 'subdir':
                listing.append('s:' + node1.attributes['name'].value)
            elif node1.nodeName == 'container':
                for node2 in node1.childNodes:
                    if node2.nodeName == 'name':
                        listing.append(node2.firstChild.nodeValue)
        self.assertEqual(listing, ['sub.1.0', 'sub.1.1', 'sub.1.2'])

    def test_GET_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'GET',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 507)

    def test_through_call(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': 'GET',
                                  'SCRIPT_NAME': '',
                                  'PATH_INFO': '/sda1/p/a',
                                  'SERVER_NAME': '127.0.0.1',
                                  'SERVER_PORT': '8080',
                                  'SERVER_PROTOCOL': 'HTTP/1.0',
                                  'CONTENT_LENGTH': '0',
                                  'wsgi.version': (1, 0),
                                  'wsgi.url_scheme': 'http',
                                  'wsgi.input': inbuf,
                                  'wsgi.errors': errbuf,
                                  'wsgi.multithread': False,
                                  'wsgi.multiprocess': False,
                                  'wsgi.run_once': False},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '404 ')

    def test_through_call_invalid_path(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': 'GET',
                                  'SCRIPT_NAME': '',
                                  'PATH_INFO': '/bob',
                                  'SERVER_NAME': '127.0.0.1',
                                  'SERVER_PORT': '8080',
                                  'SERVER_PROTOCOL': 'HTTP/1.0',
                                  'CONTENT_LENGTH': '0',
                                  'wsgi.version': (1, 0),
                                  'wsgi.url_scheme': 'http',
                                  'wsgi.input': inbuf,
                                  'wsgi.errors': errbuf,
                                  'wsgi.multithread': False,
                                  'wsgi.multiprocess': False,
                                  'wsgi.run_once': False},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '400 ')

    def test_through_call_invalid_path_utf8(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': 'GET',
                                  'SCRIPT_NAME': '',
                                  'PATH_INFO': '\x00',
                                  'SERVER_NAME': '127.0.0.1',
                                  'SERVER_PORT': '8080',
                                  'SERVER_PROTOCOL': 'HTTP/1.0',
                                  'CONTENT_LENGTH': '0',
                                  'wsgi.version': (1, 0),
                                  'wsgi.url_scheme': 'http',
                                  'wsgi.input': inbuf,
                                  'wsgi.errors': errbuf,
                                  'wsgi.multithread': False,
                                  'wsgi.multiprocess': False,
                                  'wsgi.run_once': False},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '412 ')

    def test_invalid_method_doesnt_exist(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': 'method_doesnt_exist',
                                  'PATH_INFO': '/sda1/p/a'},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': '__init__',
                                  'PATH_INFO': '/sda1/p/a'},
                                 start_response)
        self.assertEqual(errbuf.getvalue(), '')
        self.assertEqual(outbuf.getvalue()[:4], '405 ')

    def test_params_format(self):
        Request.blank('/sda1/p/a',
                      headers={'X-Timestamp': normalize_timestamp(1)},
                      environ={'REQUEST_METHOD': 'PUT'}).get_response(
                          self.controller)
        for format in ('xml', 'json'):
            req = Request.blank('/sda1/p/a?format=%s' % format,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 200)

    def test_params_utf8(self):
        # Bad UTF8 sequence, all parameters should cause 400 error
        for param in ('delimiter', 'limit', 'marker', 'prefix', 'end_marker',
                      'format'):
            req = Request.blank('/sda1/p/a?%s=\xce' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 400,
                             "%d on param %s" % (resp.status_int, param))
        # Good UTF8 sequence for delimiter, too long (1 byte delimiters only)
        req = Request.blank('/sda1/p/a?delimiter=\xce\xa9',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 412,
                         "%d on param delimiter" % (resp.status_int))
        Request.blank('/sda1/p/a',
                      headers={'X-Timestamp': normalize_timestamp(1)},
                      environ={'REQUEST_METHOD': 'PUT'}).get_response(
                          self.controller)
        # Good UTF8 sequence, ignored for limit, doesn't affect other queries
        for param in ('limit', 'marker', 'prefix', 'end_marker', 'format'):
            req = Request.blank('/sda1/p/a?%s=\xce\xa9' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 204,
                             "%d on param %s" % (resp.status_int, param))

    def test_PUT_auto_create(self):
        headers = {'x-put-timestamp': normalize_timestamp(1),
                   'x-delete-timestamp': normalize_timestamp(0),
                   'x-object-count': '0',
                   'x-bytes-used': '0'}

        req = Request.blank('/sda1/p/a/c',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

        req = Request.blank('/sda1/p/.a/c',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a/.c',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=dict(headers))
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)

    def test_content_type_on_HEAD(self):
        Request.blank('/sda1/p/a',
                      headers={'X-Timestamp': normalize_timestamp(1)},
                      environ={'REQUEST_METHOD': 'PUT'}).get_response(
                          self.controller)

        env = {'REQUEST_METHOD': 'HEAD'}

        req = Request.blank('/sda1/p/a?format=xml', environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')

        req = Request.blank('/sda1/p/a?format=json', environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a', environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'text/plain')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a', headers={'Accept': 'application/json'}, environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/json')
        self.assertEqual(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a', headers={'Accept': 'application/xml'}, environ=env)
        resp = req.get_response(self.controller)
        self.assertEqual(resp.content_type, 'application/xml')
        self.assertEqual(resp.charset, 'utf-8')

    def test_serv_reserv(self):
        # Test replication_server flag was set from configuration file.
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertEqual(AccountController(conf).replication_server, None)
        for val in [True, '1', 'True', 'true']:
            conf['replication_server'] = val
            self.assertTrue(AccountController(conf).replication_server)
        for val in [False, 0, '0', 'False', 'false', 'test_string']:
            conf['replication_server'] = val
            self.assertFalse(AccountController(conf).replication_server)

    def test_list_allowed_methods(self):
        # Test list of allowed_methods
        obj_methods = ['DELETE', 'PUT', 'HEAD', 'GET', 'POST']
        repl_methods = ['REPLICATE']
        for method_name in obj_methods:
            method = getattr(self.controller, method_name)
            self.assertFalse(hasattr(method, 'replication'))
        for method_name in repl_methods:
            method = getattr(self.controller, method_name)
            self.assertEqual(method.replication, True)

    def test_correct_allowed_method(self):
        # Test correct work for allowed method using
        # swift.account.server.AccountController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = AccountController(
            {'devices': self.testdir,
             'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

        method = 'PUT'
        env = {'REQUEST_METHOD': method,
               'SCRIPT_NAME': '',
               'PATH_INFO': '/sda1/p/a/c',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '8080',
               'SERVER_PROTOCOL': 'HTTP/1.0',
               'CONTENT_LENGTH': '0',
               'wsgi.version': (1, 0),
               'wsgi.url_scheme': 'http',
               'wsgi.input': inbuf,
               'wsgi.errors': errbuf,
               'wsgi.multithread': False,
               'wsgi.multiprocess': False,
               'wsgi.run_once': False}

        method_res = mock.MagicMock()
        mock_method = public(lambda x: mock.MagicMock(return_value=method_res))
        with mock.patch.object(self.controller, method,
                               new=mock_method):
            mock_method.replication = False
            response = self.controller(env, start_response)
            self.assertEqual(response, method_res)

    def test_not_allowed_method(self):
        # Test correct work for NOT allowed method using
        # swift.account.server.AccountController.__call__
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = AccountController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'false'})

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

        method = 'PUT'
        env = {'REQUEST_METHOD': method,
               'SCRIPT_NAME': '',
               'PATH_INFO': '/sda1/p/a/c',
               'SERVER_NAME': '127.0.0.1',
               'SERVER_PORT': '8080',
               'SERVER_PROTOCOL': 'HTTP/1.0',
               'CONTENT_LENGTH': '0',
               'wsgi.version': (1, 0),
               'wsgi.url_scheme': 'http',
               'wsgi.input': inbuf,
               'wsgi.errors': errbuf,
               'wsgi.multithread': False,
               'wsgi.multiprocess': False,
               'wsgi.run_once': False}

        answer = ['<html><h1>Method Not Allowed</h1><p>The method is not '
                  'allowed for this resource.</p></html>']
        mock_method = replication(public(lambda x: mock.MagicMock()))
        with mock.patch.object(self.controller, method,
                               new=mock_method):
            mock_method.replication = True
            response = self.controller.__call__(env, start_response)
            self.assertEqual(response, answer)

    def test_call_incorrect_replication_method(self):
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()
        self.controller = AccountController(
            {'devices': self.testdir, 'mount_check': 'false',
             'replication_server': 'true'})

        def start_response(*args):
            """Sends args to outbuf"""
            outbuf.writelines(args)

        obj_methods = ['DELETE', 'PUT', 'HEAD', 'GET', 'POST', 'OPTIONS']
        for method in obj_methods:
            env = {'REQUEST_METHOD': method,
                   'SCRIPT_NAME': '',
                   'PATH_INFO': '/sda1/p/a/c',
                   'SERVER_NAME': '127.0.0.1',
                   'SERVER_PORT': '8080',
                   'SERVER_PROTOCOL': 'HTTP/1.0',
                   'CONTENT_LENGTH': '0',
                   'wsgi.version': (1, 0),
                   'wsgi.url_scheme': 'http',
                   'wsgi.input': inbuf,
                   'wsgi.errors': errbuf,
                   'wsgi.multithread': False,
                   'wsgi.multiprocess': False,
                   'wsgi.run_once': False}
            self.controller(env, start_response)
            self.assertEquals(errbuf.getvalue(), '')
            self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_GET_log_requests_true(self):
        self.controller.logger = FakeLogger()
        self.controller.log_requests = True

        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue(self.controller.logger.log_dict['info'])

    def test_GET_log_requests_false(self):
        self.controller.logger = FakeLogger()
        self.controller.log_requests = False
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 404)
        self.assertFalse(self.controller.logger.log_dict['info'])

    def test_log_line_format(self):
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'HEAD', 'REMOTE_ADDR': '1.2.3.4'})
        self.controller.logger = FakeLogger()
        with mock.patch(
                'time.gmtime', mock.MagicMock(side_effect=[gmtime(10001.0)])):
            with mock.patch(
                    'time.time',
                    mock.MagicMock(side_effect=[10000.0, 10001.0, 10002.0])):
                with mock.patch(
                        'os.getpid', mock.MagicMock(return_value=1234)):
                    req.get_response(self.controller)
        self.assertEqual(
            self.controller.logger.log_dict['info'],
            [(('1.2.3.4 - - [01/Jan/1970:02:46:41 +0000] "HEAD /sda1/p/a" 404 '
             '- "-" "-" "-" 2.0000 "-" 1234 -',), {})])

    def test_policy_stats_with_legacy(self):
        ts = itertools.count()
        # create the account
        req = Request.blank('/sda1/p/a', method='PUT', headers={
            'X-Timestamp': normalize_timestamp(ts.next())})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity

        # add a container
        req = Request.blank('/sda1/p/a/c1', method='PUT', headers={
            'X-Put-Timestamp': normalize_timestamp(ts.next()),
            'X-Delete-Timestamp': '0',
            'X-Object-Count': '2',
            'X-Bytes-Used': '4',
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # read back rollup
        for method in ('GET', 'HEAD'):
            req = Request.blank('/sda1/p/a', method=method)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)
            self.assertEquals(resp.headers['X-Account-Object-Count'], '2')
            self.assertEquals(resp.headers['X-Account-Bytes-Used'], '4')
            self.assertEquals(
                resp.headers['X-Account-Storage-Policy-%s-Object-Count' %
                             POLICIES[0].name], '2')
            self.assertEquals(
                resp.headers['X-Account-Storage-Policy-%s-Bytes-Used' %
                             POLICIES[0].name], '4')
            self.assertEquals(
                resp.headers['X-Account-Storage-Policy-%s-Container-Count' %
                             POLICIES[0].name], '1')

    def test_policy_stats_non_default(self):
        ts = itertools.count()
        # create the account
        req = Request.blank('/sda1/p/a', method='PUT', headers={
            'X-Timestamp': normalize_timestamp(ts.next())})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity

        # add a container
        non_default_policies = [p for p in POLICIES if not p.is_default]
        policy = random.choice(non_default_policies)
        req = Request.blank('/sda1/p/a/c1', method='PUT', headers={
            'X-Put-Timestamp': normalize_timestamp(ts.next()),
            'X-Delete-Timestamp': '0',
            'X-Object-Count': '2',
            'X-Bytes-Used': '4',
            'X-Backend-Storage-Policy-Index': policy.idx,
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # read back rollup
        for method in ('GET', 'HEAD'):
            req = Request.blank('/sda1/p/a', method=method)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)
            self.assertEquals(resp.headers['X-Account-Object-Count'], '2')
            self.assertEquals(resp.headers['X-Account-Bytes-Used'], '4')
            self.assertEquals(
                resp.headers['X-Account-Storage-Policy-%s-Object-Count' %
                             policy.name], '2')
            self.assertEquals(
                resp.headers['X-Account-Storage-Policy-%s-Bytes-Used' %
                             policy.name], '4')
            self.assertEquals(
                resp.headers['X-Account-Storage-Policy-%s-Container-Count' %
                             policy.name], '1')

    def test_empty_policy_stats(self):
        ts = itertools.count()
        # create the account
        req = Request.blank('/sda1/p/a', method='PUT', headers={
            'X-Timestamp': normalize_timestamp(ts.next())})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity

        for method in ('GET', 'HEAD'):
            req = Request.blank('/sda1/p/a', method=method)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)
            for key in resp.headers:
                self.assert_('storage-policy' not in key.lower())

    def test_empty_except_for_used_policies(self):
        ts = itertools.count()
        # create the account
        req = Request.blank('/sda1/p/a', method='PUT', headers={
            'X-Timestamp': normalize_timestamp(ts.next())})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity

        # starts empty
        for method in ('GET', 'HEAD'):
            req = Request.blank('/sda1/p/a', method=method)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)
            for key in resp.headers:
                self.assert_('storage-policy' not in key.lower())

        # add a container
        policy = random.choice(POLICIES)
        req = Request.blank('/sda1/p/a/c1', method='PUT', headers={
            'X-Put-Timestamp': normalize_timestamp(ts.next()),
            'X-Delete-Timestamp': '0',
            'X-Object-Count': '2',
            'X-Bytes-Used': '4',
            'X-Backend-Storage-Policy-Index': policy.idx,
        })
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)

        # only policy of the created container should be in headers
        for method in ('GET', 'HEAD'):
            req = Request.blank('/sda1/p/a', method=method)
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int // 100, 2)
            for key in resp.headers:
                if 'storage-policy' in key.lower():
                    self.assert_(policy.name.lower() in key.lower())

    def test_multiple_policies_in_use(self):
        ts = itertools.count()
        # create the account
        req = Request.blank('/sda1/p/a', method='PUT', headers={
            'X-Timestamp': normalize_timestamp(ts.next())})
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int, 201)  # sanity

        # add some containers
        for policy in POLICIES:
            count = policy.idx * 100  # good as any integer
            container_path = '/sda1/p/a/c_%s' % policy.name
            req = Request.blank(
                container_path, method='PUT', headers={
                    'X-Put-Timestamp': normalize_timestamp(ts.next()),
                    'X-Delete-Timestamp': '0',
                    'X-Object-Count': count,
                    'X-Bytes-Used': count,
                    'X-Backend-Storage-Policy-Index': policy.idx,
                })
            resp = req.get_response(self.controller)
            self.assertEqual(resp.status_int, 201)

        req = Request.blank('/sda1/p/a', method='HEAD')
        resp = req.get_response(self.controller)
        self.assertEqual(resp.status_int // 100, 2)

        # check container counts in roll up headers
        total_object_count = 0
        total_bytes_used = 0
        for key in resp.headers:
            if 'storage-policy' not in key.lower():
                continue
            for policy in POLICIES:
                if policy.name.lower() not in key.lower():
                    continue
                if key.lower().endswith('object-count'):
                    object_count = int(resp.headers[key])
                    self.assertEqual(policy.idx * 100, object_count)
                    total_object_count += object_count
                if key.lower().endswith('bytes-used'):
                    bytes_used = int(resp.headers[key])
                    self.assertEqual(policy.idx * 100, bytes_used)
                    total_bytes_used += bytes_used

        expected_total_count = sum([p.idx * 100 for p in POLICIES])
        self.assertEqual(expected_total_count, total_object_count)
        self.assertEqual(expected_total_count, total_bytes_used)


@patch_policies([StoragePolicy(0, 'zero', False),
                 StoragePolicy(1, 'one', True),
                 StoragePolicy(2, 'two', False),
                 StoragePolicy(3, 'three', False)])
class TestNonLegacyDefaultStoragePolicy(TestAccountController):

    pass

if __name__ == '__main__':
    unittest.main()
