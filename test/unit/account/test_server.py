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

import errno
import os
import mock
import unittest
from shutil import rmtree
from StringIO import StringIO

import simplejson
import xml.dom.minidom

from swift.common.swob import Request
from swift.account.server import AccountController, ACCOUNT_LISTING_LIMIT
from swift.common.utils import normalize_timestamp


class TestAccountController(unittest.TestCase):
    """ Test swift.account_server.AccountController """
    def setUp(self):
        """ Set up for testing swift.account_server.AccountController """
        self.testdir = os.path.join(os.path.dirname(__file__), 'account_server')
        self.controller = AccountController(
            {'devices': self.testdir, 'mount_check': 'false'})

    def tearDown(self):
        """ Tear down for testing swift.account_server.AccountController """
        try:
            rmtree(self.testdir)
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise

    def test_DELETE_not_found(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

    def test_DELETE_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_DELETE_not_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        # We now allow deleting non-empty accounts
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_DELETE_now_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Put-Timestamp': '1',
                     'X-Delete-Timestamp': '2',
                     'X-Object-Count': '0',
                     'X-Bytes-Used': '0',
                     'X-Timestamp': normalize_timestamp(0)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_DELETE_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 400)

    def test_DELETE_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': 'not-float'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 400)

    def test_DELETE_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 507)

    def test_HEAD_not_found(self):
        # Test the case in which account does not exist (can be recreated)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

        # Test the case in which account was deleted but not yet reaped
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_HEAD_empty_account(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['x-account-container-count'], '0')
        self.assertEquals(resp.headers['x-account-object-count'], '0')
        self.assertEquals(resp.headers['x-account-bytes-used'], '0')

    def test_HEAD_with_containers(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['x-account-container-count'], '2')
        self.assertEquals(resp.headers['x-account-object-count'], '0')
        self.assertEquals(resp.headers['x-account-bytes-used'], '0')
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD',
            'HTTP_X_TIMESTAMP': '5'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['x-account-container-count'], '2')
        self.assertEquals(resp.headers['x-account-object-count'], '4')
        self.assertEquals(resp.headers['x-account-bytes-used'], '6')

    def test_HEAD_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'HEAD',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 400)

    def test_HEAD_invalid_content_type(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Accept': 'application/plain'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 406)

    def test_HEAD_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'HEAD',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 507)

    def test_HEAD_invalid_format(self):
        format = '%D1%BD%8A9'  # invalid UTF-8; should be %E1%BD%8A9 (E -> D)
        req = Request.blank('/sda1/p/a?format=' + format,
                            environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_not_found(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-PUT-Timestamp': normalize_timestamp(1),
                     'X-DELETE-Timestamp': normalize_timestamp(0),
                     'X-Object-Count': '1',
                     'X-Bytes-Used': '1',
                     'X-Timestamp': normalize_timestamp(0)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

    def test_PUT(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)

    def test_PUT_after_DELETE(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Timestamp': normalize_timestamp(2)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'Recently deleted')
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_PUT_GET_metadata(self):
        # Set metadata header
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Account-Meta-Test': 'Value'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'Value')
        # Set another metadata header, ensuring old one doesn't disappear
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Account-Meta-Test2': 'Value2'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'Value')
        self.assertEquals(resp.headers.get('x-account-meta-test2'), 'Value2')
        # Update metadata header
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Account-Meta-Test': 'New Value'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'New Value')
        # Send old update to metadata header
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Account-Meta-Test': 'Old Value'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Account-Meta-Test': ''})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 202)
        req = Request.blank('/sda1/p/a')
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assert_('x-account-meta-test' not in resp.headers)

    def test_PUT_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 507)

    def test_POST_HEAD_metadata(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': normalize_timestamp(1)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        # Set metadata header
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(1),
                     'X-Account-Meta-Test': 'Value'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'Value')
        # Update metadata header
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(3),
                     'X-Account-Meta-Test': 'New Value'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'New Value')
        # Send old update to metadata header
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(2),
                     'X-Account-Meta-Test': 'Old Value'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-account-meta-test'), 'New Value')
        # Remove metadata header (by setting it to empty)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Timestamp': normalize_timestamp(4),
                     'X-Account-Meta-Test': ''})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 204)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 204)
        self.assert_('x-account-meta-test' not in resp.headers)

    def test_POST_invalid_partition(self):
        req = Request.blank('/sda1/./a', environ={'REQUEST_METHOD': 'POST',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 400)

    def test_POST_timestamp_not_float(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST',
                                                  'HTTP_X_TIMESTAMP': '0'},
                            headers={'X-Timestamp': 'not-float'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 400)

    def test_POST_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'POST',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 507)

    def test_POST_after_DELETE_not_found(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'POST',
                                                  'HTTP_X_TIMESTAMP': '2'})
        resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_GET_not_found_plain(self):
        # Test the case in which account does not exist (can be recreated)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('X-Account-Status' not in resp.headers)

        # Test the case in which account was deleted but not yet reaped
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                                                  'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
                                                  'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(resp.headers['X-Account-Status'], 'Deleted')

    def test_GET_not_found_json(self):
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)

    def test_GET_not_found_xml(self):
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)

    def test_GET_empty_account_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers['Content-Type'],
                          'text/plain; charset=utf-8')

    def test_GET_empty_account_json(self):
        req = Request.blank('/sda1/p/a?format=json',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['Content-Type'],
                          'application/json; charset=utf-8')

    def test_GET_empty_account_xml(self):
        req = Request.blank('/sda1/p/a?format=xml',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['Content-Type'],
                          'application/xml; charset=utf-8')

    def test_GET_over_limit(self):
        req = Request.blank('/sda1/p/a?limit=%d' %
            (ACCOUNT_LISTING_LIMIT + 1), environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 412)

    def test_GET_with_containers_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'), ['c1', 'c2'])
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'), ['c1', 'c2'])
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.charset, 'utf-8')

        # test unknown format uses default plain
        req = Request.blank('/sda1/p/a?format=somethinglese',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'), ['c1', 'c2'])
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.charset, 'utf-8')

    def test_GET_with_containers_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(simplejson.loads(resp.body),
                          [{'count': 0, 'bytes': 0, 'name': 'c1'},
                           {'count': 0, 'bytes': 0, 'name': 'c2'}])
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(simplejson.loads(resp.body),
                          [{'count': 1, 'bytes': 2, 'name': 'c1'},
                           {'count': 3, 'bytes': 4, 'name': 'c2'}])
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(resp.charset, 'utf-8')

    def test_GET_with_containers_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.content_type, 'application/xml')
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEquals(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEquals(len(listing), 2)
        self.assertEquals(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c1')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '0')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '0')
        self.assertEquals(listing[-1].nodeName, 'container')
        container = \
            [n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c2')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '0')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '0')
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '1',
                                     'X-Bytes-Used': '2',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '2',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '3',
                                     'X-Bytes-Used': '4',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEquals(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEquals(len(listing), 2)
        self.assertEquals(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c1')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '1')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '2')
        self.assertEquals(listing[-1].nodeName, 'container')
        container = [n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c2')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '3')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '4')
        self.assertEquals(resp.charset, 'utf-8')

    def test_GET_xml_escapes_account_name(self):
        req = Request.blank(
            '/sda1/p/%22%27',   # "'
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)

        req = Request.blank(
            '/sda1/p/%22%27?format=xml',
            environ={'REQUEST_METHOD': 'GET', 'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.GET(req)

        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEquals(dom.firstChild.attributes['name'].value, '"\'')

    def test_GET_xml_escapes_container_name(self):
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)

        req = Request.blank(
            '/sda1/p/a/%22%3Cword',  # "<word
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                     'HTTP_X_PUT_TIMESTAMP': '1', 'HTTP_X_OBJECT_COUNT': '0',
                     'HTTP_X_DELETE_TIMESTAMP': '0', 'HTTP_X_BYTES_USED': '1'})
        self.controller.PUT(req)

        req = Request.blank(
            '/sda1/p/a?format=xml',
            environ={'REQUEST_METHOD': 'GET', 'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.GET(req)
        dom = xml.dom.minidom.parseString(resp.body)

        self.assertEquals(
            dom.firstChild.firstChild.nextSibling.firstChild.firstChild.data,
            '"<word')

    def test_GET_xml_escapes_container_name_as_subdir(self):
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)

        req = Request.blank(
            '/sda1/p/a/%22%3Cword-test',  # "<word-test
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '1',
                     'HTTP_X_PUT_TIMESTAMP': '1', 'HTTP_X_OBJECT_COUNT': '0',
                     'HTTP_X_DELETE_TIMESTAMP': '0', 'HTTP_X_BYTES_USED': '1'})
        self.controller.PUT(req)

        req = Request.blank(
            '/sda1/p/a?format=xml&delimiter=-',
            environ={'REQUEST_METHOD': 'GET', 'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.GET(req)
        dom = xml.dom.minidom.parseString(resp.body)

        self.assertEquals(
            dom.firstChild.firstChild.nextSibling.attributes['name'].value,
            '"<word-')

    def test_GET_limit_marker_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        for c in xrange(5):
            req = Request.blank('/sda1/p/a/c%d' % c,
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Put-Timestamp': str(c + 1),
                                         'X-Delete-Timestamp': '0',
                                         'X-Object-Count': '2',
                                         'X-Bytes-Used': '3',
                                         'X-Timestamp': normalize_timestamp(0)})
            self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?limit=3',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'), ['c0', 'c1', 'c2'])
        req = Request.blank('/sda1/p/a?limit=3&marker=c2',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'), ['c3', 'c4'])

    def test_GET_limit_marker_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        for c in xrange(5):
            req = Request.blank('/sda1/p/a/c%d' % c,
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Put-Timestamp': str(c + 1),
                                         'X-Delete-Timestamp': '0',
                                         'X-Object-Count': '2',
                                         'X-Bytes-Used': '3',
                                         'X-Timestamp': normalize_timestamp(0)})
            self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?limit=3&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(simplejson.loads(resp.body),
                          [{'count': 2, 'bytes': 3, 'name': 'c0'},
                           {'count': 2, 'bytes': 3, 'name': 'c1'},
                           {'count': 2, 'bytes': 3, 'name': 'c2'}])
        req = Request.blank('/sda1/p/a?limit=3&marker=c2&format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(simplejson.loads(resp.body),
                          [{'count': 2, 'bytes': 3, 'name': 'c3'},
                           {'count': 2, 'bytes': 3, 'name': 'c4'}])

    def test_GET_limit_marker_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        for c in xrange(5):
            req = Request.blank('/sda1/p/a/c%d' % c,
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Put-Timestamp': str(c + 1),
                                         'X-Delete-Timestamp': '0',
                                         'X-Object-Count': '2',
                                         'X-Bytes-Used': '3',
                                         'X-Timestamp': normalize_timestamp(c)})
            self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?limit=3&format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEquals(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEquals(len(listing), 3)
        self.assertEquals(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c0')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '3')
        self.assertEquals(listing[-1].nodeName, 'container')
        container = [n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c2')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '3')
        req = Request.blank('/sda1/p/a?limit=3&marker=c2&format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEquals(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEquals(len(listing), 2)
        self.assertEquals(listing[0].nodeName, 'container')
        container = [n for n in listing[0].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c3')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '3')
        self.assertEquals(listing[-1].nodeName, 'container')
        container = [n for n in listing[-1].childNodes if n.nodeName != '#text']
        self.assertEquals(sorted([n.nodeName for n in container]),
                          ['bytes', 'count', 'name'])
        node = [n for n in container if n.nodeName == 'name'][0]
        self.assertEquals(node.firstChild.nodeValue, 'c4')
        node = [n for n in container if n.nodeName == 'count'][0]
        self.assertEquals(node.firstChild.nodeValue, '2')
        node = [n for n in container if n.nodeName == 'bytes'][0]
        self.assertEquals(node.firstChild.nodeValue, '3')

    def test_GET_accept_wildcard(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = '*/*'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, 'c1\n')

    def test_GET_accept_application_wildcard(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        resp = self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/*'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(len(simplejson.loads(resp.body)), 1)

    def test_GET_accept_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(len(simplejson.loads(resp.body)), 1)

    def test_GET_accept_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/xml'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        self.assertEquals(dom.firstChild.nodeName, 'account')
        listing = \
            [n for n in dom.firstChild.childNodes if n.nodeName != '#text']
        self.assertEquals(len(listing), 1)

    def test_GET_accept_conflicting(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=plain',
                            environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/json'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, 'c1\n')

    def test_GET_accept_not_valid(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a/c1', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Put-Timestamp': '1',
                                     'X-Delete-Timestamp': '0',
                                     'X-Object-Count': '0',
                                     'X-Bytes-Used': '0',
                                     'X-Timestamp': normalize_timestamp(0)})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        req.accept = 'application/xml*'
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 406)

    def test_GET_delimiter_too_long(self):
        req = Request.blank('/sda1/p/a?delimiter=xx',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 412)

    def test_GET_prefix_delimiter_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for first in range(3):
            req = Request.blank('/sda1/p/a/sub.%s' % first,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': '1',
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '0',
                         'X-Bytes-Used': '0',
                         'X-Timestamp': normalize_timestamp(0)})
            self.controller.PUT(req)
            for second in range(3):
                req = Request.blank('/sda1/p/a/sub.%s.%s' % (first, second),
                    environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Put-Timestamp': '1',
                             'X-Delete-Timestamp': '0',
                             'X-Object-Count': '0',
                             'X-Bytes-Used': '0',
                             'X-Timestamp': normalize_timestamp(0)})
                self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?delimiter=.',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'), ['sub.'])
        req = Request.blank('/sda1/p/a?prefix=sub.&delimiter=.',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'),
            ['sub.0', 'sub.0.', 'sub.1', 'sub.1.', 'sub.2', 'sub.2.'])
        req = Request.blank('/sda1/p/a?prefix=sub.1.&delimiter=.',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body.strip().split('\n'),
            ['sub.1.0', 'sub.1.1', 'sub.1.2'])

    def test_GET_prefix_delimiter_json(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
                'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for first in range(3):
            req = Request.blank('/sda1/p/a/sub.%s' % first,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': '1',
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '0',
                         'X-Bytes-Used': '0',
                         'X-Timestamp': normalize_timestamp(0)})
            self.controller.PUT(req)
            for second in range(3):
                req = Request.blank('/sda1/p/a/sub.%s.%s' % (first, second),
                    environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Put-Timestamp': '1',
                             'X-Delete-Timestamp': '0',
                             'X-Object-Count': '0',
                             'X-Bytes-Used': '0',
                             'X-Timestamp': normalize_timestamp(0)})
                self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?delimiter=.&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals([n.get('name', 's:' + n.get('subdir', 'error'))
                           for n in simplejson.loads(resp.body)], ['s:sub.'])
        req = Request.blank('/sda1/p/a?prefix=sub.&delimiter=.&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals([n.get('name', 's:' + n.get('subdir', 'error'))
                           for n in simplejson.loads(resp.body)],
            ['sub.0', 's:sub.0.', 'sub.1', 's:sub.1.', 'sub.2', 's:sub.2.'])
        req = Request.blank('/sda1/p/a?prefix=sub.1.&delimiter=.&format=json',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals([n.get('name', 's:' + n.get('subdir', 'error'))
                           for n in simplejson.loads(resp.body)],
            ['sub.1.0', 'sub.1.1', 'sub.1.2'])

    def test_GET_prefix_delimiter_xml(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        resp = self.controller.PUT(req)
        for first in range(3):
            req = Request.blank('/sda1/p/a/sub.%s' % first,
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'X-Put-Timestamp': '1',
                         'X-Delete-Timestamp': '0',
                         'X-Object-Count': '0',
                         'X-Bytes-Used': '0',
                         'X-Timestamp': normalize_timestamp(0)})
            self.controller.PUT(req)
            for second in range(3):
                req = Request.blank('/sda1/p/a/sub.%s.%s' % (first, second),
                    environ={'REQUEST_METHOD': 'PUT'},
                    headers={'X-Put-Timestamp': '1',
                             'X-Delete-Timestamp': '0',
                             'X-Object-Count': '0',
                             'X-Bytes-Used': '0',
                             'X-Timestamp': normalize_timestamp(0)})
                self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?delimiter=.&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        listing = []
        for node1 in dom.firstChild.childNodes:
            if node1.nodeName == 'subdir':
                listing.append('s:' + node1.attributes['name'].value)
            elif node1.nodeName == 'container':
                for node2 in node1.childNodes:
                    if node2.nodeName == 'name':
                        listing.append(node2.firstChild.nodeValue)
        self.assertEquals(listing, ['s:sub.'])
        req = Request.blank('/sda1/p/a?prefix=sub.&delimiter=.&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        listing = []
        for node1 in dom.firstChild.childNodes:
            if node1.nodeName == 'subdir':
                listing.append('s:' + node1.attributes['name'].value)
            elif node1.nodeName == 'container':
                for node2 in node1.childNodes:
                    if node2.nodeName == 'name':
                        listing.append(node2.firstChild.nodeValue)
        self.assertEquals(listing,
            ['sub.0', 's:sub.0.', 'sub.1', 's:sub.1.', 'sub.2', 's:sub.2.'])
        req = Request.blank('/sda1/p/a?prefix=sub.1.&delimiter=.&format=xml',
            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)
        dom = xml.dom.minidom.parseString(resp.body)
        listing = []
        for node1 in dom.firstChild.childNodes:
            if node1.nodeName == 'subdir':
                listing.append('s:' + node1.attributes['name'].value)
            elif node1.nodeName == 'container':
                for node2 in node1.childNodes:
                    if node2.nodeName == 'name':
                        listing.append(node2.firstChild.nodeValue)
        self.assertEquals(listing, ['sub.1.0', 'sub.1.1', 'sub.1.2'])

    def test_GET_insufficient_storage(self):
        self.controller = AccountController({'devices': self.testdir})
        req = Request.blank('/sda-null/p/a', environ={'REQUEST_METHOD': 'GET',
                                                      'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 507)

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
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '404 ')

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
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '400 ')

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
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '412 ')

    def test_invalid_method_doesnt_exist(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': 'method_doesnt_exist',
                                  'PATH_INFO': '/sda1/p/a'},
                                 start_response)
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_invalid_method_is_not_public(self):
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            outbuf.writelines(args)

        self.controller.__call__({'REQUEST_METHOD': '__init__',
                                  'PATH_INFO': '/sda1/p/a'},
                                 start_response)
        self.assertEquals(errbuf.getvalue(), '')
        self.assertEquals(outbuf.getvalue()[:4], '405 ')

    def test_params_format(self):
        self.controller.PUT(Request.blank('/sda1/p/a',
                            headers={'X-Timestamp': normalize_timestamp(1)},
                            environ={'REQUEST_METHOD': 'PUT'}))
        for format in ('xml', 'json'):
            req = Request.blank('/sda1/p/a?format=%s' % format,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assertEquals(resp.status_int, 200)

    def test_params_utf8(self):
        # Bad UTF8 sequence, all parameters should cause 400 error
        for param in ('delimiter', 'limit', 'marker', 'prefix', 'end_marker',
                      'format'):
            req = Request.blank('/sda1/p/a?%s=\xce' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assertEquals(resp.status_int, 400,
                              "%d on param %s" % (resp.status_int, param))
        # Good UTF8 sequence for delimiter, too long (1 byte delimiters only)
        req = Request.blank('/sda1/p/a?delimiter=\xce\xa9',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 412,
                          "%d on param delimiter" % (resp.status_int))
        self.controller.PUT(Request.blank('/sda1/p/a',
                            headers={'X-Timestamp': normalize_timestamp(1)},
                            environ={'REQUEST_METHOD': 'PUT'}))
        # Good UTF8 sequence, ignored for limit, doesn't affect other queries
        for param in ('limit', 'marker', 'prefix', 'end_marker', 'format'):
            req = Request.blank('/sda1/p/a?%s=\xce\xa9' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assertEquals(resp.status_int, 204,
                              "%d on param %s" % (resp.status_int, param))

    def test_put_auto_create(self):
        headers = {'x-put-timestamp': normalize_timestamp(1),
                   'x-delete-timestamp': normalize_timestamp(0),
                   'x-object-count': '0',
                   'x-bytes-used': '0'}

        resp = self.controller.PUT(Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

        resp = self.controller.PUT(Request.blank('/sda1/p/.a/c',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 201)

        resp = self.controller.PUT(Request.blank('/sda1/p/a/.c',
            environ={'REQUEST_METHOD': 'PUT'}, headers=dict(headers)))
        self.assertEquals(resp.status_int, 404)

    def test_content_type_on_HEAD(self):
        self.controller.PUT(Request.blank('/sda1/p/a',
                            headers={'X-Timestamp': normalize_timestamp(1)},
                            environ={'REQUEST_METHOD': 'PUT'}))

        env = {'REQUEST_METHOD': 'HEAD'}

        req = Request.blank('/sda1/p/a?format=xml', environ=env)
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.content_type, 'application/xml')

        req = Request.blank('/sda1/p/a?format=json', environ=env)
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank('/sda1/p/a', environ=env)
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a', headers={'Accept': 'application/json'}, environ=env)
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.content_type, 'application/json')
        self.assertEquals(resp.charset, 'utf-8')

        req = Request.blank(
            '/sda1/p/a', headers={'Accept': 'application/xml'}, environ=env)
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.content_type, 'application/xml')
        self.assertEquals(resp.charset, 'utf-8')

    def test_serv_reserv(self):
        """
        Test replication_server flag
        was set from configuration file.
        """
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertEquals(AccountController(conf).replication_server, None)
        for val in [True, '1', 'True', 'true']:
            conf['replication_server'] = val
            self.assertTrue(AccountController(conf).replication_server)
        for val in [False, 0, '0', 'False', 'false', 'test_string']:
            conf['replication_server'] = val
            self.assertFalse(AccountController(conf).replication_server)

    def test_list_allowed_methods(self):
        """ Test list of allowed_methods """
        methods = ['DELETE', 'PUT', 'HEAD', 'GET', 'REPLICATE', 'POST']
        self.assertEquals(self.controller.allowed_methods, methods)

    def test_allowed_methods_from_configuration_file(self):
        """
        Test list of allowed_methods which
        were set from configuration file.
        """
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.assertEquals(AccountController(conf).allowed_methods,
                          ['DELETE', 'PUT', 'HEAD', 'GET', 'REPLICATE',
                           'POST'])
        conf['replication_server'] = 'True'
        self.assertEquals(AccountController(conf).allowed_methods,
                          ['REPLICATE'])
        conf['replication_server'] = 'False'
        self.assertEquals(AccountController(conf).allowed_methods,
                          ['DELETE', 'PUT', 'HEAD', 'GET', 'POST'])

    def test_correct_allowed_method(self):
        """
        Test correct work for allowed method using
        swift.account_server.AccountController.__call__
        """
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            """ Sends args to outbuf """
            outbuf.writelines(args)

        method = self.controller.allowed_methods[0]

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

        with mock.patch.object(self.controller, method,
                               return_value=mock.MagicMock()) as mock_method:
            response = self.controller.__call__(env, start_response)
            self.assertNotEqual(response, answer)
            self.assertEqual(mock_method.call_count, 1)

    def test_not_allowed_method(self):
        """
        Test correct work for NOT allowed method using
        swift.account_server.AccountController.__call__
        """
        inbuf = StringIO()
        errbuf = StringIO()
        outbuf = StringIO()

        def start_response(*args):
            """ Sends args to outbuf """
            outbuf.writelines(args)

        method = self.controller.allowed_methods[0]

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

        with mock.patch.object(self.controller, method,
                               return_value=mock.MagicMock()) as mock_method:
            self.controller.allowed_methods.remove(method)
            response = self.controller.__call__(env, start_response)
            self.assertEqual(mock_method.call_count, 0)
            self.assertEqual(response, answer)
            self.controller.allowed_methods.append(method)

if __name__ == '__main__':
    unittest.main()
