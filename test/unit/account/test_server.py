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

    def test_DELETE_empty(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'DELETE',
            'HTTP_X_TIMESTAMP': '1'})
        resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

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

    def test_HEAD_not_found(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'HEAD'})
        resp = self.controller.HEAD(req)
        self.assertEquals(resp.status_int, 404)

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

    def test_PUT_not_found(self):
        req = Request.blank('/sda1/p/a/c', environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-PUT-Timestamp': normalize_timestamp(1),
                     'X-DELETE-Timestamp': normalize_timestamp(0),
                     'X-Object-Count': '1',
                     'X-Bytes-Used': '1',
                     'X-Timestamp': normalize_timestamp(0)})
        resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 404)

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

    def test_GET_not_found_plain(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 404)

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

    def test_GET_empty_account_json(self):
        req = Request.blank('/sda1/p/a?format=json',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=json',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)

    def test_GET_empty_account_xml(self):
        req = Request.blank('/sda1/p/a?format=xml',
            environ={'REQUEST_METHOD': 'PUT', 'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
        req = Request.blank('/sda1/p/a?format=xml',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)

    def test_GET_over_limit(self):
        req = Request.blank('/sda1/p/a', environ={'REQUEST_METHOD': 'PUT',
            'HTTP_X_TIMESTAMP': '0'})
        self.controller.PUT(req)
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

    def test_GET_prefix_delimeter_plain(self):
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

    def test_GET_prefix_delimeter_json(self):
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

    def test_GET_prefix_delimeter_xml(self):
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

    def test_invalid_method_doesnt_exist(self):
        inbuf = StringIO()
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
        inbuf = StringIO()
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
        self.controller.PUT(Request.blank('/sda1/p/a',
                            headers={'X-Timestamp': normalize_timestamp(1)},
                            environ={'REQUEST_METHOD': 'PUT'}))
        for param in ('delimiter', 'limit', 'marker', 'prefix'):
            req = Request.blank('/sda1/p/a?%s=\xce' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assertEquals(resp.status_int, 400)
            req = Request.blank('/sda1/p/a?%s=\xce\xa9' % param,
                                environ={'REQUEST_METHOD': 'GET'})
            resp = self.controller.GET(req)
            self.assert_(resp.status_int in (204, 412), resp.status_int)

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


if __name__ == '__main__':
    unittest.main()
