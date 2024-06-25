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

"""Tests for swift.common.request_helpers"""
import argparse
import unittest
from swift.common.swob import Request, HTTPException, HeaderKeyDict, HTTPOk
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY
from swift.common import request_helpers as rh
from swift.common.constraints import AUTO_CREATE_ACCOUNT_PREFIX

from test.debug_logger import debug_logger
from test.unit import patch_policies
from test.unit.common.test_utils import FakeResponse
from test.unit.common.middleware.helpers import FakeSwift


server_types = ['account', 'container', 'object']


class TestRequestHelpers(unittest.TestCase):

    def test_append_log_info(self):
        req = Request.blank('/v/a/c/o')
        self.assertNotIn('swift.log_info', req.environ)
        rh.append_log_info(req.environ, 'msg1')
        self.assertEqual(['msg1'], req.environ.get('swift.log_info'))
        rh.append_log_info(req.environ, 'msg2')
        self.assertEqual(['msg1', 'msg2'], req.environ.get('swift.log_info'))

    def test_get_log_info(self):
        req = Request.blank('/v/a/c/o')
        self.assertEqual('', rh.get_log_info(req.environ))
        req.environ['swift.log_info'] = ['msg1']
        self.assertEqual('msg1', rh.get_log_info(req.environ))
        rh.append_log_info(req.environ, 'msg2')
        self.assertEqual('msg1,msg2', rh.get_log_info(req.environ))

    def test_constrain_req_limit(self):
        req = Request.blank('')
        self.assertEqual(10, rh.constrain_req_limit(req, 10))
        req = Request.blank('', query_string='limit=1')
        self.assertEqual(1, rh.constrain_req_limit(req, 10))
        req = Request.blank('', query_string='limit=1.0')
        self.assertEqual(10, rh.constrain_req_limit(req, 10))
        req = Request.blank('', query_string='limit=11')
        with self.assertRaises(HTTPException) as raised:
            rh.constrain_req_limit(req, 10)
        self.assertEqual(raised.exception.status_int, 412)

    def test_validate_params(self):
        req = Request.blank('')
        actual = rh.validate_params(req, ('limit', 'marker', 'end_marker'))
        self.assertEqual({}, actual)

        req = Request.blank('', query_string='limit=1&junk=here&marker=foo')
        actual = rh.validate_params(req, ())
        self.assertEqual({}, actual)

        req = Request.blank('', query_string='limit=1&junk=here&marker=foo')
        actual = rh.validate_params(req, ('limit', 'marker', 'end_marker'))
        expected = {'limit': '1', 'marker': 'foo'}
        self.assertEqual(expected, actual)

        req = Request.blank('', query_string='limit=1&junk=here&marker=')
        actual = rh.validate_params(req, ('limit', 'marker', 'end_marker'))
        expected = {'limit': '1', 'marker': ''}
        self.assertEqual(expected, actual)

        # ignore bad junk
        req = Request.blank('', query_string='limit=1&junk=%ff&marker=foo')
        actual = rh.validate_params(req, ('limit', 'marker', 'end_marker'))
        expected = {'limit': '1', 'marker': 'foo'}
        self.assertEqual(expected, actual)

        # error on bad wanted parameter
        req = Request.blank('', query_string='limit=1&junk=here&marker=%ff')
        with self.assertRaises(HTTPException) as raised:
            rh.validate_params(req, ('limit', 'marker', 'end_marker'))
        self.assertEqual(raised.exception.status_int, 400)

    def test_validate_container_params(self):
        req = Request.blank('')
        actual = rh.validate_container_params(req)
        self.assertEqual({'limit': 10000}, actual)

        req = Request.blank('', query_string='limit=1&junk=here&marker=foo')
        actual = rh.validate_container_params(req)
        expected = {'limit': 1, 'marker': 'foo'}
        self.assertEqual(expected, actual)

        req = Request.blank('', query_string='limit=1&junk=here&marker=')
        actual = rh.validate_container_params(req)
        expected = {'limit': 1, 'marker': ''}
        self.assertEqual(expected, actual)

        # ignore bad junk
        req = Request.blank('', query_string='limit=1&junk=%ff&marker=foo')
        actual = rh.validate_container_params(req)
        expected = {'limit': 1, 'marker': 'foo'}
        self.assertEqual(expected, actual)

        # error on bad wanted parameter
        req = Request.blank('', query_string='limit=1&junk=here&marker=%ff')
        with self.assertRaises(HTTPException) as raised:
            rh.validate_container_params(req)
        self.assertEqual(raised.exception.status_int, 400)

        # error on bad limit
        req = Request.blank('', query_string='limit=10001')
        with self.assertRaises(HTTPException) as raised:
            rh.validate_container_params(req)
        self.assertEqual(raised.exception.status_int, 412)

    def test_is_user_meta(self):
        m_type = 'meta'
        for st in server_types:
            self.assertTrue(rh.is_user_meta(st, 'x-%s-%s-foo' % (st, m_type)))
            self.assertFalse(rh.is_user_meta(st, 'x-%s-%s-' % (st, m_type)))
            self.assertFalse(rh.is_user_meta(st, 'x-%s-%sfoo' % (st, m_type)))

    def test_is_sys_meta(self):
        m_type = 'sysmeta'
        for st in server_types:
            self.assertTrue(rh.is_sys_meta(st, 'x-%s-%s-foo' % (st, m_type)))
            self.assertFalse(rh.is_sys_meta(st, 'x-%s-%s-' % (st, m_type)))
            self.assertFalse(rh.is_sys_meta(st, 'x-%s-%sfoo' % (st, m_type)))

    def test_is_sys_or_user_meta(self):
        m_types = ['sysmeta', 'meta']
        for mt in m_types:
            for st in server_types:
                self.assertTrue(rh.is_sys_or_user_meta(
                    st, 'x-%s-%s-foo' % (st, mt)))
                self.assertFalse(rh.is_sys_or_user_meta(
                    st, 'x-%s-%s-' % (st, mt)))
                self.assertFalse(rh.is_sys_or_user_meta(
                    st, 'x-%s-%sfoo' % (st, mt)))

    def test_strip_sys_meta_prefix(self):
        mt = 'sysmeta'
        for st in server_types:
            self.assertEqual(rh.strip_sys_meta_prefix(
                st, 'x-%s-%s-a' % (st, mt)), 'a')
        mt = 'not-sysmeta'
        for st in server_types:
            with self.assertRaises(ValueError):
                rh.strip_sys_meta_prefix(st, 'x-%s-%s-a' % (st, mt))

    def test_strip_user_meta_prefix(self):
        mt = 'meta'
        for st in server_types:
            self.assertEqual(rh.strip_user_meta_prefix(
                st, 'x-%s-%s-a' % (st, mt)), 'a')
        mt = 'not-meta'
        for st in server_types:
            with self.assertRaises(ValueError):
                rh.strip_sys_meta_prefix(st, 'x-%s-%s-a' % (st, mt))

    def test_is_object_transient_sysmeta(self):
        self.assertTrue(rh.is_object_transient_sysmeta(
            'x-object-transient-sysmeta-foo'))
        self.assertFalse(rh.is_object_transient_sysmeta(
            'x-object-transient-sysmeta-'))
        self.assertFalse(rh.is_object_transient_sysmeta(
            'x-object-meatmeta-foo'))

    def test_strip_object_transient_sysmeta_prefix(self):
        mt = 'object-transient-sysmeta'
        self.assertEqual(rh.strip_object_transient_sysmeta_prefix(
            'x-%s-a' % mt), 'a')

        mt = 'object-sysmeta-transient'
        with self.assertRaises(ValueError):
            rh.strip_object_transient_sysmeta_prefix('x-%s-a' % mt)

    def test_remove_items(self):
        src = {'a': 'b',
               'c': 'd'}
        test = lambda x: x == 'a'
        rem = rh.remove_items(src, test)
        self.assertEqual(src, {'c': 'd'})
        self.assertEqual(rem, {'a': 'b'})

    def test_copy_header_subset(self):
        src = {'a': 'b',
               'c': 'd'}
        from_req = Request.blank('/path', environ={}, headers=src)
        to_req = Request.blank('/path', {})
        test = lambda x: x.lower() == 'a'
        rh.copy_header_subset(from_req, to_req, test)
        self.assertTrue('A' in to_req.headers)
        self.assertEqual(to_req.headers['A'], 'b')
        self.assertFalse('c' in to_req.headers)
        self.assertFalse('C' in to_req.headers)

    def test_is_use_replication_network(self):
        self.assertFalse(rh.is_use_replication_network())
        self.assertFalse(rh.is_use_replication_network({}))
        self.assertFalse(rh.is_use_replication_network(
            {'x-backend-use-replication-network': 'false'}))
        self.assertFalse(rh.is_use_replication_network(
            {'x-backend-use-replication-network': 'no'}))

        self.assertTrue(rh.is_use_replication_network(
            {'x-backend-use-replication-network': 'true'}))
        self.assertTrue(rh.is_use_replication_network(
            {'x-backend-use-replication-network': 'yes'}))
        self.assertTrue(rh.is_use_replication_network(
            {'X-Backend-Use-Replication-Network': 'True'}))

    def test_get_ip_port(self):
        node = {
            'ip': '1.2.3.4',
            'port': 6000,
            'replication_ip': '5.6.7.8',
            'replication_port': 7000,
        }
        self.assertEqual(('1.2.3.4', 6000), rh.get_ip_port(node, {}))
        self.assertEqual(('5.6.7.8', 7000), rh.get_ip_port(node, {
            rh.USE_REPLICATION_NETWORK_HEADER: 'true'}))
        self.assertEqual(('1.2.3.4', 6000), rh.get_ip_port(node, {
            rh.USE_REPLICATION_NETWORK_HEADER: 'false'}))

        # node trumps absent header and False header
        node['use_replication'] = True
        self.assertEqual(('5.6.7.8', 7000), rh.get_ip_port(node, {}))
        self.assertEqual(('5.6.7.8', 7000), rh.get_ip_port(node, {
            rh.USE_REPLICATION_NETWORK_HEADER: 'false'}))

        # True header trumps node
        node['use_replication'] = False
        self.assertEqual(('5.6.7.8', 7000), rh.get_ip_port(node, {
            rh.USE_REPLICATION_NETWORK_HEADER: 'true'}))

    @patch_policies(with_ec_default=True)
    def test_get_name_and_placement_object_req(self):
        path = '/device/part/account/container/object'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '0'})
        device, part, account, container, obj, policy = \
            rh.get_name_and_placement(req, 5, 5, True)
        self.assertEqual(device, 'device')
        self.assertEqual(part, 'part')
        self.assertEqual(account, 'account')
        self.assertEqual(container, 'container')
        self.assertEqual(obj, 'object')
        self.assertEqual(policy, POLICIES[0])
        self.assertEqual(policy.policy_type, EC_POLICY)

        req.headers['X-Backend-Storage-Policy-Index'] = 1
        device, part, account, container, obj, policy = \
            rh.get_name_and_placement(req, 5, 5, True)
        self.assertEqual(device, 'device')
        self.assertEqual(part, 'part')
        self.assertEqual(account, 'account')
        self.assertEqual(container, 'container')
        self.assertEqual(obj, 'object')
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

        req.headers['X-Backend-Storage-Policy-Index'] = 'foo'
        with self.assertRaises(HTTPException) as raised:
            device, part, account, container, obj, policy = \
                rh.get_name_and_placement(req, 5, 5, True)
        e = raised.exception
        self.assertEqual(e.status_int, 503)
        self.assertEqual(str(e), '503 Service Unavailable')
        self.assertEqual(e.body, b"No policy with index foo")

    @patch_policies(with_ec_default=True)
    def test_get_name_and_placement_object_replication(self):
        # yup, suffixes are sent '-'.joined in the path
        path = '/device/part/012-345-678-9ab-cde'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '0'})
        device, partition, suffix_parts, policy = \
            rh.get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, '012-345-678-9ab-cde')
        self.assertEqual(policy, POLICIES[0])
        self.assertEqual(policy.policy_type, EC_POLICY)

        path = '/device/part'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '1'})
        device, partition, suffix_parts, policy = \
            rh.get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertIsNone(suffix_parts)  # false-y
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

        path = '/device/part/'  # with a trailing slash
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '1'})
        device, partition, suffix_parts, policy = \
            rh.get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, '')  # still false-y
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

    def test_validate_internal_name(self):
        self.assertIsNone(rh._validate_internal_name('foo'))
        self.assertIsNone(rh._validate_internal_name(
            rh.get_reserved_name('foo')))
        self.assertIsNone(rh._validate_internal_name(
            rh.get_reserved_name('foo', 'bar')))
        self.assertIsNone(rh._validate_internal_name(''))
        self.assertIsNone(rh._validate_internal_name(rh.RESERVED))

    def test_invalid_reserved_name(self):
        with self.assertRaises(HTTPException) as raised:
            rh._validate_internal_name('foo' + rh.RESERVED)
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace name")

    def test_validate_internal_account(self):
        self.assertIsNone(rh.validate_internal_account('AUTH_foo'))
        self.assertIsNone(rh.validate_internal_account(
            rh.get_reserved_name('AUTH_foo')))
        with self.assertRaises(HTTPException) as raised:
            rh.validate_internal_account('AUTH_foo' + rh.RESERVED)
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace account")

    def test_validate_internal_container(self):
        self.assertIsNone(rh.validate_internal_container('AUTH_foo', 'bar'))
        self.assertIsNone(rh.validate_internal_container(
            rh.get_reserved_name('AUTH_foo'), 'bar'))
        self.assertIsNone(rh.validate_internal_container(
            'foo', rh.get_reserved_name('bar')))
        self.assertIsNone(rh.validate_internal_container(
            rh.get_reserved_name('AUTH_foo'), rh.get_reserved_name('bar')))
        with self.assertRaises(HTTPException) as raised:
            rh.validate_internal_container('AUTH_foo' + rh.RESERVED, 'bar')
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace account")
        with self.assertRaises(HTTPException) as raised:
            rh.validate_internal_container('AUTH_foo', 'bar' + rh.RESERVED)
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace container")

        # These should always be operating on split_path outputs so this
        # shouldn't really be an issue, but just in case...
        for acct in ('', None):
            with self.assertRaises(ValueError) as raised:
                rh.validate_internal_container(
                    acct, 'bar')
            self.assertEqual(raised.exception.args[0], 'Account is required')

    def test_validate_internal_object(self):
        self.assertIsNone(rh.validate_internal_obj('AUTH_foo', 'bar', 'baz'))
        self.assertIsNone(rh.validate_internal_obj(
            rh.get_reserved_name('AUTH_foo'), 'bar', 'baz'))
        for acct in ('AUTH_foo', rh.get_reserved_name('AUTH_foo')):
            self.assertIsNone(rh.validate_internal_obj(
                acct,
                rh.get_reserved_name('bar'),
                rh.get_reserved_name('baz')))
        for acct in ('AUTH_foo', rh.get_reserved_name('AUTH_foo')):
            with self.assertRaises(HTTPException) as raised:
                rh.validate_internal_obj(
                    acct, 'bar', rh.get_reserved_name('baz'))
            e = raised.exception
            self.assertEqual(e.status_int, 400)
            self.assertEqual(str(e), '400 Bad Request')
            self.assertEqual(e.body, b"Invalid reserved-namespace object "
                             b"in user-namespace container")
        for acct in ('AUTH_foo', rh.get_reserved_name('AUTH_foo')):
            with self.assertRaises(HTTPException) as raised:
                rh.validate_internal_obj(
                    acct, rh.get_reserved_name('bar'), 'baz')
            e = raised.exception
            self.assertEqual(e.status_int, 400)
            self.assertEqual(str(e), '400 Bad Request')
            self.assertEqual(e.body, b"Invalid user-namespace object "
                             b"in reserved-namespace container")

        # These should always be operating on split_path outputs so this
        # shouldn't really be an issue, but just in case...
        for acct in ('', None):
            with self.assertRaises(ValueError) as raised:
                rh.validate_internal_obj(
                    acct, 'bar', 'baz')
            self.assertEqual(raised.exception.args[0], 'Account is required')

        for cont in ('', None):
            with self.assertRaises(ValueError) as raised:
                rh.validate_internal_obj(
                    'AUTH_foo', cont, 'baz')
            self.assertEqual(raised.exception.args[0], 'Container is required')

    def test_invalid_names_in_system_accounts(self):
        self.assertIsNone(rh.validate_internal_obj(
            AUTO_CREATE_ACCOUNT_PREFIX + 'system_account', 'foo',
            'crazy%stown' % rh.RESERVED))

    def test_invalid_reserved_names(self):
        with self.assertRaises(HTTPException) as raised:
            rh.validate_internal_obj('AUTH_foo' + rh.RESERVED, 'bar', 'baz')
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace account")
        with self.assertRaises(HTTPException) as raised:
            rh.validate_internal_obj('AUTH_foo', 'bar' + rh.RESERVED, 'baz')
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace container")
        with self.assertRaises(HTTPException) as raised:
            rh.validate_internal_obj('AUTH_foo', 'bar', 'baz' + rh.RESERVED)
        e = raised.exception
        self.assertEqual(e.status_int, 400)
        self.assertEqual(str(e), '400 Bad Request')
        self.assertEqual(e.body, b"Invalid reserved-namespace object")

    def test_get_reserved_name(self):
        expectations = {
            tuple(): rh.RESERVED,
            ('',): rh.RESERVED,
            ('foo',): rh.RESERVED + 'foo',
            ('foo', 'bar'): rh.RESERVED + 'foo' + rh.RESERVED + 'bar',
            ('foo', ''): rh.RESERVED + 'foo' + rh.RESERVED,
            ('', ''): rh.RESERVED * 2,
        }
        failures = []
        for parts, expected in expectations.items():
            name = rh.get_reserved_name(*parts)
            if name != expected:
                failures.append('get given %r expected %r != %r' % (
                    parts, expected, name))
        if failures:
            self.fail('Unexpected reults:\n' + '\n'.join(failures))

    def test_invalid_get_reserved_name(self):
        self.assertRaises(ValueError)
        with self.assertRaises(ValueError) as ctx:
            rh.get_reserved_name('foo', rh.RESERVED + 'bar', 'baz')
        self.assertEqual(str(ctx.exception),
                         'Invalid reserved part in components')

    def test_split_reserved_name(self):
        expectations = {
            rh.RESERVED: ('',),
            rh.RESERVED + 'foo': ('foo',),
            rh.RESERVED + 'foo' + rh.RESERVED + 'bar': ('foo', 'bar'),
            rh.RESERVED + 'foo' + rh.RESERVED: ('foo', ''),
            rh.RESERVED * 2: ('', ''),
        }
        failures = []
        for name, expected in expectations.items():
            parts = rh.split_reserved_name(name)
            if tuple(parts) != expected:
                failures.append('split given %r expected %r != %r' % (
                    name, expected, parts))
        if failures:
            self.fail('Unexpected reults:\n' + '\n'.join(failures))

    def test_invalid_split_reserved_name(self):
        self.assertRaises(ValueError)
        with self.assertRaises(ValueError) as ctx:
            rh.split_reserved_name('foo')
        self.assertEqual(str(ctx.exception),
                         'Invalid reserved name')

    def test_is_open_expired(self):
        app = argparse.Namespace(allow_open_expired=False)
        req = Request.blank('/v1/a/c/o', headers={'X-Open-Expired': 'yes'})
        self.assertFalse(rh.is_open_expired(app, req))
        req = Request.blank('/v1/a/c/o', headers={'X-Open-Expired': 'no'})
        self.assertFalse(rh.is_open_expired(app, req))
        req = Request.blank('/v1/a/c/o', headers={})
        self.assertFalse(rh.is_open_expired(app, req))

        app = argparse.Namespace(allow_open_expired=True)
        req = Request.blank('/v1/a/c/o', headers={'X-Open-Expired': 'no'})
        self.assertFalse(rh.is_open_expired(app, req))
        req = Request.blank('/v1/a/c/o', headers={})
        self.assertFalse(rh.is_open_expired(app, req))

        req = Request.blank('/v1/a/c/o', headers={'X-Open-Expired': 'yes'})
        self.assertTrue(rh.is_open_expired(app, req))

    def test_is_backend_open_expired(self):
        req = Request.blank('/v1/a/c/o', headers={
            'X-Backend-Open-Expired': 'yes'
        })
        self.assertTrue(rh.is_backend_open_expired(req))
        req = Request.blank('/v1/a/c/o', headers={
            'X-Backend-Open-Expired': 'no'
        })
        self.assertFalse(rh.is_backend_open_expired(req))

        req = Request.blank('/v1/a/c/o', headers={
            'X-Backend-Replication': 'yes'
        })
        self.assertTrue(rh.is_backend_open_expired(req))
        req = Request.blank('/v1/a/c/o', headers={
            'X-Backend-Replication': 'no'
        })
        self.assertFalse(rh.is_backend_open_expired(req))

        req = Request.blank('/v1/a/c/o', headers={})
        self.assertFalse(rh.is_backend_open_expired(req))


class TestHTTPResponseToDocumentIters(unittest.TestCase):
    def test_200(self):
        fr = FakeResponse(
            200,
            {'Content-Length': '10', 'Content-Type': 'application/lunch'},
            b'sandwiches')

        doc_iters = rh.http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 0)
        self.assertEqual(last_byte, 9)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Length'), '10')
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'sandwiches')

        self.assertRaises(StopIteration, next, doc_iters)

        fr = FakeResponse(
            200,
            {'Transfer-Encoding': 'chunked',
             'Content-Type': 'application/lunch'},
            b'sandwiches')

        doc_iters = rh.http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 0)
        self.assertIsNone(last_byte)
        self.assertIsNone(length)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Transfer-Encoding'), 'chunked')
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'sandwiches')

        self.assertRaises(StopIteration, next, doc_iters)

    def test_206_single_range(self):
        fr = FakeResponse(
            206,
            {'Content-Length': '8', 'Content-Type': 'application/lunch',
             'Content-Range': 'bytes 1-8/10'},
            b'andwiche')

        doc_iters = rh.http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 1)
        self.assertEqual(last_byte, 8)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Length'), '8')
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'andwiche')

        self.assertRaises(StopIteration, next, doc_iters)

        # Chunked response should be treated in the same way as non-chunked one
        fr = FakeResponse(
            206,
            {'Transfer-Encoding': 'chunked',
             'Content-Type': 'application/lunch',
             'Content-Range': 'bytes 1-8/10'},
            b'andwiche')

        doc_iters = rh.http_response_to_document_iters(fr)
        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 1)
        self.assertEqual(last_byte, 8)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'andwiche')

        self.assertRaises(StopIteration, next, doc_iters)

    def test_206_multiple_ranges(self):
        fr = FakeResponse(
            206,
            {'Content-Type': 'multipart/byteranges; boundary=asdfasdfasdf'},
            (b"--asdfasdfasdf\r\n"
             b"Content-Type: application/lunch\r\n"
             b"Content-Range: bytes 0-3/10\r\n"
             b"\r\n"
             b"sand\r\n"
             b"--asdfasdfasdf\r\n"
             b"Content-Type: application/lunch\r\n"
             b"Content-Range: bytes 6-9/10\r\n"
             b"\r\n"
             b"ches\r\n"
             b"--asdfasdfasdf--"))

        doc_iters = rh.http_response_to_document_iters(fr)

        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 0)
        self.assertEqual(last_byte, 3)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'sand')

        first_byte, last_byte, length, headers, body = next(doc_iters)
        self.assertEqual(first_byte, 6)
        self.assertEqual(last_byte, 9)
        self.assertEqual(length, 10)
        header_dict = HeaderKeyDict(headers)
        self.assertEqual(header_dict.get('Content-Type'), 'application/lunch')
        self.assertEqual(body.read(), b'ches')

        self.assertRaises(StopIteration, next, doc_iters)

    def test_update_etag_is_at_header(self):
        # start with no existing X-Backend-Etag-Is-At
        req = Request.blank('/v/a/c/o')
        rh.update_etag_is_at_header(req, 'X-Object-Sysmeta-My-Etag')
        self.assertEqual('X-Object-Sysmeta-My-Etag',
                         req.headers['X-Backend-Etag-Is-At'])
        # add another alternate
        rh.update_etag_is_at_header(req, 'X-Object-Sysmeta-Ec-Etag')
        self.assertEqual('X-Object-Sysmeta-My-Etag,X-Object-Sysmeta-Ec-Etag',
                         req.headers['X-Backend-Etag-Is-At'])
        with self.assertRaises(ValueError) as cm:
            rh.update_etag_is_at_header(req, 'X-Object-Sysmeta-,-Bad')
        self.assertEqual('Header name must not contain commas',
                         cm.exception.args[0])

    def test_resolve_etag_is_at_header(self):
        def do_test():
            req = Request.blank('/v/a/c/o')
            # ok to have no X-Backend-Etag-Is-At
            self.assertIsNone(rh.resolve_etag_is_at_header(req, metadata))

            # ok to have no matching metadata
            req.headers['X-Backend-Etag-Is-At'] = 'X-Not-There'
            self.assertIsNone(rh.resolve_etag_is_at_header(req, metadata))

            # selects from metadata
            req.headers['X-Backend-Etag-Is-At'] = 'X-Object-Sysmeta-Ec-Etag'
            self.assertEqual('an etag value',
                             rh.resolve_etag_is_at_header(req, metadata))
            req.headers['X-Backend-Etag-Is-At'] = 'X-Object-Sysmeta-My-Etag'
            self.assertEqual('another etag value',
                             rh.resolve_etag_is_at_header(req, metadata))

            # first in list takes precedence
            req.headers['X-Backend-Etag-Is-At'] = \
                'X-Object-Sysmeta-My-Etag,X-Object-Sysmeta-Ec-Etag'
            self.assertEqual('another etag value',
                             rh.resolve_etag_is_at_header(req, metadata))

            # non-existent alternates are passed over
            req.headers['X-Backend-Etag-Is-At'] = \
                'X-Bogus,X-Object-Sysmeta-My-Etag,X-Object-Sysmeta-Ec-Etag'
            self.assertEqual('another etag value',
                             rh.resolve_etag_is_at_header(req, metadata))

            # spaces in list are ok
            alts = 'X-Foo, X-Object-Sysmeta-My-Etag , X-Object-Sysmeta-Ec-Etag'
            req.headers['X-Backend-Etag-Is-At'] = alts
            self.assertEqual('another etag value',
                             rh.resolve_etag_is_at_header(req, metadata))

            # lower case in list is ok
            alts = alts.lower()
            req.headers['X-Backend-Etag-Is-At'] = alts
            self.assertEqual('another etag value',
                             rh.resolve_etag_is_at_header(req, metadata))

            # upper case in list is ok
            alts = alts.upper()
            req.headers['X-Backend-Etag-Is-At'] = alts
            self.assertEqual('another etag value',
                             rh.resolve_etag_is_at_header(req, metadata))

        metadata = {'X-Object-Sysmeta-Ec-Etag': 'an etag value',
                    'X-Object-Sysmeta-My-Etag': 'another etag value'}
        do_test()
        metadata = dict((k.lower(), v) for k, v in metadata.items())
        do_test()
        metadata = dict((k.upper(), v) for k, v in metadata.items())
        do_test()

    def test_ignore_range_header(self):
        req = Request.blank('/v/a/c/o')
        self.assertIsNone(req.headers.get(
            'X-Backend-Ignore-Range-If-Metadata-Present'))
        rh.update_ignore_range_header(req, 'X-Static-Large-Object')
        self.assertEqual('X-Static-Large-Object', req.headers.get(
            'X-Backend-Ignore-Range-If-Metadata-Present'))
        rh.update_ignore_range_header(req, 'X-Static-Large-Object')
        self.assertEqual(
            'X-Static-Large-Object,X-Static-Large-Object',
            req.headers.get('X-Backend-Ignore-Range-If-Metadata-Present'))
        rh.update_ignore_range_header(req, 'X-Object-Sysmeta-Slo-Etag')
        self.assertEqual(
            'X-Static-Large-Object,X-Static-Large-Object,'
            'X-Object-Sysmeta-Slo-Etag',
            req.headers.get('X-Backend-Ignore-Range-If-Metadata-Present'))

    def test_resolove_ignore_range_header(self):
        # no ignore header is no-op
        req = Request.blank('/v/a/c/o', headers={'Range': 'bytes=0-4'})
        self.assertEqual(str(req.range), 'bytes=0-4')
        rh.resolve_ignore_range_header(req, {
            'X-Static-Large-Object': True,
            'X-Object-Meta-Color': 'blue',
        })
        self.assertEqual(str(req.range), 'bytes=0-4')

        # missing matching metadata is no-op
        rh.update_ignore_range_header(req, 'X-Static-Large-Object')
        rh.resolve_ignore_range_header(req, {
            'X-Object-Meta-Color': 'blue',
        })
        self.assertEqual(str(req.range), 'bytes=0-4')

        # matching metadata pops range
        rh.resolve_ignore_range_header(req, {
            'X-Static-Large-Object': True,
            'X-Object-Meta-Color': 'blue',
        })
        self.assertIsNone(req.range)

    def test_multiple_resolove_ignore_range_header(self):
        req = Request.blank('/v/a/c/o', headers={'Range': 'bytes=0-4'})
        rh.update_ignore_range_header(req, 'X-Static-Large-Object')
        rh.update_ignore_range_header(req, 'X-Object-Sysmeta-Slo-Etag')
        rh.resolve_ignore_range_header(req, {
            'X-Static-Large-Object': True,
            'X-Object-Meta-Color': 'blue',
        })
        self.assertIsNone(req.range)


class TestSegmentedIterable(unittest.TestCase):

    def setUp(self):
        self.logger = debug_logger()
        self.app = FakeSwift()
        self.expected_unread_requests = {}

    def tearDown(self):
        self.assertFalse(self.app.unclosed_requests)
        self.assertEqual(self.app.unread_requests,
                         self.expected_unread_requests)

    def test_simple_segments_app_iter(self):
        self.app.register('GET', '/a/c/seg1', HTTPOk, {}, 'segment1')
        self.app.register('GET', '/a/c/seg2', HTTPOk, {}, 'segment2')
        req = Request.blank('/v1/a/c/mpu')
        listing_iter = [
            {'path': '/a/c/seg1', 'first_byte': None, 'last_byte': None},
            {'path': '/a/c/seg2', 'first_byte': None, 'last_byte': None},
        ]
        si = rh.SegmentedIterable(req, self.app, listing_iter, 60, self.logger,
                                  'test-agent', 'test-source')
        body = b''.join(si.app_iter)
        self.assertEqual(b'segment1segment2', body)

    def test_simple_segments_app_iter_ranges(self):
        self.app.register('GET', '/a/c/seg1', HTTPOk, {}, 'segment1')
        self.app.register('GET', '/a/c/seg2', HTTPOk, {}, 'segment2')
        req = Request.blank('/v1/a/c/mpu')
        listing_iter = [
            {'path': '/a/c/seg1', 'first_byte': None, 'last_byte': None},
            {'path': '/a/c/seg2', 'first_byte': None, 'last_byte': None},
        ]
        si = rh.SegmentedIterable(req, self.app, listing_iter, 60, self.logger,
                                  'test-agent', 'test-source')
        body = b''.join(si.app_iter_ranges(
            [(0, 8), (8, 16)], b'app/foo', b'bound', 16))
        expected = b'\r\n'.join([
            b'--bound',
            b'Content-Type: app/foo',
            b'Content-Range: bytes 0-7/16',
            b'',
            b'segment1',
            b'--bound',
            b'Content-Type: app/foo',
            b'Content-Range: bytes 8-15/16',
            b'',
            b'segment2',
            b'--bound--',
        ])
        self.assertEqual(expected, body)
