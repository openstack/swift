# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import collections
import itertools
import json
from unittest import mock
import unittest

from swift.cli import container_deleter
from swift.common import internal_client
from swift.common import swob
from swift.common import utils

AppCall = collections.namedtuple('AppCall', [
    'method', 'path', 'query', 'headers', 'body'])


class FakeInternalClient(internal_client.InternalClient):
    def __init__(self, responses):
        self.resp_iter = iter(responses)
        self.calls = []

    def make_request(self, method, path, headers, acceptable_statuses,
                     body_file=None, params=None):
        if body_file is None:
            body = None
        else:
            body = body_file.read()
        path, _, query = path.partition('?')
        self.calls.append(AppCall(method, path, query, headers, body))
        resp = next(self.resp_iter)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def __enter__(self):
        return self

    def __exit__(self, *args):
        unused_responses = [r for r in self.resp_iter]
        if unused_responses:
            raise Exception('Unused responses: %r' % unused_responses)


class TestContainerDeleter(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(container_deleter.time, 'time',
                                    side_effect=itertools.count())
        patcher.__enter__()
        self.addCleanup(patcher.__exit__, None, None, None)

        patcher = mock.patch.object(container_deleter, 'OBJECTS_PER_UPDATE', 5)
        patcher.__enter__()
        self.addCleanup(patcher.__exit__, None, None, None)

    def test_make_delete_jobs(self):
        ts = '1558463777.42739'
        self.assertEqual(
            container_deleter.make_delete_jobs(
                'acct', 'cont', ['obj1', 'obj2'],
                utils.Timestamp(ts)),
            [{'name': ts + '-acct/cont/obj1',
              'deleted': 0,
              'created_at': ts,
              'etag': utils.MD5_OF_EMPTY_STRING,
              'size': 0,
              'storage_policy_index': 0,
              'content_type': 'application/async-deleted'},
             {'name': ts + '-acct/cont/obj2',
              'deleted': 0,
              'created_at': ts,
              'etag': utils.MD5_OF_EMPTY_STRING,
              'size': 0,
              'storage_policy_index': 0,
              'content_type': 'application/async-deleted'}])

    def test_make_delete_jobs_native_utf8(self):
        ts = '1558463777.42739'
        uacct = acct = u'acct-\U0001f334'
        ucont = cont = u'cont-\N{SNOWMAN}'
        uobj1 = obj1 = u'obj-\N{GREEK CAPITAL LETTER ALPHA}'
        uobj2 = obj2 = u'/obj-\N{GREEK CAPITAL LETTER OMEGA}'
        self.assertEqual(
            container_deleter.make_delete_jobs(
                acct, cont, [obj1, obj2], utils.Timestamp(ts)),
            [{'name': u'%s-%s/%s/%s' % (ts, uacct, ucont, uobj1),
              'deleted': 0,
              'created_at': ts,
              'etag': utils.MD5_OF_EMPTY_STRING,
              'size': 0,
              'storage_policy_index': 0,
              'content_type': 'application/async-deleted'},
             {'name': u'%s-%s/%s/%s' % (ts, uacct, ucont, uobj2),
              'deleted': 0,
              'created_at': ts,
              'etag': utils.MD5_OF_EMPTY_STRING,
              'size': 0,
              'storage_policy_index': 0,
              'content_type': 'application/async-deleted'}])

    def test_make_delete_jobs_unicode_utf8(self):
        ts = '1558463777.42739'
        acct = u'acct-\U0001f334'
        cont = u'cont-\N{SNOWMAN}'
        obj1 = u'obj-\N{GREEK CAPITAL LETTER ALPHA}'
        obj2 = u'obj-\N{GREEK CAPITAL LETTER OMEGA}'
        self.assertEqual(
            container_deleter.make_delete_jobs(
                acct, cont, [obj1, obj2], utils.Timestamp(ts)),
            [{'name': u'%s-%s/%s/%s' % (ts, acct, cont, obj1),
              'deleted': 0,
              'created_at': ts,
              'etag': utils.MD5_OF_EMPTY_STRING,
              'size': 0,
              'storage_policy_index': 0,
              'content_type': 'application/async-deleted'},
             {'name': u'%s-%s/%s/%s' % (ts, acct, cont, obj2),
              'deleted': 0,
              'created_at': ts,
              'etag': utils.MD5_OF_EMPTY_STRING,
              'size': 0,
              'storage_policy_index': 0,
              'content_type': 'application/async-deleted'}])

    def test_mark_for_deletion_empty_no_yield(self):
        with FakeInternalClient([
            swob.Response(json.dumps([
            ])),
        ]) as swift:
            self.assertEqual(container_deleter.mark_for_deletion(
                swift,
                'account',
                'container',
                'marker',
                'end',
                'prefix',
                timestamp=None,
                yield_time=None,
            ), 0)
            self.assertEqual(swift.calls, [
                ('GET', '/v1/account/container',
                 'format=json&marker=marker&end_marker=end&prefix=prefix',
                 {}, None),
            ])

    def test_mark_for_deletion_empty_with_yield(self):
        with FakeInternalClient([
            swob.Response(json.dumps([
            ])),
        ]) as swift:
            self.assertEqual(list(container_deleter.mark_for_deletion(
                swift,
                'account',
                'container',
                'marker',
                'end',
                'prefix',
                timestamp=None,
                yield_time=0.5,
            )), [(0, None)])
            self.assertEqual(swift.calls, [
                ('GET', '/v1/account/container',
                 'format=json&marker=marker&end_marker=end&prefix=prefix',
                 {}, None),
            ])

    def test_mark_for_deletion_one_update_no_yield(self):
        ts = '1558463777.42739'
        with FakeInternalClient([
            swob.Response(json.dumps([
                {'name': '/obj1'},
                {'name': 'obj2'},
                {'name': 'obj3'},
            ])),
            swob.Response(json.dumps([
            ])),
            swob.Response(status=202),
        ]) as swift:
            self.assertEqual(container_deleter.mark_for_deletion(
                swift,
                'account',
                'container',
                '',
                '',
                '',
                timestamp=utils.Timestamp(ts),
                yield_time=None,
            ), 3)
            self.assertEqual(swift.calls, [
                ('GET', '/v1/account/container',
                 'format=json&marker=&end_marker=&prefix=', {}, None),
                ('GET', '/v1/account/container',
                 'format=json&marker=obj3&end_marker=&prefix=', {}, None),
                ('UPDATE', '/v1/.expiring_objects/' + ts.split('.')[0], '', {
                    'X-Backend-Allow-Private-Methods': 'True',
                    'X-Backend-Storage-Policy-Index': '0',
                    'X-Timestamp': ts}, mock.ANY),
            ])
            self.assertEqual(
                json.loads(swift.calls[-1].body),
                container_deleter.make_delete_jobs(
                    'account', 'container', ['/obj1', 'obj2', 'obj3'],
                    utils.Timestamp(ts)
                )
            )

    def test_mark_for_deletion_two_updates_with_yield(self):
        ts = '1558463777.42739'
        with FakeInternalClient([
            swob.Response(json.dumps([
                {'name': 'obj1'},
                {'name': 'obj2'},
                {'name': 'obj3'},
                {'name': u'obj4-\N{SNOWMAN}'},
                {'name': 'obj5'},
                {'name': 'obj6'},
            ])),
            swob.Response(status=202),
            swob.Response(json.dumps([
            ])),
            swob.Response(status=202),
        ]) as swift:
            self.assertEqual(list(container_deleter.mark_for_deletion(
                swift,
                'account',
                'container',
                '',
                'end',
                'pre',
                timestamp=utils.Timestamp(ts),
                yield_time=0,
            )), [(5, 'obj5'), (6, 'obj6'), (6, None)])
            self.assertEqual(swift.calls, [
                ('GET', '/v1/account/container',
                 'format=json&marker=&end_marker=end&prefix=pre', {}, None),
                ('UPDATE', '/v1/.expiring_objects/' + ts.split('.')[0], '', {
                    'X-Backend-Allow-Private-Methods': 'True',
                    'X-Backend-Storage-Policy-Index': '0',
                    'X-Timestamp': ts}, mock.ANY),
                ('GET', '/v1/account/container',
                 'format=json&marker=obj6&end_marker=end&prefix=pre',
                 {}, None),
                ('UPDATE', '/v1/.expiring_objects/' + ts.split('.')[0], '', {
                    'X-Backend-Allow-Private-Methods': 'True',
                    'X-Backend-Storage-Policy-Index': '0',
                    'X-Timestamp': ts}, mock.ANY),
            ])
            self.assertEqual(
                json.loads(swift.calls[-3].body),
                container_deleter.make_delete_jobs(
                    'account', 'container',
                    ['obj1', 'obj2', 'obj3', u'obj4-\N{SNOWMAN}', 'obj5'],
                    utils.Timestamp(ts)
                )
            )
            self.assertEqual(
                json.loads(swift.calls[-1].body),
                container_deleter.make_delete_jobs(
                    'account', 'container', ['obj6'],
                    utils.Timestamp(ts)
                )
            )

    def test_init_internal_client_log_name(self):
        with mock.patch(
                'swift.cli.container_deleter.InternalClient') \
                as mock_ic:
            container_deleter.main(['a', 'c', '--request-tries', '2'])
        mock_ic.assert_called_once_with(
            '/etc/swift/internal-client.conf',
            'Swift Container Deleter', 2,
            global_conf={'log_name': 'container-deleter-ic'})
