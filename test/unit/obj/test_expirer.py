# Copyright (c) 2011 OpenStack Foundation
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

from time import time
from unittest import main, TestCase
from test.unit import FakeRing, mocked_http_conn, debug_logger
from copy import deepcopy
from tempfile import mkdtemp
from shutil import rmtree
from collections import defaultdict

import mock
import six
from six.moves import urllib

from swift.common import internal_client, utils, swob
from swift.obj import expirer


def not_random():
    return 0.5


last_not_sleep = 0


def not_sleep(seconds):
    global last_not_sleep
    last_not_sleep = seconds


class FakeInternalClient(object):
    container_ring = FakeRing()

    def __init__(self, aco_dict):
        """
        :param aco_dict: A dict of account ,container, object that
            FakeInternalClient can return when each method called. Each account
            has container name dict, and each container dict has object name
            list in the container.
            e.g. {'account1': {
                      'container1: ['obj1', 'obj2', 'obj3'],
                      'container2: [],
                      },
                  'account2': {},
                 }
        """
        self.aco_dict = defaultdict(dict)
        self.aco_dict.update(aco_dict)

    def get_account_info(self, account):
        return 1, 2

    def iter_containers(self, account, prefix=''):
        acc_dict = self.aco_dict[account]
        return [{'name': six.text_type(container)} for container in
                acc_dict if container.startswith(prefix)]

    def delete_container(*a, **kw):
        pass

    def iter_objects(self, account, container):
        acc_dict = self.aco_dict[account]
        obj_iter = acc_dict.get(container, [])
        return [{'name': six.text_type(obj)} for obj in obj_iter]

    def make_request(*a, **kw):
        pass


class TestObjectExpirer(TestCase):
    maxDiff = None
    internal_client = None

    def setUp(self):
        global not_sleep

        self.old_loadapp = internal_client.loadapp
        self.old_sleep = internal_client.sleep

        internal_client.loadapp = lambda *a, **kw: None
        internal_client.sleep = not_sleep

        self.rcache = mkdtemp()
        self.conf = {'recon_cache_path': self.rcache}
        self.logger = debug_logger('test-expirer')

    def tearDown(self):
        rmtree(self.rcache)
        internal_client.sleep = self.old_sleep
        internal_client.loadapp = self.old_loadapp

    def test_get_process_values_from_kwargs(self):
        x = expirer.ObjectExpirer({})
        vals = {
            'processes': 5,
            'process': 1,
        }
        x.get_process_values(vals)
        self.assertEqual(x.processes, 5)
        self.assertEqual(x.process, 1)

    def test_get_process_values_from_config(self):
        vals = {
            'processes': 5,
            'process': 1,
        }
        x = expirer.ObjectExpirer(vals)
        x.get_process_values({})
        self.assertEqual(x.processes, 5)
        self.assertEqual(x.process, 1)

    def test_get_process_values_negative_process(self):
        vals = {
            'processes': 5,
            'process': -1,
        }
        # from config
        x = expirer.ObjectExpirer(vals)
        expected_msg = 'process must be an integer greater' \
                       ' than or equal to 0'
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values({})
        self.assertEqual(str(ctx.exception), expected_msg)
        # from kwargs
        x = expirer.ObjectExpirer({})
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values(vals)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_get_process_values_negative_processes(self):
        vals = {
            'processes': -5,
            'process': 1,
        }
        # from config
        x = expirer.ObjectExpirer(vals)
        expected_msg = 'processes must be an integer greater' \
                       ' than or equal to 0'
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values({})
        self.assertEqual(str(ctx.exception), expected_msg)
        # from kwargs
        x = expirer.ObjectExpirer({})
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values(vals)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_get_process_values_process_greater_than_processes(self):
        vals = {
            'processes': 5,
            'process': 7,
        }
        # from config
        x = expirer.ObjectExpirer(vals)
        expected_msg = 'process must be less than processes'
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values({})
        self.assertEqual(str(ctx.exception), expected_msg)
        # from kwargs
        x = expirer.ObjectExpirer({})
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values(vals)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_get_process_values_process_equal_to_processes(self):
        vals = {
            'processes': 5,
            'process': 5,
        }
        # from config
        x = expirer.ObjectExpirer(vals)
        expected_msg = 'process must be less than processes'
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values({})
        self.assertEqual(str(ctx.exception), expected_msg)
        # from kwargs
        x = expirer.ObjectExpirer({})
        with self.assertRaises(ValueError) as ctx:
            x.get_process_values(vals)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_init_concurrency_too_small(self):
        conf = {
            'concurrency': 0,
        }
        self.assertRaises(ValueError, expirer.ObjectExpirer, conf)
        conf = {
            'concurrency': -1,
        }
        self.assertRaises(ValueError, expirer.ObjectExpirer, conf)

    def test_process_based_concurrency(self):

        class ObjectExpirer(expirer.ObjectExpirer):

            def __init__(self, conf, swift):
                super(ObjectExpirer, self).__init__(conf, swift=swift)
                self.processes = 3
                self.deleted_objects = {}
                self.obj_containers_in_order = []

            def delete_object(self, actual_obj, timestamp, container, obj):
                if container not in self.deleted_objects:
                    self.deleted_objects[container] = set()
                self.deleted_objects[container].add(obj)
                self.obj_containers_in_order.append(container)

        aco_dict = {
            '.expiring_objects': {
                '0': set('1-one 2-two 3-three'.split()),
                '1': set('2-two 3-three 4-four'.split()),
                '2': set('5-five 6-six'.split()),
                '3': set(u'7-seven\u2661'.split()),
            },
        }
        fake_swift = FakeInternalClient(aco_dict)
        x = ObjectExpirer(self.conf, swift=fake_swift)

        deleted_objects = {}
        for i in range(3):
            x.process = i
            x.run_once()
            self.assertNotEqual(deleted_objects, x.deleted_objects)
            deleted_objects = deepcopy(x.deleted_objects)
        self.assertEqual(aco_dict['.expiring_objects']['3'].pop(),
                         deleted_objects['3'].pop().decode('utf8'))
        self.assertEqual(aco_dict['.expiring_objects'], deleted_objects)
        self.assertEqual(len(set(x.obj_containers_in_order[:4])), 4)

    def test_delete_object(self):
        x = expirer.ObjectExpirer({}, logger=self.logger)
        actual_obj = 'actual_obj'
        timestamp = int(time())
        reclaim_ts = timestamp - x.reclaim_age
        container = 'container'
        obj = 'obj'

        http_exc = {
            resp_code:
                internal_client.UnexpectedResponse(
                    str(resp_code), swob.HTTPException(status=resp_code))
            for resp_code in {404, 412, 500}
        }
        exc_other = Exception()

        def check_call_to_delete_object(exc, ts, should_pop):
            x.logger.clear()
            start_reports = x.report_objects
            with mock.patch.object(x, 'delete_actual_object',
                                   side_effect=exc) as delete_actual:
                with mock.patch.object(x, 'pop_queue') as pop_queue:
                    x.delete_object(actual_obj, ts, container, obj)

            delete_actual.assert_called_once_with(actual_obj, ts)
            log_lines = x.logger.get_lines_for_level('error')
            if should_pop:
                pop_queue.assert_called_once_with(container, obj)
                self.assertEqual(start_reports + 1, x.report_objects)
                self.assertFalse(log_lines)
            else:
                self.assertFalse(pop_queue.called)
                self.assertEqual(start_reports, x.report_objects)
                self.assertEqual(1, len(log_lines))
                if isinstance(exc, internal_client.UnexpectedResponse):
                    self.assertEqual(
                        log_lines[0],
                        'Unexpected response while deleting object container '
                        'obj: %s' % exc.resp.status_int)
                else:
                    self.assertTrue(log_lines[0].startswith(
                        'Exception while deleting object container obj'))

        # verify pop_queue logic on exceptions
        for exc, ts, should_pop in [(None, timestamp, True),
                                    (http_exc[404], timestamp, False),
                                    (http_exc[412], timestamp, False),
                                    (http_exc[500], reclaim_ts, False),
                                    (exc_other, reclaim_ts, False),
                                    (http_exc[404], reclaim_ts, True),
                                    (http_exc[412], reclaim_ts, True)]:

            try:
                check_call_to_delete_object(exc, ts, should_pop)
            except AssertionError as err:
                self.fail("Failed on %r at %f: %s" % (exc, ts, err))

    def test_report(self):
        x = expirer.ObjectExpirer({}, logger=self.logger)

        x.report()
        self.assertEqual(x.logger.get_lines_for_level('info'), [])

        x.logger._clear()
        x.report(final=True)
        self.assertTrue(
            'completed' in str(x.logger.get_lines_for_level('info')))
        self.assertTrue(
            'so far' not in str(x.logger.get_lines_for_level('info')))

        x.logger._clear()
        x.report_last_time = time() - x.report_interval
        x.report()
        self.assertTrue(
            'completed' not in str(x.logger.get_lines_for_level('info')))
        self.assertTrue(
            'so far' in str(x.logger.get_lines_for_level('info')))

    def test_run_once_nothing_to_do(self):
        x = expirer.ObjectExpirer(self.conf, logger=self.logger)
        x.swift = 'throw error because a string does not have needed methods'
        x.run_once()
        self.assertEqual(x.logger.get_lines_for_level('error'),
                         ["Unhandled exception: "])
        log_args, log_kwargs = x.logger.log_dict['error'][0]
        self.assertEqual(str(log_kwargs['exc_info'][1]),
                         "'str' object has no attribute 'get_account_info'")

    def test_run_once_calls_report(self):
        fake_swift = FakeInternalClient({})
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.run_once()
        self.assertEqual(
            x.logger.get_lines_for_level('info'), [
                'Pass beginning; 1 possible containers; 2 possible objects',
                'Pass completed in 0s; 0 objects expired',
            ])

    def test_run_once_unicode_problem(self):
        fake_swift = FakeInternalClient({
            '.expiring_objects': {u'1234': [u'1234-troms\xf8']}
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)

        requests = []

        def capture_requests(ipaddr, port, method, path, *args, **kwargs):
            requests.append((method, path))

        with mocked_http_conn(200, 200, 200, give_connect=capture_requests):
            x.run_once()
        self.assertEqual(len(requests), 3)

    def test_container_timestamp_break(self):
        def fail_to_iter_objects(*a, **kw):
            raise Exception('This should not have been called')

        fake_swift = FakeInternalClient({
            '.expiring_objects': {str(int(time() + 86400)): []}
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        with mock.patch.object(fake_swift, 'iter_objects',
                               fail_to_iter_objects):
            x.run_once()
        logs = x.logger.all_log_lines()
        self.assertEqual(logs['info'], [
            'Pass beginning; 1 possible containers; 2 possible objects',
            'Pass completed in 0s; 0 objects expired',
        ])
        self.assertNotIn('error', logs)

        # Reverse test to be sure it still would blow up the way expected.
        fake_swift = FakeInternalClient({
            '.expiring_objects': {str(int(time() - 86400)): []}
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        with mock.patch.object(fake_swift, 'iter_objects',
                               fail_to_iter_objects):
            x.run_once()
        self.assertEqual(
            x.logger.get_lines_for_level('error'), ['Unhandled exception: '])
        log_args, log_kwargs = x.logger.log_dict['error'][-1]
        self.assertEqual(str(log_kwargs['exc_info'][1]),
                         'This should not have been called')

    def test_object_timestamp_break(self):
        def should_not_be_called(*a, **kw):
            raise Exception('This should not have been called')

        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(int(time() - 86400)): [
                    '%d-actual-obj' % int(time() + 86400)],
            },
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.run_once()
        self.assertNotIn('error', x.logger.all_log_lines())
        self.assertEqual(x.logger.get_lines_for_level('info'), [
            'Pass beginning; 1 possible containers; 2 possible objects',
            'Pass completed in 0s; 0 objects expired',
        ])
        # Reverse test to be sure it still would blow up the way expected.
        ts = int(time() - 86400)
        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(int(time() - 86400)): ['%d-actual-obj' % ts],
            },
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.delete_actual_object = should_not_be_called
        x.run_once()
        self.assertEqual(
            x.logger.get_lines_for_level('error'),
            ['Exception while deleting object %d %d-actual-obj '
             'This should not have been called: ' % (ts, ts)])

    def test_failed_delete_keeps_entry(self):
        def deliberately_blow_up(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        def should_not_get_called(container, obj):
            raise Exception('This should not have been called')

        ts = int(time() - 86400)
        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(int(time() - 86400)): ['%d-actual-obj' % ts],
            },
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.delete_actual_object = deliberately_blow_up
        x.pop_queue = should_not_get_called
        x.run_once()
        self.assertEqual(
            x.logger.get_lines_for_level('error'),
            ['Exception while deleting object %d %d-actual-obj '
             'failed to delete actual object: ' % (ts, ts)])
        self.assertEqual(
            x.logger.get_lines_for_level('info'), [
                'Pass beginning; 1 possible containers; 2 possible objects',
                'Pass completed in 0s; 0 objects expired',
            ])

        # Reverse test to be sure it still would blow up the way expected.
        ts = int(time() - 86400)
        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(int(time() - 86400)): ['%d-actual-obj' % ts],
            },
        })
        self.logger._clear()
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.delete_actual_object = lambda o, t: None
        x.pop_queue = should_not_get_called
        x.run_once()
        self.assertEqual(
            self.logger.get_lines_for_level('error'),
            ['Exception while deleting object %d %d-actual-obj This should '
             'not have been called: ' % (ts, ts)])

    def test_success_gets_counted(self):
        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(int(time() - 86400)): [
                    '%d-acc/c/actual-obj' % int(time() - 86400)],
            },
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.delete_actual_object = lambda o, t: None
        x.pop_queue = lambda c, o: None
        self.assertEqual(x.report_objects, 0)
        with mock.patch('swift.obj.expirer.MAX_OBJECTS_TO_CACHE', 0):
            x.run_once()
            self.assertEqual(x.report_objects, 1)
            self.assertEqual(
                x.logger.get_lines_for_level('info'),
                ['Pass beginning; 1 possible containers; 2 possible objects',
                 'Pass completed in 0s; 1 objects expired'])

    def test_delete_actual_object_does_not_get_unicode(self):
        got_unicode = [False]

        def delete_actual_object_test_for_unicode(actual_obj, timestamp):
            if isinstance(actual_obj, six.text_type):
                got_unicode[0] = True

        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(int(time() - 86400)): [
                    '%d-actual-obj' % int(time() - 86400)],
            },
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.delete_actual_object = delete_actual_object_test_for_unicode
        x.pop_queue = lambda c, o: None
        self.assertEqual(x.report_objects, 0)
        x.run_once()
        self.assertEqual(x.report_objects, 1)
        self.assertEqual(
            x.logger.get_lines_for_level('info'), [
                'Pass beginning; 1 possible containers; 2 possible objects',
                'Pass completed in 0s; 1 objects expired',
            ])
        self.assertFalse(got_unicode[0])

    def test_failed_delete_continues_on(self):
        def fail_delete_container(*a, **kw):
            raise Exception('failed to delete container')

        def fail_delete_actual_object(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        cts = int(time() - 86400)
        ots = int(time() - 86400)

        fake_swift = FakeInternalClient({
            '.expiring_objects': {
                str(cts): ['%d-actual-obj' % ots, '%d-next-obj' % ots],
                str(cts + 1): ['%d-actual-obj' % ots, '%d-next-obj' % ots],
            },
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.delete_actual_object = fail_delete_actual_object
        with mock.patch.object(fake_swift, 'delete_container',
                               fail_delete_container):
            x.run_once()
        error_lines = x.logger.get_lines_for_level('error')
        self.assertEqual(sorted(error_lines), sorted([
            'Exception while deleting object %d %d-actual-obj failed to '
            'delete actual object: ' % (cts, ots),
            'Exception while deleting object %d %d-next-obj failed to '
            'delete actual object: ' % (cts, ots),
            'Exception while deleting object %d %d-actual-obj failed to '
            'delete actual object: ' % (cts + 1, ots),
            'Exception while deleting object %d %d-next-obj failed to '
            'delete actual object: ' % (cts + 1, ots),
            'Exception while deleting container %d failed to delete '
            'container: ' % (cts,),
            'Exception while deleting container %d failed to delete '
            'container: ' % (cts + 1,)]))
        self.assertEqual(x.logger.get_lines_for_level('info'), [
            'Pass beginning; 1 possible containers; 2 possible objects',
            'Pass completed in 0s; 0 objects expired',
        ])

    def test_run_forever_initial_sleep_random(self):
        global last_not_sleep

        def raise_system_exit():
            raise SystemExit('test_run_forever')

        interval = 1234
        x = expirer.ObjectExpirer({'__file__': 'unit_test',
                                   'interval': interval})
        orig_random = expirer.random
        orig_sleep = expirer.sleep
        try:
            expirer.random = not_random
            expirer.sleep = not_sleep
            x.run_once = raise_system_exit
            x.run_forever()
        except SystemExit as err:
            pass
        finally:
            expirer.random = orig_random
            expirer.sleep = orig_sleep
        self.assertEqual(str(err), 'test_run_forever')
        self.assertEqual(last_not_sleep, 0.5 * interval)

    def test_run_forever_catches_usual_exceptions(self):
        raises = [0]

        def raise_exceptions():
            raises[0] += 1
            if raises[0] < 2:
                raise Exception('exception %d' % raises[0])
            raise SystemExit('exiting exception %d' % raises[0])

        x = expirer.ObjectExpirer({}, logger=self.logger)
        orig_sleep = expirer.sleep
        try:
            expirer.sleep = not_sleep
            x.run_once = raise_exceptions
            x.run_forever()
        except SystemExit as err:
            pass
        finally:
            expirer.sleep = orig_sleep
        self.assertEqual(str(err), 'exiting exception 2')
        self.assertEqual(x.logger.get_lines_for_level('error'),
                         ['Unhandled exception: '])
        log_args, log_kwargs = x.logger.log_dict['error'][0]
        self.assertEqual(str(log_kwargs['exc_info'][1]),
                         'exception 1')

    def test_delete_actual_object(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda *a, **kw: fake_app

        x = expirer.ObjectExpirer({})
        ts = '1234'
        x.delete_actual_object('/path/to/object', ts)
        self.assertEqual(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)
        self.assertEqual(got_env[0]['HTTP_X_TIMESTAMP'],
                         got_env[0]['HTTP_X_IF_DELETE_AT'])

    def test_delete_actual_object_nourlquoting(self):
        # delete_actual_object should not do its own url quoting because
        # internal client's make_request handles that.
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda *a, **kw: fake_app

        x = expirer.ObjectExpirer({})
        ts = '1234'
        x.delete_actual_object('/path/to/object name', ts)
        self.assertEqual(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)
        self.assertEqual(got_env[0]['HTTP_X_TIMESTAMP'],
                         got_env[0]['HTTP_X_IF_DELETE_AT'])
        self.assertEqual(got_env[0]['PATH_INFO'], '/v1/path/to/object name')

    def test_delete_actual_object_returns_expected_error(self):
        def do_test(test_status, should_raise):
            calls = [0]

            def fake_app(env, start_response):
                calls[0] += 1
                start_response(test_status, [('Content-Length', '0')])
                return []

            internal_client.loadapp = lambda *a, **kw: fake_app

            x = expirer.ObjectExpirer({})
            if should_raise:
                with self.assertRaises(internal_client.UnexpectedResponse):
                    x.delete_actual_object('/path/to/object', '1234')
            else:
                x.delete_actual_object('/path/to/object', '1234')
            self.assertEqual(calls[0], 1)

        # object was deleted and tombstone reaped
        do_test('404 Not Found', True)
        # object was overwritten *after* the original expiration, or
        do_test('409 Conflict', False)
        # object was deleted but tombstone still exists, or
        # object was overwritten ahead of the original expiration, or
        # object was POSTed to with a new (or no) expiration, or ...
        do_test('412 Precondition Failed', True)

    def test_delete_actual_object_does_not_handle_odd_stuff(self):

        def fake_app(env, start_response):
            start_response(
                '503 Internal Server Error',
                [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda *a, **kw: fake_app

        x = expirer.ObjectExpirer({})
        exc = None
        try:
            x.delete_actual_object('/path/to/object', '1234')
        except Exception as err:
            exc = err
        finally:
            pass
        self.assertEqual(503, exc.resp.status_int)

    def test_delete_actual_object_quotes(self):
        name = 'this name should get quoted'
        timestamp = '1366063156.863045'
        x = expirer.ObjectExpirer({})
        x.swift.make_request = mock.Mock()
        x.swift.make_request.return_value.status_int = 204
        x.delete_actual_object(name, timestamp)
        self.assertEqual(x.swift.make_request.call_count, 1)
        self.assertEqual(x.swift.make_request.call_args[0][1],
                         '/v1/' + urllib.parse.quote(name))

    def test_delete_actual_object_queue_cleaning(self):
        name = 'something'
        timestamp = '1515544858.80602'
        x = expirer.ObjectExpirer({})
        x.swift.make_request = mock.MagicMock()
        x.delete_actual_object(name, timestamp)
        self.assertEqual(x.swift.make_request.call_count, 1)
        header = 'X-Backend-Clean-Expiring-Object-Queue'
        self.assertEqual(
            x.swift.make_request.call_args[0][2].get(header),
            'no')

    def test_pop_queue(self):
        x = expirer.ObjectExpirer({}, logger=self.logger,
                                  swift=FakeInternalClient({}))
        requests = []

        def capture_requests(ipaddr, port, method, path, *args, **kwargs):
            requests.append((method, path))
        with mocked_http_conn(
                200, 200, 200, give_connect=capture_requests) as fake_conn:
            x.pop_queue('c', 'o')
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
        for method, path in requests:
            self.assertEqual(method, 'DELETE')
            device, part, account, container, obj = utils.split_path(
                path, 5, 5, True)
            self.assertEqual(account, '.expiring_objects')
            self.assertEqual(container, 'c')
            self.assertEqual(obj, 'o')


if __name__ == '__main__':
    main()
