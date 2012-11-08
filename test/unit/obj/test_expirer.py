# Copyright (c) 2011 OpenStack, LLC.
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

import json
from sys import exc_info
from time import time
from unittest import main, TestCase
from test.unit import FakeLogger

from swift.common import internal_client
from swift.obj import expirer
from swift.proxy.server import Application


def not_random():
    return 0.5


last_not_sleep = 0


def not_sleep(seconds):
    global last_not_sleep
    last_not_sleep = seconds


class TestObjectExpirer(TestCase):
    maxDiff = None

    def setUp(self):
        global not_sleep

        self.old_loadapp = internal_client.loadapp
        self.old_sleep = internal_client.sleep

        internal_client.loadapp = lambda x: None
        internal_client.sleep = not_sleep

    def teardown(self):
        internal_client.sleep = self.old_sleep
        internal_client.loadapp = self.loadapp

    def test_report(self):
        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()

        x.report()
        self.assertEquals(x.logger.log_dict['info'], [])

        x.logger._clear()
        x.report(final=True)
        self.assertTrue('completed' in x.logger.log_dict['info'][-1][0][0],
                        x.logger.log_dict['info'])
        self.assertTrue('so far' not in x.logger.log_dict['info'][-1][0][0],
                        x.logger.log_dict['info'])

        x.logger._clear()
        x.report_last_time = time() - x.report_interval
        x.report()
        self.assertTrue('completed' not in x.logger.log_dict['info'][-1][0][0],
                        x.logger.log_dict['info'])
        self.assertTrue('so far' in x.logger.log_dict['info'][-1][0][0],
                        x.logger.log_dict['info'])

    def test_run_once_nothing_to_do(self):
        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.swift = 'throw error because a string does not have needed methods'
        x.run_once()
        self.assertEquals(x.logger.log_dict['exception'],
                          [(("Unhandled exception",), {},
                            "'str' object has no attribute "
                            "'get_account_info'")])

    def test_run_once_calls_report(self):
        class InternalClient(object):
            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(*a, **kw):
                return []

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.swift = InternalClient()
        x.run_once()
        self.assertEquals(
            x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 0 objects expired',), {})])

    def test_container_timestamp_break(self):
        class InternalClient(object):
            def __init__(self, containers):
                self.containers = containers

            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(self, *a, **kw):
                return self.containers

            def iter_objects(*a, **kw):
                raise Exception('This should not have been called')

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.swift = InternalClient([{'name': str(int(time() + 86400))}])
        x.run_once()
        for exccall in x.logger.log_dict['exception']:
            self.assertTrue(
                'This should not have been called' not in exccall[0][0])
        self.assertEquals(
            x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 0 objects expired',), {})])

        # Reverse test to be sure it still would blow up the way expected.
        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.swift = InternalClient([{'name': str(int(time() - 86400))}])
        x.run_once()
        self.assertEquals(x.logger.log_dict['exception'],
            [(('Unhandled exception',), {},
              str(Exception('This should not have been called')))])

    def test_object_timestamp_break(self):
        class InternalClient(object):
            def __init__(self, containers, objects):
                self.containers = containers
                self.objects = objects

            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(self, *a, **kw):
                return self.containers

            def delete_container(*a, **kw):
                pass

            def iter_objects(self, *a, **kw):
                return self.objects

        def should_not_be_called(*a, **kw):
            raise Exception('This should not have been called')

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.swift = InternalClient([{'name': str(int(time() - 86400))}],
            [{'name': '%d-actual-obj' % int(time() + 86400)}])
        x.run_once()
        for exccall in x.logger.log_dict['exception']:
            self.assertTrue(
                'This should not have been called' not in exccall[0][0])
        self.assertEquals(x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 0 objects expired',), {})])

        # Reverse test to be sure it still would blow up the way expected.
        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        ts = int(time() - 86400)
        x.swift = InternalClient([{'name': str(int(time() - 86400))}],
            [{'name': '%d-actual-obj' % ts}])
        x.delete_actual_object = should_not_be_called
        x.run_once()
        excswhiledeleting = []
        for exccall in x.logger.log_dict['exception']:
            if exccall[0][0].startswith('Exception while deleting '):
                excswhiledeleting.append(exccall[0][0])
        self.assertEquals(excswhiledeleting,
            ['Exception while deleting object %d %d-actual-obj '
             'This should not have been called' % (ts, ts)])

    def test_failed_delete_keeps_entry(self):
        class InternalClient(object):
            def __init__(self, containers, objects):
                self.containers = containers
                self.objects = objects

            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(self, *a, **kw):
                return self.containers

            def delete_container(*a, **kw):
                pass

            def delete_object(*a, **kw):
                raise Exception('This should not have been called')

            def iter_objects(self, *a, **kw):
                return self.objects

        def deliberately_blow_up(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        def should_not_get_called(container, obj):
            raise Exception('This should not have been called')

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.iter_containers = lambda: [str(int(time() - 86400))]
        ts = int(time() - 86400)
        x.delete_actual_object = deliberately_blow_up
        x.swift = InternalClient([{'name': str(int(time() - 86400))}],
            [{'name': '%d-actual-obj' % ts}])
        x.run_once()
        excswhiledeleting = []
        for exccall in x.logger.log_dict['exception']:
            if exccall[0][0].startswith('Exception while deleting '):
                excswhiledeleting.append(exccall[0][0])
        self.assertEquals(excswhiledeleting,
            ['Exception while deleting object %d %d-actual-obj '
             'failed to delete actual object' % (ts, ts)])
        self.assertEquals(x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 0 objects expired',), {})])

        # Reverse test to be sure it still would blow up the way expected.
        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        ts = int(time() - 86400)
        x.delete_actual_object = lambda o, t: None
        x.swift = InternalClient([{'name': str(int(time() - 86400))}],
            [{'name': '%d-actual-obj' % ts}])
        x.run_once()
        excswhiledeleting = []
        for exccall in x.logger.log_dict['exception']:
            if exccall[0][0].startswith('Exception while deleting '):
                excswhiledeleting.append(exccall[0][0])
        self.assertEquals(excswhiledeleting,
            ['Exception while deleting object %d %d-actual-obj This should '
             'not have been called' % (ts, ts)])

    def test_success_gets_counted(self):
        class InternalClient(object):
            def __init__(self, containers, objects):
                self.containers = containers
                self.objects = objects

            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(self, *a, **kw):
                return self.containers

            def delete_container(*a, **kw):
                pass

            def delete_object(*a, **kw):
                pass

            def iter_objects(self, *a, **kw):
                return self.objects

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.delete_actual_object = lambda o, t: None
        self.assertEquals(x.report_objects, 0)
        x.swift = InternalClient([{'name': str(int(time() - 86400))}],
            [{'name': '%d-actual-obj' % int(time() - 86400)}])
        x.run_once()
        self.assertEquals(x.report_objects, 1)
        self.assertEquals(x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 1 objects expired',), {})])

    def test_delete_actual_object_does_not_get_unicode(self):
        class InternalClient(object):
            def __init__(self, containers, objects):
                self.containers = containers
                self.objects = objects

            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(self, *a, **kw):
                return self.containers

            def delete_container(*a, **kw):
                pass

            def delete_object(*a, **kw):
                pass

            def iter_objects(self, *a, **kw):
                return self.objects

        got_unicode = [False]

        def delete_actual_object_test_for_unicode(actual_obj, timestamp):
            if isinstance(actual_obj, unicode):
                got_unicode[0] = True

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        x.delete_actual_object = delete_actual_object_test_for_unicode
        self.assertEquals(x.report_objects, 0)
        x.swift = InternalClient([{'name': str(int(time() - 86400))}],
            [{'name': u'%d-actual-obj' % int(time() - 86400)}])
        x.run_once()
        self.assertEquals(x.report_objects, 1)
        self.assertEquals(x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 1 objects expired',), {})])
        self.assertFalse(got_unicode[0])

    def test_failed_delete_continues_on(self):
        class InternalClient(object):
            def __init__(self, containers, objects):
                self.containers = containers
                self.objects = objects

            def get_account_info(*a, **kw):
                return 1, 2

            def iter_containers(self, *a, **kw):
                return self.containers

            def delete_container(*a, **kw):
                raise Exception('failed to delete container')

            def delete_object(*a, **kw):
                pass

            def iter_objects(self, *a, **kw):
                return self.objects

        def fail_delete_actual_object(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()

        cts = int(time() - 86400)
        ots = int(time() - 86400)

        containers = [
            {'name': str(cts)},
            {'name': str(cts + 1)},
        ]

        objects = [
            {'name': '%d-actual-obj' % ots},
            {'name': '%d-next-obj' % ots}
        ]

        x.swift = InternalClient(containers, objects)
        x.delete_actual_object = fail_delete_actual_object
        x.run_once()
        excswhiledeleting = []
        for exccall in x.logger.log_dict['exception']:
            if exccall[0][0].startswith('Exception while deleting '):
                excswhiledeleting.append(exccall[0][0])
        self.assertEquals(excswhiledeleting, [
            'Exception while deleting object %d %d-actual-obj failed to '
            'delete actual object' % (cts, ots),
            'Exception while deleting object %d %d-next-obj failed to '
            'delete actual object' % (cts, ots),
            'Exception while deleting container %d failed to delete '
            'container' % (cts,),
            'Exception while deleting object %d %d-actual-obj failed to '
            'delete actual object' % (cts + 1, ots),
            'Exception while deleting object %d %d-next-obj failed to '
            'delete actual object' % (cts + 1, ots),
            'Exception while deleting container %d failed to delete '
            'container' % (cts + 1,)])
        self.assertEquals(x.logger.log_dict['info'],
            [(('Pass beginning; 1 possible containers; '
               '2 possible objects',), {}),
             (('Pass completed in 0s; 0 objects expired',), {})])

    def test_run_forever_initial_sleep_random(self):
        global last_not_sleep

        def raise_system_exit():
            raise SystemExit('test_run_forever')

        interval = 1234
        x = expirer.ObjectExpirer({'__file__': 'unit_test',
                                   'interval': interval})
        orig_random = expirer.random
        orig_sleep = expirer.sleep
        exc = None
        try:
            expirer.random = not_random
            expirer.sleep = not_sleep
            x.run_once = raise_system_exit
            x.run_forever()
        except SystemExit, err:
            exc = err
        finally:
            expirer.random = orig_random
            expirer.sleep = orig_sleep
        self.assertEquals(str(err), 'test_run_forever')
        self.assertEquals(last_not_sleep, 0.5 * interval)

    def test_run_forever_catches_usual_exceptions(self):
        raises = [0]

        def raise_exceptions():
            raises[0] += 1
            if raises[0] < 2:
                raise Exception('exception %d' % raises[0])
            raise SystemExit('exiting exception %d' % raises[0])

        x = expirer.ObjectExpirer({})
        x.logger = FakeLogger()
        orig_sleep = expirer.sleep
        exc = None
        try:
            expirer.sleep = not_sleep
            x.run_once = raise_exceptions
            x.run_forever()
        except SystemExit, err:
            exc = err
        finally:
            expirer.sleep = orig_sleep
        self.assertEquals(str(err), 'exiting exception 2')
        self.assertEquals(x.logger.log_dict['exception'],
                          [(('Unhandled exception',), {},
                            'exception 1')])

    def test_delete_actual_object(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda x: fake_app

        x = expirer.ObjectExpirer({})
        ts = '1234'
        x.delete_actual_object('/path/to/object', ts)
        self.assertEquals(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)

    def test_delete_actual_object_nourlquoting(self):
        # delete_actual_object should not do its own url quoting because
        # internal client's make_request handles that.
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda x: fake_app

        x = expirer.ObjectExpirer({})
        ts = '1234'
        x.delete_actual_object('/path/to/object name', ts)
        self.assertEquals(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)
        self.assertEquals(got_env[0]['PATH_INFO'], '/v1/path/to/object name')

    def test_delete_actual_object_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda x: fake_app

        x = expirer.ObjectExpirer({})
        x.delete_actual_object('/path/to/object', '1234')

    def test_delete_actual_object_handles_412(self):

        def fake_app(env, start_response):
            start_response('412 Precondition Failed',
                           [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda x: fake_app

        x = expirer.ObjectExpirer({})
        x.delete_actual_object('/path/to/object', '1234')

    def test_delete_actual_object_does_not_handle_odd_stuff(self):

        def fake_app(env, start_response):
            start_response('503 Internal Server Error',
                [('Content-Length', '0')])
            return []

        internal_client.loadapp = lambda x: fake_app

        x = expirer.ObjectExpirer({})
        exc = None
        try:
            x.delete_actual_object('/path/to/object', '1234')
        except Exception, err:
            exc = err
        finally:
            pass
        self.assertEquals(503, exc.resp.status_int)

if __name__ == '__main__':
    main()
