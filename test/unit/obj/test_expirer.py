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

from swift.obj import expirer
from swift.proxy.server import Application


def not_random():
    return 0.5


last_not_sleep = 0


def not_sleep(seconds):
    global last_not_sleep
    last_not_sleep = seconds


class MockLogger(object):

    def __init__(self):
        self.debugs = []
        self.infos = []
        self.exceptions = []

    def debug(self, msg):
        self.debugs.append(msg)

    def info(self, msg):
        self.infos.append(msg)

    def exception(self, msg):
        self.exceptions.append('%s: %s' % (msg, exc_info()[1]))


class FakeRing(object):

    def __init__(self):
        self.devs = {1: {'ip': '10.0.0.1', 'port': 1000, 'device': 'sda'},
                     2: {'ip': '10.0.0.2', 'port': 1000, 'device': 'sda'},
                     3: {'ip': '10.0.0.3', 'port': 1000, 'device': 'sda'},
                     4: {'ip': '10.0.0.4', 'port': 1000, 'device': 'sda'}}
        self.replica_count = 3

    def get_nodes(self, account, container=None, obj=None):
        return 1, [self.devs[i] for i in xrange(1, self.replica_count + 1)]

    def get_part_nodes(self, part):
        return self.get_nodes('')[1]

    def get_more_nodes(self, nodes):
        yield self.devs[self.replica_count + 1]


class TestObjectExpirer(TestCase):

    def setUp(self):
        self.orig_loadapp = expirer.loadapp
        expirer.loadapp = lambda x: Application({}, account_ring=FakeRing(),
            container_ring=FakeRing(), object_ring=FakeRing())

    def tearDown(self):
        expirer.loadapp = self.orig_loadapp

    def test_report(self):
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()

        x.logger.infos = []
        x.report()
        self.assertEquals(x.logger.infos, [])

        x.logger.infos = []
        x.report(final=True)
        self.assertTrue('completed' in x.logger.infos[-1], x.logger.infos)
        self.assertTrue('so far' not in x.logger.infos[-1], x.logger.infos)

        x.logger.infos = []
        x.report_last_time = time() - x.report_interval
        x.report()
        self.assertTrue('completed' not in x.logger.infos[-1], x.logger.infos)
        self.assertTrue('so far' in x.logger.infos[-1], x.logger.infos)

    def test_run_once_nothing_to_do(self):
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = 'throw error because a string is not callable'
        x.run_once()
        self.assertEquals(x.logger.exceptions,
            ["Unhandled exception: 'str' object is not callable"])

    def test_run_once_calls_report(self):
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: []
        x.run_once()
        self.assertEquals(x.logger.exceptions, [])
        self.assertEquals(x.logger.infos,
            ['Pass beginning; 1 possible containers; 2 possible objects',
             'Pass completed in 0s; 0 objects expired'])

    def test_container_timestamp_break(self):

        def should_not_get_called(container):
            raise Exception('This should not have been called')

        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() + 86400))]
        x.iter_objects = should_not_get_called
        x.run_once()
        self.assertEquals(x.logger.exceptions, [])
        self.assertEquals(x.logger.infos,
            ['Pass beginning; 1 possible containers; 2 possible objects',
             'Pass completed in 0s; 0 objects expired'])

        # Reverse test to be sure it still would blow up the way expected.
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() - 86400))]
        x.iter_objects = should_not_get_called
        x.run_once()
        self.assertEquals(x.logger.exceptions,
            ['Unhandled exception: This should not have been called'])


    def test_object_timestamp_break(self):

        def should_not_get_called(actual_obj, timestamp):
            raise Exception('This should not have been called')

        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() - 86400))]
        x.iter_objects = lambda c: ['%d-actual-obj' % int(time() + 86400)]
        x.delete_actual_object = should_not_get_called
        x.delete_container = lambda c: None
        x.run_once()
        self.assertEquals(x.logger.exceptions, [])
        self.assertEquals(x.logger.infos,
            ['Pass beginning; 1 possible containers; 2 possible objects',
             'Pass completed in 0s; 0 objects expired'])

        # Reverse test to be sure it still would blow up the way expected.
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() - 86400))]
        ts = int(time() - 86400)
        x.iter_objects = lambda c: ['%d-actual-obj' % ts]
        x.delete_actual_object = should_not_get_called
        x.delete_container = lambda c: None
        x.run_once()
        self.assertEquals(x.logger.exceptions, ['Exception while deleting '
            'object %d %d-actual-obj This should not have been called: This '
            'should not have been called' % (ts, ts)])

    def test_failed_delete_keeps_entry(self):

        def deliberately_blow_up(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        def should_not_get_called(container, obj):
            raise Exception('This should not have been called')

        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() - 86400))]
        ts = int(time() - 86400)
        x.iter_objects = lambda c: ['%d-actual-obj' % ts]
        x.delete_actual_object = deliberately_blow_up
        x.delete_object = should_not_get_called
        x.delete_container = lambda c: None
        x.run_once()
        self.assertEquals(x.logger.exceptions, ['Exception while deleting '
            'object %d %d-actual-obj failed to delete actual object: failed '
            'to delete actual object' % (ts, ts)])
        self.assertEquals(x.logger.infos,
            ['Pass beginning; 1 possible containers; 2 possible objects',
             'Pass completed in 0s; 0 objects expired'])

        # Reverse test to be sure it still would blow up the way expected.
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() - 86400))]
        ts = int(time() - 86400)
        x.iter_objects = lambda c: ['%d-actual-obj' % ts]
        x.delete_actual_object = lambda o, t: None
        x.delete_object = should_not_get_called
        x.delete_container = lambda c: None
        x.run_once()
        self.assertEquals(x.logger.exceptions, ['Exception while deleting '
            'object %d %d-actual-obj This should not have been called: This '
            'should not have been called' % (ts, ts)])

    def test_success_gets_counted(self):
        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        x.iter_containers = lambda: [str(int(time() - 86400))]
        x.iter_objects = lambda c: ['%d-actual-obj' % int(time() - 86400)]
        x.delete_actual_object = lambda o, t: None
        x.delete_object = lambda c, o: None
        x.delete_container = lambda c: None
        self.assertEquals(x.report_objects, 0)
        x.run_once()
        self.assertEquals(x.report_objects, 1)
        self.assertEquals(x.logger.exceptions, [])
        self.assertEquals(x.logger.infos,
            ['Pass beginning; 1 possible containers; 2 possible objects',
             'Pass completed in 0s; 1 objects expired'])

    def test_failed_delete_continues_on(self):

        def fail_delete_actual_object(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        def fail_delete_container(container):
            raise Exception('failed to delete container')

        x = expirer.ObjectExpirer({})
        x.logger = MockLogger()
        x.get_account_info = lambda: (1, 2)
        cts = int(time() - 86400)
        x.iter_containers = lambda: [str(cts), str(cts + 1)]
        ots = int(time() - 86400)
        x.iter_objects = lambda c: ['%d-actual-obj' % ots, '%d-next-obj' % ots]
        x.delete_actual_object = fail_delete_actual_object
        x.delete_object = lambda c, o: None
        x.delete_container = fail_delete_container
        x.run_once()
        self.assertEquals(x.logger.exceptions, [
            'Exception while deleting object %d %d-actual-obj failed to '
            'delete actual object: failed to delete actual object' %
            (cts, ots),
            'Exception while deleting object %d %d-next-obj failed to delete '
            'actual object: failed to delete actual object' % (cts, ots),
            'Exception while deleting container %d failed to delete '
            'container: failed to delete container' % cts,
            'Exception while deleting object %d %d-actual-obj failed to '
            'delete actual object: failed to delete actual object' %
            (cts + 1, ots),
            'Exception while deleting object %d %d-next-obj failed to delete '
            'actual object: failed to delete actual object' % (cts + 1, ots),
            'Exception while deleting container %d failed to delete '
            'container: failed to delete container' % (cts + 1)])
        self.assertEquals(x.logger.infos,
            ['Pass beginning; 1 possible containers; 2 possible objects',
             'Pass completed in 0s; 0 objects expired'])

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
        x.logger = MockLogger()
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
        self.assertEquals(x.logger.exceptions,
                          ['Unhandled exception: exception 1'])

    def test_get_response_sets_user_agent(self):
        env_given = [None]

        def fake_app(env, start_response):
            env_given[0] = env
            start_response('200 Ok', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        resp = x.get_response('GET', '/', {}, (200,))
        self.assertEquals(env_given[0]['HTTP_USER_AGENT'],
                          'Swift Object Expirer')

    def test_get_response_retries(self):
        global last_not_sleep
        tries = [0]

        def fake_app(env, start_response):
            tries[0] += 1
            if tries[0] < 3:
                start_response('500 Internal Server Error',
                               [('Content-Length', '0')])
            else:
                start_response('200 Ok', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        orig_sleep = expirer.sleep
        try:
            expirer.sleep = not_sleep
            resp = x.get_response('GET', '/', {}, (200,))
        finally:
            expirer.sleep = orig_sleep
        self.assertEquals(tries[0], 3)
        self.assertEquals(last_not_sleep, 4)

    def test_get_response_method_path_headers(self):
        env_given = [None]

        def fake_app(env, start_response):
            env_given[0] = env
            start_response('200 Ok', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        for method in ('GET', 'SOMETHINGELSE'):
            resp = x.get_response(method, '/', {}, (200,))
            self.assertEquals(env_given[0]['REQUEST_METHOD'], method)
        for path in ('/one', '/two/three'):
            resp = x.get_response('GET', path, {'X-Test': path}, (200,))
            self.assertEquals(env_given[0]['PATH_INFO'], path)
            self.assertEquals(env_given[0]['HTTP_X_TEST'], path)

    def test_get_response_codes(self):

        def fake_app(env, start_response):
            start_response('200 Ok', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        orig_sleep = expirer.sleep
        try:
            expirer.sleep = not_sleep
            resp = x.get_response('GET', '/', {}, (200,))
            resp = x.get_response('GET', '/', {}, (2,))
            resp = x.get_response('GET', '/', {}, (400, 200))
            resp = x.get_response('GET', '/', {}, (400, 2))
            try:
                resp = x.get_response('GET', '/', {}, (400,))
            except Exception, err:
                exc = err
            self.assertEquals(str(err), 'Unexpected response 200 Ok')
            try:
                resp = x.get_response('GET', '/', {}, (201,))
            except Exception, err:
                exc = err
            self.assertEquals(str(err), 'Unexpected response 200 Ok')
        finally:
            expirer.sleep = orig_sleep

    def test_get_account_info(self):

        def fake_app(env, start_response):
            start_response('200 Ok', [('Content-Length', '0'),
                ('X-Account-Container-Count', '80'),
                ('X-Account-Object-Count', '90')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        self.assertEquals(x.get_account_info(), (80, 90))

    def test_get_account_info_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        self.assertEquals(x.get_account_info(), (0, 0))

    def test_iter_containers(self):
        calls = [0]

        def fake_app(env, start_response):
            calls[0] += 1
            if calls[0] == 1:
                body = json.dumps([{'name': 'one'}, {'name': 'two'}])
                start_response('200 Ok', [('Content-Length', str(len(body)))])
                return [body]
            elif calls[0] == 2:
                body = json.dumps([{'name': 'three'}, {'name': 'four'}])
                start_response('200 Ok', [('Content-Length', str(len(body)))])
                return [body]
            elif calls[0] == 3:
                start_response('204 Ok', [('Content-Length', '0')])
                return []
            raise Exception('Should not get here')

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        self.assertEquals(list(x.iter_containers()),
                          ['one', 'two', 'three', 'four'])

    def test_iter_containers_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        self.assertEquals(list(x.iter_containers()), [])

    def test_iter_objects(self):
        calls = [0]

        def fake_app(env, start_response):
            calls[0] += 1
            if calls[0] == 1:
                body = json.dumps([{'name': 'one'}, {'name': 'two'}])
                start_response('200 Ok', [('Content-Length', str(len(body)))])
                return [body]
            elif calls[0] == 2:
                body = json.dumps([{'name': 'three'}, {'name': 'four'}])
                start_response('200 Ok', [('Content-Length', str(len(body)))])
                return [body]
            elif calls[0] == 3:
                start_response('204 Ok', [('Content-Length', '0')])
                return []
            raise Exception('Should not get here')

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        self.assertEquals(list(x.iter_objects('container')),
                          ['one', 'two', 'three', 'four'])

    def test_iter_objects_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        self.assertEquals(list(x.iter_objects('container')), [])

    def test_delete_actual_object(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        ts = '1234'
        x.delete_actual_object('/path/to/object', ts)
        self.assertEquals(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)

    def test_delete_actual_object_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_actual_object('/path/to/object', '1234')

    def test_delete_actual_object_handles_412(self):

        def fake_app(env, start_response):
            start_response('412 Precondition Failed',
                           [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_actual_object('/path/to/object', '1234')

    def test_delete_actual_object_does_not_handle_odd_stuff(self):

        def fake_app(env, start_response):
            start_response('503 Internal Server Error',
                           [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        orig_sleep = expirer.sleep
        exc = None
        try:
            expirer.sleep = not_sleep
            x.delete_actual_object('/path/to/object', '1234')
        except Exception, err:
            exc = err
        finally:
            expirer.sleep = orig_sleep
        self.assertEquals(str(err),
                          'Unexpected response 503 Internal Server Error')

    def test_delete_object(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_object('container', 'object')

    def test_delete_object_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_object('container', 'object')

    def test_delete_object_does_not_handle_odd_stuff(self):

        def fake_app(env, start_response):
            start_response('503 Internal Server Error',
                           [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        orig_sleep = expirer.sleep
        exc = None
        try:
            expirer.sleep = not_sleep
            x.delete_object('container', 'object')
        except Exception, err:
            exc = err
        finally:
            expirer.sleep = orig_sleep
        self.assertEquals(str(err),
                          'Unexpected response 503 Internal Server Error')

    def test_delete_container(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_container('container')

    def test_delete_container_handles_404(self):

        def fake_app(env, start_response):
            start_response('404 Not Found', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_container('container')

    def test_delete_container_handles_409(self):

        def fake_app(env, start_response):
            start_response('409 Conflict', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        x.delete_container('container')

    def test_delete_container_does_not_handle_odd_stuff(self):

        def fake_app(env, start_response):
            start_response('503 Internal Server Error',
                           [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({})
        x.app = fake_app
        orig_sleep = expirer.sleep
        exc = None
        try:
            expirer.sleep = not_sleep
            x.delete_container('container')
        except Exception, err:
            exc = err
        finally:
            expirer.sleep = orig_sleep
        self.assertEquals(str(err),
                          'Unexpected response 503 Internal Server Error')


if __name__ == '__main__':
    main()
