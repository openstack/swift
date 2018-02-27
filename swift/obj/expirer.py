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

from six.moves import urllib

from random import random
from time import time
from os.path import join
from swift import gettext_ as _
from collections import defaultdict, deque
import hashlib

from eventlet import sleep, Timeout
from eventlet.greenpool import GreenPool

from swift.common.daemon import Daemon
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.utils import get_logger, dump_recon_cache, split_path, \
    Timestamp
from swift.common.http import HTTP_NOT_FOUND, HTTP_CONFLICT, \
    HTTP_PRECONDITION_FAILED

from swift.container.reconciler import direct_delete_container_entry

MAX_OBJECTS_TO_CACHE = 100000


class ObjectExpirer(Daemon):
    """
    Daemon that queries the internal hidden expiring_objects_account to
    discover objects that need to be deleted.

    :param conf: The daemon configuration.
    """

    def __init__(self, conf, logger=None, swift=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='object-expirer')
        self.interval = int(conf.get('interval') or 300)
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            (conf.get('expiring_objects_account_name') or 'expiring_objects')
        conf_path = conf.get('__file__') or '/etc/swift/object-expirer.conf'
        request_tries = int(conf.get('request_tries') or 3)
        self.swift = swift or InternalClient(
            conf_path, 'Swift Object Expirer', request_tries)
        self.report_interval = int(conf.get('report_interval') or 300)
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = join(self.recon_cache_path, 'object.recon')
        self.concurrency = int(conf.get('concurrency', 1))
        if self.concurrency < 1:
            raise ValueError("concurrency must be set to at least 1")
        self.processes = int(self.conf.get('processes', 0))
        self.process = int(self.conf.get('process', 0))
        # This option defines how long an un-processable expired object
        # marker will be retried before it is abandoned.  It is not coupled
        # with the tombstone reclaim age in the consistency engine.
        self.reclaim_age = int(conf.get('reclaim_age', 604800))

    def report(self, final=False):
        """
        Emits a log line report of the progress so far, or the final progress
        is final=True.

        :param final: Set to True for the last report once the expiration pass
                      has completed.
        """
        if final:
            elapsed = time() - self.report_first_time
            self.logger.info(_('Pass completed in %(time)ds; '
                               '%(objects)d objects expired') % {
                             'time': elapsed, 'objects': self.report_objects})
            dump_recon_cache({'object_expiration_pass': elapsed,
                              'expired_last_pass': self.report_objects},
                             self.rcache, self.logger)
        elif time() - self.report_last_time >= self.report_interval:
            elapsed = time() - self.report_first_time
            self.logger.info(_('Pass so far %(time)ds; '
                               '%(objects)d objects expired') % {
                             'time': elapsed, 'objects': self.report_objects})
            self.report_last_time = time()

    def round_robin_order(self, task_iter):
        """
        Change order of expiration tasks to avoid deleting objects in a
        certain container continuously.

        :param task_iter: An iterator of delete-task dicts, which should each
            have a ``target_path`` key.
        """
        obj_cache = defaultdict(deque)
        cnt = 0

        def dump_obj_cache_in_round_robin():
            while obj_cache:
                for key in sorted(obj_cache):
                    if obj_cache[key]:
                        yield obj_cache[key].popleft()
                    else:
                        del obj_cache[key]

        for delete_task in task_iter:
            try:
                target_account, target_container, _junk = \
                    split_path('/' + delete_task['target_path'], 3, 3, True)
                cache_key = '%s/%s' % (target_account, target_container)
            except ValueError:
                self.logger.exception('Unexcepted error handling task %r' %
                                      delete_task)
                continue

            obj_cache[cache_key].append(delete_task)
            cnt += 1

            if cnt > MAX_OBJECTS_TO_CACHE:
                for task in dump_obj_cache_in_round_robin():
                    yield task
                cnt = 0

        for task in dump_obj_cache_in_round_robin():
            yield task

    def iter_task_containers_to_expire(self):
        """
        Yields container name under the expiring_objects_account if
        the container name (i.e. timestamp) is past.
        """
        for c in self.swift.iter_containers(self.expiring_objects_account):
            task_container = str(c['name'])
            timestamp = Timestamp(task_container)
            if timestamp > Timestamp.now():
                break
            yield task_container

    def iter_task_to_expire(self, task_containers):
        """
        Yields task expire info dict which consists of task_container,
        target_path, timestamp_to_delete, and target_path
        """

        for task_container in task_containers:
            for o in self.swift.iter_objects(self.expiring_objects_account,
                                             task_container):
                task_object = o['name'].encode('utf8')
                delete_timestamp, target_path = task_object.split('-', 1)
                delete_timestamp = Timestamp(delete_timestamp)
                if delete_timestamp > Timestamp.now():
                    # we shouldn't yield the object that doesn't reach
                    # the expiration date yet.
                    break

                if self.processes > 0:
                    obj_process = int(
                        hashlib.md5('%s/%s' % (task_container, task_object)).
                        hexdigest(), 16)
                    if obj_process % self.processes != self.process:
                        continue

                yield {'task_container': task_container,
                       'task_object': task_object,
                       'target_path': target_path,
                       'delete_timestamp': delete_timestamp}

    def run_once(self, *args, **kwargs):
        """
        Executes a single pass, looking for objects to expire.

        :param args: Extra args to fulfill the Daemon interface; this daemon
                     has no additional args.
        :param kwargs: Extra keyword args to fulfill the Daemon interface; this
                       daemon accepts processes and process keyword args.
                       These will override the values from the config file if
                       provided.
        """
        self.get_process_values(kwargs)
        pool = GreenPool(self.concurrency)
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        try:
            self.logger.debug('Run begin')
            containers, objects = \
                self.swift.get_account_info(self.expiring_objects_account)
            self.logger.info(_('Pass beginning; '
                               '%(containers)s possible containers; '
                               '%(objects)s possible objects') % {
                             'containers': containers, 'objects': objects})

            task_containers = list(self.iter_task_containers_to_expire())

            # delete_task_iter is a generator to yield a dict of
            # task_container, task_object, delete_timestamp, target_path
            # to handle delete actual object and pop the task from the queue.
            delete_task_iter = self.round_robin_order(
                self.iter_task_to_expire(task_containers))

            for delete_task in delete_task_iter:
                pool.spawn_n(self.delete_object, **delete_task)

            pool.waitall()
            for container in task_containers:
                try:
                    self.swift.delete_container(
                        self.expiring_objects_account,
                        container,
                        acceptable_statuses=(2, HTTP_NOT_FOUND, HTTP_CONFLICT))
                except (Exception, Timeout) as err:
                    self.logger.exception(
                        _('Exception while deleting container %(container)s '
                          '%(err)s') % {'container': container,
                                        'err': str(err)})
            self.logger.debug('Run end')
            self.report(final=True)
        except (Exception, Timeout):
            self.logger.exception(_('Unhandled exception'))

    def run_forever(self, *args, **kwargs):
        """
        Executes passes forever, looking for objects to expire.

        :param args: Extra args to fulfill the Daemon interface; this daemon
                     has no additional args.
        :param kwargs: Extra keyword args to fulfill the Daemon interface; this
                       daemon has no additional keyword args.
        """
        sleep(random() * self.interval)
        while True:
            begin = time()
            try:
                self.run_once(*args, **kwargs)
            except (Exception, Timeout):
                self.logger.exception(_('Unhandled exception'))
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(random() * (self.interval - elapsed))

    def get_process_values(self, kwargs):
        """
        Sets self.processes and self.process from the kwargs if those
        values exist, otherwise, leaves those values as they were set in
        the config file.

        :param kwargs: Keyword args passed into the run_forever(), run_once()
                       methods.  They have values specified on the command
                       line when the daemon is run.
        """
        if kwargs.get('processes') is not None:
            self.processes = int(kwargs['processes'])

        if kwargs.get('process') is not None:
            self.process = int(kwargs['process'])

        if self.process < 0:
            raise ValueError(
                'process must be an integer greater than or equal to 0')

        if self.processes < 0:
            raise ValueError(
                'processes must be an integer greater than or equal to 0')

        if self.processes and self.process >= self.processes:
            raise ValueError(
                'process must be less than processes')

    def delete_object(self, target_path, delete_timestamp,
                      task_container, task_object):
        start_time = time()
        try:
            try:
                self.delete_actual_object(target_path, delete_timestamp)
            except UnexpectedResponse as err:
                if err.resp.status_int not in {HTTP_NOT_FOUND,
                                               HTTP_PRECONDITION_FAILED}:
                    raise
                if float(delete_timestamp) > time() - self.reclaim_age:
                    # we'll have to retry the DELETE later
                    raise
            self.pop_queue(task_container, task_object)
            self.report_objects += 1
            self.logger.increment('objects')
        except UnexpectedResponse as err:
            self.logger.increment('errors')
            self.logger.error(
                'Unexpected response while deleting object %(container)s '
                '%(obj)s: %(err)s' % {
                    'container': task_container, 'obj': task_object,
                    'err': str(err.resp.status_int)})
        except (Exception, Timeout) as err:
            self.logger.increment('errors')
            self.logger.exception(
                'Exception while deleting object %(container)s %(obj)s'
                ' %(err)s' % {'container': task_container,
                              'obj': task_object, 'err': str(err)})
        self.logger.timing_since('timing', start_time)
        self.report()

    def pop_queue(self, container, obj):
        """
        Issue a delete object request to the container for the expiring object
        queue entry.
        """
        direct_delete_container_entry(self.swift.container_ring,
                                      self.expiring_objects_account,
                                      container, obj)

    def delete_actual_object(self, actual_obj, timestamp):
        """
        Deletes the end-user object indicated by the actual object name given
        '<account>/<container>/<object>' if and only if the X-Delete-At value
        of the object is exactly the timestamp given.

        :param actual_obj: The name of the end-user object to delete:
                           '<account>/<container>/<object>'
        :param timestamp: The swift.common.utils.Timestamp instance the
                          X-Delete-At value must match to perform the actual
                          delete.
        :raises UnexpectedResponse: if the delete was unsuccessful and
                                    should be retried later
        """
        path = '/v1/' + urllib.parse.quote(actual_obj.lstrip('/'))
        self.swift.make_request(
            'DELETE', path,
            {'X-If-Delete-At': timestamp.normal,
             'X-Timestamp': timestamp.normal,
             'X-Backend-Clean-Expiring-Object-Queue': 'no'},
            (2, HTTP_CONFLICT))
