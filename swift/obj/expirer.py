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

import six
from six.moves import urllib

from random import random
from time import time
from optparse import OptionParser
from os.path import join
from collections import defaultdict, deque

from eventlet import sleep, Timeout
from eventlet.greenpool import GreenPool

from swift.common.constraints import AUTO_CREATE_ACCOUNT_PREFIX
from swift.common.daemon import Daemon, run_daemon
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.utils import get_logger, dump_recon_cache, split_path, \
    Timestamp, config_true_value, normalize_delete_at_timestamp, \
    RateLimitedIterator, md5, non_negative_float, non_negative_int, \
    parse_content_type, parse_options
from swift.common.http import HTTP_NOT_FOUND, HTTP_CONFLICT, \
    HTTP_PRECONDITION_FAILED
from swift.common.recon import RECON_OBJECT_FILE, DEFAULT_RECON_CACHE_PATH

from swift.container.reconciler import direct_delete_container_entry

MAX_OBJECTS_TO_CACHE = 100000
X_DELETE_TYPE = 'text/plain'
ASYNC_DELETE_TYPE = 'application/async-deleted'


def build_task_obj(timestamp, target_account, target_container,
                   target_obj, high_precision=False):
    """
    :return: a task object name in format of
             "<timestamp>-<target_account>/<target_container>/<target_obj>"
    """
    timestamp = Timestamp(timestamp)
    return '%s-%s/%s/%s' % (
        normalize_delete_at_timestamp(timestamp, high_precision),
        target_account, target_container, target_obj)


def parse_task_obj(task_obj):
    """
    :param task_obj: a task object name in format of
                     "<timestamp>-<target_account>/<target_container>" +
                     "/<target_obj>"
    :return: 4-tuples of (delete_at_time, target_account, target_container,
             target_obj)
    """
    timestamp, target_path = task_obj.split('-', 1)
    timestamp = Timestamp(timestamp)
    target_account, target_container, target_obj = \
        split_path('/' + target_path, 3, 3, True)
    return timestamp, target_account, target_container, target_obj


def extract_expirer_bytes_from_ctype(content_type):
    """
    Parse a content-type and return the number of bytes.

    :param content_type: a content-type string
    :return: int or None
    """
    content_type, params = parse_content_type(content_type)
    bytes_size = None
    for k, v in params:
        if k == 'swift_expirer_bytes':
            bytes_size = int(v)
    return bytes_size


def embed_expirer_bytes_in_ctype(content_type, metadata):
    """
    Embed number of bytes into content-type.  The bytes should come from
    content-length on regular objects, but future extensions to "bytes in
    expirer queue" monitoring may want to more closely consider expiration of
    large multipart object manifests.

    :param content_type: a content-type string
    :param metadata: a dict, from Diskfile metadata
    :return: str
    """
    # as best I can tell this key is required by df.open
    report_bytes = metadata['Content-Length']
    return "%s;swift_expirer_bytes=%d" % (content_type, int(report_bytes))


def read_conf_for_delay_reaping_times(conf):
    delay_reaping_times = {}
    for conf_key in conf:
        delay_reaping_prefix = "delay_reaping_"
        if not conf_key.startswith(delay_reaping_prefix):
            continue
        delay_reaping_key = urllib.parse.unquote(
            conf_key[len(delay_reaping_prefix):])
        if delay_reaping_key.strip('/') != delay_reaping_key:
            raise ValueError(
                '%s '
                'should be in the form delay_reaping_<account> '
                'or delay_reaping_<account>/<container> '
                '(leading or trailing "/" is not allowed)' % conf_key)
        try:
            # If split_path fails, have multiple '/' or
            # account name is invalid
            account, container = split_path(
                '/' + delay_reaping_key, 1, 2
            )
        except ValueError:
            raise ValueError(
                '%s '
                'should be in the form delay_reaping_<account> '
                'or delay_reaping_<account>/<container> '
                '(at most one "/" is allowed)' % conf_key)
        try:
            delay_reaping_times[(account, container)] = non_negative_float(
                conf.get(conf_key)
            )
        except ValueError:
            raise ValueError(
                '%s must be a float '
                'greater than or equal to 0' % conf_key)
    return delay_reaping_times


def get_delay_reaping(delay_reaping_times, target_account, target_container):
    return delay_reaping_times.get(
        (target_account, target_container),
        delay_reaping_times.get((target_account, None), 0.0))


class ObjectExpirer(Daemon):
    """
    Daemon that queries the internal hidden task accounts to discover objects
    that need to be deleted.

    :param conf: The daemon configuration.
    """
    log_route = 'object-expirer'

    def __init__(self, conf, logger=None, swift=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route=self.log_route)
        self.interval = float(conf.get('interval') or 300)
        self.tasks_per_second = float(conf.get('tasks_per_second', 50.0))

        self.conf_path = \
            self.conf.get('__file__') or '/etc/swift/object-expirer.conf'
        # True, if the conf file is 'object-expirer.conf'.
        is_legacy_conf = 'expirer' in self.conf_path
        # object-expirer.conf supports only legacy queue
        self.dequeue_from_legacy = \
            True if is_legacy_conf else \
            config_true_value(conf.get('dequeue_from_legacy', 'false'))
        self.swift = swift or self._make_internal_client(is_legacy_conf)
        self.read_conf_for_queue_access()
        self.report_interval = float(conf.get('report_interval') or 300)
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.rcache = join(self.recon_cache_path, RECON_OBJECT_FILE)
        self.concurrency = int(conf.get('concurrency', 1))
        if self.concurrency < 1:
            raise ValueError("concurrency must be set to at least 1")
        # This option defines how long an un-processable expired object
        # marker will be retried before it is abandoned.  It is not coupled
        # with the tombstone reclaim age in the consistency engine.
        self.reclaim_age = int(conf.get('reclaim_age', 604800))

        self.delay_reaping_times = read_conf_for_delay_reaping_times(conf)

    def _make_internal_client(self, is_legacy_conf):
        if is_legacy_conf and 'internal_client_conf_path' not in self.conf:
            ic_conf_path = self.conf_path
        else:
            ic_conf_path = \
                self.conf.get('internal_client_conf_path') or \
                '/etc/swift/internal-client.conf'
        request_tries = int(self.conf.get('request_tries') or 3)
        return InternalClient(
            ic_conf_path, 'Swift Object Expirer', request_tries,
            use_replication_network=True,
            global_conf={'log_name': '%s-ic' % self.conf.get(
                'log_name', self.log_route)})

    def read_conf_for_queue_access(self):
        self.expiring_objects_account = AUTO_CREATE_ACCOUNT_PREFIX + \
            (self.conf.get('expiring_objects_account_name') or
             'expiring_objects')

        # This is for common parameter with general task queue in future
        self.task_container_prefix = ''
        self.processes = non_negative_int(self.conf.get('processes', 0))
        self.process = non_negative_int(self.conf.get('process', 0))
        self._validate_processes_config()

    def report(self, final=False):
        """
        Emits a log line report of the progress so far, or the final progress
        is final=True.

        :param final: Set to True for the last report once the expiration pass
                      has completed.
        """
        if final:
            elapsed = time() - self.report_first_time
            self.logger.info(
                'Pass completed in %(time)ds; %(objects)d objects expired', {
                    'time': elapsed, 'objects': self.report_objects})
            dump_recon_cache({'object_expiration_pass': elapsed,
                              'expired_last_pass': self.report_objects},
                             self.rcache, self.logger)
        elif time() - self.report_last_time >= self.report_interval:
            elapsed = time() - self.report_first_time
            self.logger.info(
                'Pass so far %(time)ds; %(objects)d objects expired', {
                    'time': elapsed, 'objects': self.report_objects})
            self.report_last_time = time()

    def parse_task_obj(self, task_obj):
        return parse_task_obj(task_obj)

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
            # sanity
            except ValueError:
                self.logger.error('Unexcepted error handling task %r' %
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

    def hash_mod(self, name, divisor):
        """
        :param name: a task object name
        :param divisor: a divisor number
        :return: an integer to decide which expirer is assigned to the task
        """
        if not isinstance(name, bytes):
            name = name.encode('utf8')
        # md5 is only used for shuffling mod
        return int(md5(
            name, usedforsecurity=False).hexdigest(), 16) % divisor

    def iter_task_accounts_to_expire(self):
        """
        Yields (task_account, my_index, divisor).
        my_index and divisor is used to assign task obj to only one
        expirer. In expirer method, expirer calculates assigned index for each
        expiration task. The assigned index is in [0, 1, ..., divisor - 1].
        Expirers have their own "my_index" for each task_account. Expirer whose
        "my_index" is equal to the assigned index executes the task. Because
        each expirer have different "my_index", task objects are executed by
        only one expirer.
        """
        if self.processes > 0:
            yield self.expiring_objects_account, self.process, self.processes
        else:
            yield self.expiring_objects_account, 0, 1

    def delete_at_time_of_task_container(self, task_container):
        """
        get delete_at timestamp from task_container name
        """
        # task_container name is timestamp
        return Timestamp(task_container)

    def iter_task_containers_to_expire(self, task_account):
        """
        Yields task_container names under the task_account if the delete at
        timestamp of task_container is past.
        """
        for c in self.swift.iter_containers(task_account,
                                            prefix=self.task_container_prefix):
            task_container = str(c['name'])
            timestamp = self.delete_at_time_of_task_container(task_container)
            if timestamp > Timestamp.now():
                break
            yield task_container

    def get_delay_reaping(self, target_account, target_container):
        return get_delay_reaping(self.delay_reaping_times, target_account,
                                 target_container)

    def iter_task_to_expire(self, task_account_container_list,
                            my_index, divisor):
        """
        Yields task expire info dict which consists of task_account,
        task_container, task_object, timestamp_to_delete, and target_path
        """
        for task_account, task_container in task_account_container_list:
            container_empty = True
            for o in self.swift.iter_objects(task_account, task_container):
                container_empty = False
                if six.PY2:
                    task_object = o['name'].encode('utf8')
                else:
                    task_object = o['name']
                try:
                    delete_timestamp, target_account, target_container, \
                        target_object = parse_task_obj(task_object)
                except ValueError:
                    self.logger.exception('Unexcepted error handling task %r' %
                                          task_object)
                    self.logger.increment('tasks.parse_errors')
                    continue
                is_async = o.get('content_type') == ASYNC_DELETE_TYPE
                delay_reaping = self.get_delay_reaping(target_account,
                                                       target_container)

                if delete_timestamp > Timestamp.now():
                    # we shouldn't yield ANY more objects that can't reach
                    # the expiration date yet.
                    break

                # Only one expirer daemon assigned for each task
                if self.hash_mod('%s/%s' % (task_container, task_object),
                                 divisor) != my_index:
                    self.logger.increment('tasks.skipped')
                    continue

                if delete_timestamp > Timestamp(time() - delay_reaping) \
                        and not is_async:
                    # we shouldn't yield the object during the delay
                    self.logger.increment('tasks.delayed')
                    continue

                self.logger.increment('tasks.assigned')
                yield {'task_account': task_account,
                       'task_container': task_container,
                       'task_object': task_object,
                       'target_path': '/'.join([
                           target_account, target_container, target_object]),
                       'delete_timestamp': delete_timestamp,
                       'is_async_delete': is_async}
            if container_empty:
                try:
                    self.swift.delete_container(
                        task_account, task_container,
                        acceptable_statuses=(2, HTTP_NOT_FOUND, HTTP_CONFLICT))
                except (Exception, Timeout) as err:
                    self.logger.exception(
                        'Exception while deleting container %(account)s '
                        '%(container)s %(err)s', {
                            'account': task_account,
                            'container': task_container, 'err': str(err)})

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
        # these config values are available to override at the command line,
        # blow-up now if they're wrong
        self.override_proceses_config_from_command_line(**kwargs)
        # This if-clause will be removed when general task queue feature is
        # implemented.
        if not self.dequeue_from_legacy:
            self.logger.info('This node is not configured to dequeue tasks '
                             'from the legacy queue.  This node will '
                             'not process any expiration tasks.  At least '
                             'one node in your cluster must be configured '
                             'with dequeue_from_legacy == true.')
            return

        pool = GreenPool(self.concurrency)
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        try:
            self.logger.debug('Run begin')
            for task_account, my_index, divisor in \
                    self.iter_task_accounts_to_expire():
                container_count, obj_count = \
                    self.swift.get_account_info(task_account)

                # the task account is skipped if there are no task container
                if not container_count:
                    continue

                self.logger.info(
                    'Pass beginning for task account %(account)s; '
                    '%(container_count)s possible containers; '
                    '%(obj_count)s possible objects', {
                        'account': task_account,
                        'container_count': container_count,
                        'obj_count': obj_count})

                task_account_container_list = \
                    [(task_account, task_container) for task_container in
                     self.iter_task_containers_to_expire(task_account)]

                # delete_task_iter is a generator to yield a dict of
                # task_account, task_container, task_object, delete_timestamp,
                # target_path to handle delete actual object and pop the task
                # from the queue.
                delete_task_iter = \
                    self.round_robin_order(self.iter_task_to_expire(
                        task_account_container_list, my_index, divisor))
                rate_limited_iter = RateLimitedIterator(
                    delete_task_iter,
                    elements_per_second=self.tasks_per_second)
                for delete_task in rate_limited_iter:
                    pool.spawn_n(self.delete_object, **delete_task)

            pool.waitall()
            self.logger.debug('Run end')
            self.report(final=True)
        except (Exception, Timeout):
            self.logger.exception('Unhandled exception')

    def run_forever(self, *args, **kwargs):
        """
        Executes passes forever, looking for objects to expire.

        :param args: Extra args to fulfill the Daemon interface; this daemon
                     has no additional args.
        :param kwargs: Extra keyword args to fulfill the Daemon interface; this
                       daemon has no additional keyword args.
        """
        # these config values are available to override at the command line
        # blow-up now if they're wrong
        self.override_proceses_config_from_command_line(**kwargs)
        sleep(random() * self.interval)
        while True:
            begin = time()
            try:
                self.run_once(*args, **kwargs)
            except (Exception, Timeout):
                self.logger.exception('Unhandled exception')
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(random() * (self.interval - elapsed))

    def override_proceses_config_from_command_line(self, **kwargs):
        """
        Sets self.processes and self.process from the kwargs if those
        values exist, otherwise, leaves those values as they were set in
        the config file.

        :param kwargs: Keyword args passed into the run_forever(), run_once()
                       methods.  They have values specified on the command
                       line when the daemon is run.
        """
        if kwargs.get('processes') is not None:
            self.processes = non_negative_int(kwargs['processes'])

        if kwargs.get('process') is not None:
            self.process = non_negative_int(kwargs['process'])

        self._validate_processes_config()

    def _validate_processes_config(self):
        """
        Used in constructor and in override_proceses_config_from_command_line
        to validate the processes configuration requirements.

        :raiess: ValueError if processes config is invalid
        """
        if self.processes and self.process >= self.processes:
            raise ValueError(
                'process must be less than processes')

    def delete_object(self, target_path, delete_timestamp,
                      task_account, task_container, task_object,
                      is_async_delete):
        start_time = time()
        try:
            try:
                self.delete_actual_object(target_path, delete_timestamp,
                                          is_async_delete)
            except UnexpectedResponse as err:
                if err.resp.status_int not in {HTTP_NOT_FOUND,
                                               HTTP_PRECONDITION_FAILED}:
                    raise
                if float(delete_timestamp) > time() - self.reclaim_age:
                    # we'll have to retry the DELETE later
                    raise
            self.pop_queue(task_account, task_container, task_object)
            self.report_objects += 1
            self.logger.increment('objects')
        except UnexpectedResponse as err:
            self.logger.increment('errors')
            self.logger.error(
                'Unexpected response while deleting object '
                '%(account)s %(container)s %(obj)s: %(err)s' % {
                    'account': task_account, 'container': task_container,
                    'obj': task_object, 'err': str(err.resp.status_int)})
            self.logger.debug(err.resp.body)
        except (Exception, Timeout) as err:
            self.logger.increment('errors')
            self.logger.exception(
                'Exception while deleting object %(account)s %(container)s '
                '%(obj)s %(err)s' % {
                    'account': task_account, 'container': task_container,
                    'obj': task_object, 'err': str(err)})
        self.logger.timing_since('timing', start_time)
        self.report()

    def pop_queue(self, task_account, task_container, task_object):
        """
        Issue a delete object request to the task_container for the expiring
        object queue entry.
        """
        direct_delete_container_entry(self.swift.container_ring, task_account,
                                      task_container, task_object)

    def delete_actual_object(self, actual_obj, timestamp, is_async_delete):
        """
        Deletes the end-user object indicated by the actual object name given
        '<account>/<container>/<object>' if and only if the X-Delete-At value
        of the object is exactly the timestamp given.

        :param actual_obj: The name of the end-user object to delete:
                           '<account>/<container>/<object>'
        :param timestamp: The swift.common.utils.Timestamp instance the
                          X-Delete-At value must match to perform the actual
                          delete.
        :param is_async_delete: False if the object should be deleted because
                                of "normal" expiration, or True if it should
                                be async-deleted.
        :raises UnexpectedResponse: if the delete was unsuccessful and
                                    should be retried later
        """
        if is_async_delete:
            headers = {'X-Timestamp': timestamp.normal}
            acceptable_statuses = (2, HTTP_CONFLICT, HTTP_NOT_FOUND)
        else:
            headers = {'X-Timestamp': timestamp.normal,
                       'X-If-Delete-At': timestamp.normal,
                       'X-Backend-Clean-Expiring-Object-Queue': 'no'}
            acceptable_statuses = (2, HTTP_CONFLICT)
        self.swift.delete_object(*split_path('/' + actual_obj, 3, 3, True),
                                 headers=headers,
                                 acceptable_statuses=acceptable_statuses)


def main():
    parser = OptionParser("%prog CONFIG [options]")
    parser.add_option('--processes', dest='processes',
                      help="Number of processes to use to do the work, don't "
                      "use this option to do all the work in one process")
    parser.add_option('--process', dest='process',
                      help="Process number for this process, don't use "
                      "this option to do all the work in one process, this "
                      "is used to determine which part of the work this "
                      "process should do")
    conf_file, options = parse_options(parser=parser, once=True)
    run_daemon(ObjectExpirer, conf_file, **options)


if __name__ == '__main__':
    main()
