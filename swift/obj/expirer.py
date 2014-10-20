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

import urllib
from random import random
from time import time
from os.path import join
from swift import gettext_ as _
import hashlib

from eventlet import sleep, Timeout
from eventlet.greenpool import GreenPool

from swift.common.daemon import Daemon
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.utils import get_logger, dump_recon_cache, split_path
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
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))

    def report(self, final=False):
        """
        Emits a log line report of the progress so far, or the final progress
        is final=True.

        :param final: Set to True for the last report once the expiration pass
                      has completed.
        """
        if final:
            elapsed = time() - self.report_first_time
            self.logger.info(_('Pass completed in %ds; %d objects expired') %
                             (elapsed, self.report_objects))
            dump_recon_cache({'object_expiration_pass': elapsed,
                              'expired_last_pass': self.report_objects},
                             self.rcache, self.logger)
        elif time() - self.report_last_time >= self.report_interval:
            elapsed = time() - self.report_first_time
            self.logger.info(_('Pass so far %ds; %d objects expired') %
                             (elapsed, self.report_objects))
            self.report_last_time = time()

    def iter_cont_objs_to_expire(self):
        """
        Yields (container, obj) tuples to be deleted
        """
        obj_cache = {}
        cnt = 0

        all_containers = set()

        for c in self.swift.iter_containers(self.expiring_objects_account):
            container = str(c['name'])
            timestamp = int(container)
            if timestamp > int(time()):
                break
            all_containers.add(container)
            for o in self.swift.iter_objects(self.expiring_objects_account,
                                             container):
                obj = o['name'].encode('utf8')
                timestamp, actual_obj = obj.split('-', 1)
                timestamp = int(timestamp)
                if timestamp > int(time()):
                    break
                try:
                    cust_account, cust_cont, cust_obj = \
                        split_path('/' + actual_obj, 3, 3, True)
                    cache_key = '%s/%s' % (cust_account, cust_cont)
                except ValueError:
                    cache_key = None

                if self.processes > 0:
                    obj_process = int(
                        hashlib.md5('%s/%s' % (container, obj)).
                        hexdigest(), 16)
                    if obj_process % self.processes != self.process:
                        continue

                if cache_key not in obj_cache:
                    obj_cache[cache_key] = []
                obj_cache[cache_key].append((container, obj))
                cnt += 1

                if cnt > MAX_OBJECTS_TO_CACHE:
                    while obj_cache:
                        for key in obj_cache.keys():
                            if obj_cache[key]:
                                yield obj_cache[key].pop()
                                cnt -= 1
                            else:
                                del obj_cache[key]

        while obj_cache:
            for key in obj_cache.keys():
                if obj_cache[key]:
                    yield obj_cache[key].pop()
                else:
                    del obj_cache[key]

        for container in all_containers:
            yield (container, None)

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
        containers_to_delete = set([])
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        try:
            self.logger.debug('Run begin')
            containers, objects = \
                self.swift.get_account_info(self.expiring_objects_account)
            self.logger.info(_('Pass beginning; %s possible containers; %s '
                               'possible objects') % (containers, objects))

            for container, obj in self.iter_cont_objs_to_expire():
                containers_to_delete.add(container)

                if not obj:
                    continue

                timestamp, actual_obj = obj.split('-', 1)
                timestamp = int(timestamp)
                if timestamp > int(time()):
                    break
                pool.spawn_n(
                    self.delete_object, actual_obj, timestamp,
                    container, obj)

            pool.waitall()
            for container in containers_to_delete:
                try:
                    self.swift.delete_container(
                        self.expiring_objects_account,
                        container,
                        acceptable_statuses=(2, HTTP_NOT_FOUND, HTTP_CONFLICT))
                except (Exception, Timeout) as err:
                    self.logger.exception(
                        _('Exception while deleting container %s %s') %
                        (container, str(err)))
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
                'process must be less than or equal to processes')

    def delete_object(self, actual_obj, timestamp, container, obj):
        start_time = time()
        try:
            try:
                self.delete_actual_object(actual_obj, timestamp)
            except UnexpectedResponse as err:
                if err.resp.status_int != HTTP_NOT_FOUND:
                    raise
                if float(timestamp) > time() - self.reclaim_age:
                    # we'll have to retry the DELETE later
                    raise
            self.pop_queue(container, obj)
            self.report_objects += 1
            self.logger.increment('objects')
        except (Exception, Timeout) as err:
            self.logger.increment('errors')
            self.logger.exception(
                _('Exception while deleting object %s %s %s') %
                (container, obj, str(err)))
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
        :param timestamp: The timestamp the X-Delete-At value must match to
                          perform the actual delete.
        """
        path = '/v1/' + urllib.quote(actual_obj.lstrip('/'))
        self.swift.make_request('DELETE', path,
                                {'X-If-Delete-At': str(timestamp)},
                                (2, HTTP_PRECONDITION_FAILED))
