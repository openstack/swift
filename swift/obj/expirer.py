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
from swift.common.internal_client import InternalClient
from swift.common.utils import get_logger, dump_recon_cache
from swift.common.http import HTTP_NOT_FOUND, HTTP_CONFLICT, \
    HTTP_PRECONDITION_FAILED


class ObjectExpirer(Daemon):
    """
    Daemon that queries the internal hidden expiring_objects_account to
    discover objects that need to be deleted.

    :param conf: The daemon configuration.
    """

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-expirer')
        self.interval = int(conf.get('interval') or 300)
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            'expiring_objects'
        conf_path = conf.get('__file__') or '/etc/swift/object-expirer.conf'
        request_tries = int(conf.get('request_tries') or 3)
        self.swift = InternalClient(conf_path,
                                    'Swift Object Expirer',
                                    request_tries)
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
        processes, process = self.get_process_values(kwargs)
        pool = GreenPool(self.concurrency)
        containers_to_delete = []
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        try:
            self.logger.debug(_('Run begin'))
            containers, objects = \
                self.swift.get_account_info(self.expiring_objects_account)
            self.logger.info(_('Pass beginning; %s possible containers; %s '
                               'possible objects') % (containers, objects))
            for c in self.swift.iter_containers(self.expiring_objects_account):
                container = c['name']
                timestamp = int(container)
                if timestamp > int(time()):
                    break
                containers_to_delete.append(container)
                for o in self.swift.iter_objects(self.expiring_objects_account,
                                                 container):
                    obj = o['name'].encode('utf8')
                    if processes > 0:
                        obj_process = int(
                            hashlib.md5('%s/%s' % (container, obj)).
                            hexdigest(), 16)
                        if obj_process % processes != process:
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
            self.logger.debug(_('Run end'))
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
        Gets the processes, process from the kwargs if those values exist.

        Otherwise, return processes, process set in the config file.

        :param kwargs: Keyword args passed into the run_forever(), run_once()
                       methods.  They have values specified on the command
                       line when the daemon is run.
        """
        if kwargs.get('processes') is not None:
            processes = int(kwargs['processes'])
        else:
            processes = self.processes

        if kwargs.get('process') is not None:
            process = int(kwargs['process'])
        else:
            process = self.process

        if process < 0:
            raise ValueError(
                'process must be an integer greater than or equal to 0')

        if processes < 0:
            raise ValueError(
                'processes must be an integer greater than or equal to 0')

        if processes and process >= processes:
            raise ValueError(
                'process must be less than or equal to processes')

        return processes, process

    def delete_object(self, actual_obj, timestamp, container, obj):
        start_time = time()
        try:
            self.delete_actual_object(actual_obj, timestamp)
            self.swift.delete_object(self.expiring_objects_account,
                                     container, obj)
            self.report_objects += 1
            self.logger.increment('objects')
        except (Exception, Timeout) as err:
            self.logger.increment('errors')
            self.logger.exception(
                _('Exception while deleting object %s %s %s') %
                (container, obj, str(err)))
        self.logger.timing_since('timing', start_time)
        self.report()

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
                                (2, HTTP_NOT_FOUND, HTTP_PRECONDITION_FAILED))
