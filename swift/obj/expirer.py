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

from random import random
from sys import exc_info
from time import time
from urllib import quote

from eventlet import sleep, Timeout
from paste.deploy import loadapp
from webob import Request

from swift.common.daemon import Daemon
from swift.common.utils import get_logger

try:
    import simplejson as json
except ImportError:
    import json


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
        self.retries = int(conf.get('retries') or 3)
        self.app = loadapp('config:' + (conf.get('__file__') or
                           '/etc/swift/object-expirer.conf'))
        self.report_interval = int(conf.get('report_interval') or 300)
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0

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
                       daemon has no additional keyword args.
        """
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        try:
            self.logger.debug(_('Run begin'))
            self.logger.info(_('Pass beginning; %s possible containers; %s '
                'possible objects') % self.get_account_info())
            for container in self.iter_containers():
                timestamp = int(container)
                if timestamp > int(time()):
                    break
                for obj in self.iter_objects(container):
                    timestamp, actual_obj = obj.split('-', 1)
                    timestamp = int(timestamp)
                    if timestamp > int(time()):
                        break
                    try:
                        self.delete_actual_object(actual_obj, timestamp)
                        self.delete_object(container, obj)
                        self.report_objects += 1
                    except (Exception, Timeout), err:
                        self.logger.exception(
                            _('Exception while deleting object %s %s %s') %
                            (container, obj, str(err)))
                    self.report()
                try:
                    self.delete_container(container)
                except (Exception, Timeout), err:
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
                self.run_once()
            except (Exception, Timeout):
                self.logger.exception(_('Unhandled exception'))
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(random() * (self.interval - elapsed))

    def get_response(self, method, path, headers, acceptable_statuses):
        headers['user-agent'] = 'Swift Object Expirer'
        resp = exc_type = exc_value = exc_traceback = None
        for attempt in xrange(self.retries):
            req = Request.blank(path, environ={'REQUEST_METHOD': method},
                                headers=headers)
            try:
                resp = req.get_response(self.app)
                if resp.status_int in acceptable_statuses or \
                        resp.status_int // 100 in acceptable_statuses:
                    return resp
            except (Exception, Timeout):
                exc_type, exc_value, exc_traceback = exc_info()
            sleep(2 ** (attempt + 1))
        if resp:
            raise Exception(_('Unexpected response %s') % (resp.status,))
        if exc_type:
            # To make pep8 tool happy, in place of raise t, v, tb:
            raise exc_type(*exc_value.args), None, exc_traceback

    def get_account_info(self):
        """
        Returns (container_count, object_count) tuple indicating the values for
        the hidden expiration account.
        """
        resp = self.get_response('HEAD',
            '/v1/' + quote(self.expiring_objects_account), {}, (2, 404))
        if resp.status_int == 404:
            return (0, 0)
        return (int(resp.headers['x-account-container-count']),
                int(resp.headers['x-account-object-count']))

    def iter_containers(self):
        """
        Returns an iterator of container names of the hidden expiration account
        listing.
        """
        path = '/v1/%s?format=json' % (quote(self.expiring_objects_account),)
        marker = ''
        while True:
            resp = self.get_response('GET', path + '&marker=' + quote(marker),
                                     {}, (2, 404))
            if resp.status_int in (204, 404):
                break
            data = json.loads(resp.body)
            if not data:
                break
            for item in data:
                yield item['name']
            marker = data[-1]['name']

    def iter_objects(self, container):
        """
        Returns an iterator of object names of the hidden expiration account's
        container listing for the container name given.

        :param container: The name of the container to list.
        """
        path = '/v1/%s/%s?format=json' % \
               (quote(self.expiring_objects_account), quote(container))
        marker = ''
        while True:
            resp = self.get_response('GET', path + '&' + quote(marker),
                                     {}, (2, 404))
            if resp.status_int in (204, 404):
                break
            data = json.loads(resp.body)
            if not data:
                break
            for item in data:
                yield item['name']
            marker = data[-1]['name']

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
        self.get_response('DELETE', '/v1/%s' % (quote(actual_obj),),
                          {'X-If-Delete-At': str(timestamp)}, (2, 404, 412))

    def delete_object(self, container, obj):
        """
        Deletes an object from the hidden expiring object account.

        :param container: The name of the container for the object.
        :param obj: The name of the object to delete.
        """
        self.get_response('DELETE',
            '/v1/%s/%s/%s' % (quote(self.expiring_objects_account),
                              quote(container), quote(obj)),
            {}, (2, 404))

    def delete_container(self, container):
        """
        Deletes a container from the hidden expiring object account.

        :param container: The name of the container to delete.
        """
        self.get_response('DELETE',
            '/v1/%s/%s' % (quote(self.expiring_objects_account),
                           quote(container)),
            {}, (2, 404, 409))
