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

import re
import sys
import uuid
import time
import random
import signal
import socket
import logging
from contextlib import contextmanager
from optparse import Values

import eventlet
import eventlet.pools
from eventlet.green.httplib import CannotSendRequest

from swift.common.utils import config_true_value, LogAdapter
import swiftclient as client
from swift.common import direct_client
from swift.common.http import HTTP_CONFLICT
from swift.common.utils import json


def _func_on_containers(logger, conf, concurrency_key, func):
    """Run a function on each container with concurrency."""

    bench = Bench(logger, conf, [])
    pool = eventlet.GreenPool(int(getattr(conf, concurrency_key)))
    for container in conf.containers:
        pool.spawn_n(func, bench.url, bench.token, container)
    pool.waitall()


def delete_containers(logger, conf):
    """Utility function to delete benchmark containers."""

    def _deleter(url, token, container):
        try:
            client.delete_container(url, token, container)
        except client.ClientException, e:
            if e.http_status != HTTP_CONFLICT:
                logger.warn("Unable to delete container '%s'. "
                            "Got http status '%d'."
                            % (container, e.http_status))

    _func_on_containers(logger, conf, 'del_concurrency', _deleter)


def create_containers(logger, conf):
    """Utility function to create benchmark containers."""

    _func_on_containers(logger, conf, 'put_concurrency', client.put_container)


class SourceFile(object):
    """
    Iterable, file-like object to lazily emit a bunch of zeros in
    reasonable-size chunks.

    swift.common.direct_client wants iterables, but swiftclient wants
    file-like objects where hasattr(thing, 'read') is true. Therefore,
    this class can do both.
    """

    def __init__(self, size, chunk_size=1024 * 64):
        self.pos = 0
        self.size = size
        self.chunk_size = chunk_size

    def __iter__(self):
        return self

    def __len__(self):
        return self.size

    def next(self):
        if self.pos >= self.size:
            raise StopIteration
        chunk_size = min(self.size - self.pos, self.chunk_size)
        yield '0' * chunk_size
        self.pos += chunk_size

    def read(self, desired_size):
        chunk_size = min(self.size - self.pos, desired_size)
        self.pos += chunk_size
        return '0' * chunk_size


class ConnectionPool(eventlet.pools.Pool):

    def __init__(self, url, size):
        self.url = url
        eventlet.pools.Pool.__init__(self, size, size)

    def create(self):
        return client.http_connection(self.url)


class BenchServer(object):
    """
    A BenchServer binds to an IP/port and listens for bench jobs.  A bench
    job consists of the normal conf "dict" encoded in JSON, terminated with an
    EOF.  The log level is at least INFO, but DEBUG may also be specified in
    the conf dict.

    The server will wait forever for jobs, running them one at a time.
    """
    def __init__(self, logger, bind_ip, bind_port):
        self.logger = logger
        self.bind_ip = bind_ip
        self.bind_port = int(bind_port)

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger.info('Binding to %s:%s', self.bind_ip, self.bind_port)
        s.bind((self.bind_ip, self.bind_port))
        s.listen(20)
        while True:
            client, address = s.accept()
            self.logger.debug('Accepting connection from %s:%s', *address)
            client_file = client.makefile('rb+', 1)
            json_data = client_file.read()
            conf = Values(json.loads(json_data))

            self.logger.info(
                'Starting run for %s:%s [put/get/del_concurrency: %s/%s/%s, '
                'num_objects: %s, num_gets: %s]', address[0], address[1],
                conf.put_concurrency, conf.get_concurrency,
                conf.del_concurrency, conf.num_objects, conf.num_gets)

            logger = logging.getLogger('bench-server')
            level = logging.DEBUG if conf.log_level.lower() == 'debug' \
                else logging.INFO
            logger.setLevel(level)
            loghandler = logging.StreamHandler(client_file)
            logformat = logging.Formatter(
                '%(server)s %(asctime)s %(levelname)s %(message)s')
            loghandler.setFormatter(logformat)
            logger.addHandler(loghandler)
            logger = LogAdapter(logger, 'swift-bench-server')

            controller = BenchController(logger, conf)
            try:
                controller.run()
            except socket.error:
                logger.warning('Socket error', exc_info=1)

            logger.logger.removeHandler(loghandler)
            client_file.close()
            client.close()

            self.logger.info('...bench run completed; waiting for next run.')


class Bench(object):

    def __init__(self, logger, conf, names):
        self.logger = logger
        self.aborted = False
        self.user = conf.user
        self.key = conf.key
        self.auth_url = conf.auth
        self.use_proxy = config_true_value(conf.use_proxy)
        self.auth_version = conf.auth_version
        self.logger.info("Auth version: %s" % self.auth_version)
        if self.use_proxy:
            url, token = client.get_auth(self.auth_url, self.user, self.key,
                                         auth_version=self.auth_version)
            self.token = token
            self.account = url.split('/')[-1]
            if conf.url == '':
                self.url = url
            else:
                self.url = conf.url
        else:
            self.token = 'SlapChop!'
            self.account = conf.account
            self.url = conf.url
            self.ip, self.port = self.url.split('/')[2].split(':')

        self.object_size = int(conf.object_size)
        self.object_sources = conf.object_sources
        self.lower_object_size = int(conf.lower_object_size)
        self.upper_object_size = int(conf.upper_object_size)
        self.files = []
        if self.object_sources:
            self.object_sources = self.object_sources.split()
            self.files = [file(f, 'rb').read() for f in self.object_sources]

        self.put_concurrency = int(conf.put_concurrency)
        self.get_concurrency = int(conf.get_concurrency)
        self.del_concurrency = int(conf.del_concurrency)
        self.total_objects = int(conf.num_objects)
        self.total_gets = int(conf.num_gets)
        self.timeout = int(conf.timeout)
        self.devices = conf.devices.split()
        self.names = names
        self.conn_pool = ConnectionPool(self.url,
                                        max(self.put_concurrency,
                                            self.get_concurrency,
                                            self.del_concurrency))

    def _log_status(self, title):
        total = time.time() - self.beginbeat
        self.logger.info(_('%(complete)s %(title)s [%(fail)s failures], '
                           '%(rate).01f/s'),
                         {'title': title, 'complete': self.complete,
                          'fail': self.failures,
                          'rate': (float(self.complete) / total)})

    @contextmanager
    def connection(self):
        try:
            hc = self.conn_pool.get()
            try:
                yield hc
            except CannotSendRequest:
                self.logger.info(_("CannotSendRequest.  Skipping..."))
                try:
                    hc.close()
                except Exception:
                    pass
                self.failures += 1
                hc = self.conn_pool.create()
        finally:
            self.conn_pool.put(hc)

    def run(self):
        pool = eventlet.GreenPool(self.concurrency)
        self.beginbeat = self.heartbeat = time.time()
        self.heartbeat -= 13    # just to get the first report quicker
        self.failures = 0
        self.complete = 0
        for i in xrange(self.total):
            if self.aborted:
                break
            pool.spawn_n(self._run, i)
        pool.waitall()
        self._log_status(self.msg + ' **FINAL**')

    def _run(self, thread):
        return


class DistributedBenchController(object):
    """
    This class manages a distributed swift-bench run.  For this Controller
    class to make sense, the conf.bench_clients list must contain at least one
    entry.

    The idea is to split the configured load between one or more
    swift-bench-client processes, each of which use eventlet for concurrency.
    We deliberately take a simple, naive approach with these limitations:
        1) Concurrency, num_objects, and num_gets are spread evenly between the
           swift-bench-client processes.  With a low concurrency to
           swift-bench-client count ratio, rounding may result in a greater
           than desired aggregate concurrency.
        2) Each swift-bench-client process runs independently so some may
           finish up before others, i.e. the target aggregate concurrency is
           not necessarily present the whole time.  This may bias aggregate
           reported rates lower than a more efficient architecture.
        3) Because of #2, some swift-bench-client processes may be running GETs
           while others are still runinng their PUTs.  Because of this
           potential skew, distributed runs will not isolate one operation at a
           time like a single swift-bench run will.
        3) Reported aggregate rates are simply the sum of each
           swift-bench-client process reported FINAL number.  That's probably
           inaccurate somehow.
    """

    def __init__(self, logger, conf):
        self.logger = logger
        # ... INFO 1000 PUTS **FINAL** [0 failures], 34.9/s
        self.final_re = re.compile(
            'INFO (\d+) (.*) \*\*FINAL\*\* \[(\d+) failures\], (\d+\.\d+)/s')
        self.clients = conf.bench_clients
        del conf.bench_clients
        for key, minval in [('put_concurrency', 1),
                            ('get_concurrency', 1),
                            ('del_concurrency', 1),
                            ('num_objects', 0),
                            ('num_gets', 0)]:
            setattr(conf, key,
                    max(minval, int(getattr(conf, key)) / len(self.clients)))
        self.conf = conf

    def run(self):
        eventlet.patcher.monkey_patch(socket=True)
        pool = eventlet.GreenPool(size=len(self.clients))
        pile = eventlet.GreenPile(pool)
        for client in self.clients:
            pile.spawn(self.do_run, client)
        results = {
            'PUTS': dict(count=0, failures=0, rate=0.0),
            'GETS': dict(count=0, failures=0, rate=0.0),
            'DEL': dict(count=0, failures=0, rate=0.0),
        }
        for result in pile:
            for k, v in result.iteritems():
                target = results[k]
                target['count'] += int(v['count'])
                target['failures'] += int(v['failures'])
                target['rate'] += float(v['rate'])
        for k in ['PUTS', 'GETS', 'DEL']:
            v = results[k]
            self.logger.info('%d %s **FINAL** [%d failures], %.1f/s' % (
                v['count'], k, v['failures'], v['rate']))

    def do_run(self, client):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ip, port = client.split(':')
        s.connect((ip, int(port)))
        s.sendall(json.dumps(self.conf.__dict__))
        s.shutdown(socket.SHUT_WR)
        s_file = s.makefile('rb', 1)
        result = {}
        for line in s_file:
            match = self.final_re.search(line)
            if match:
                g = match.groups()
                result[g[1]] = {
                    'count': g[0],
                    'failures': g[2],
                    'rate': g[3],
                }
            else:
                sys.stderr.write('%s %s' % (client, line))
        return result


class BenchController(object):

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
        self.names = []
        self.delete = config_true_value(conf.delete)
        self.gets = int(conf.num_gets)
        self.aborted = False

    def sigint1(self, signum, frame):
        if self.delete:
            print >>sys.stderr, (
                'SIGINT received; finishing up and running DELETE.\n'
                'Send one more SIGINT to exit *immediately*.')
            self.aborted = True
            if self.running and not isinstance(self.running, BenchDELETE):
                self.running.aborted = True
            signal.signal(signal.SIGINT, self.sigint2)
        else:
            self.sigint2(signum, frame)

    def sigint2(self, signum, frame):
        sys.exit('Final SIGINT received.')

    def run(self):
        signal.signal(signal.SIGINT, self.sigint1)
        puts = BenchPUT(self.logger, self.conf, self.names)
        self.running = puts
        puts.run()
        if self.gets and not self.aborted:
            gets = BenchGET(self.logger, self.conf, self.names)
            self.running = gets
            gets.run()
        if self.delete:
            dels = BenchDELETE(self.logger, self.conf, self.names)
            self.running = dels
            dels.run()


class BenchDELETE(Bench):

    def __init__(self, logger, conf, names):
        Bench.__init__(self, logger, conf, names)
        self.concurrency = self.del_concurrency
        self.total = len(names)
        self.msg = 'DEL'

    def _run(self, thread):
        if time.time() - self.heartbeat >= 15:
            self.heartbeat = time.time()
            self._log_status('DEL')
        device, partition, name, container_name = self.names.pop()
        with self.connection() as conn:
            try:
                if self.use_proxy:
                    client.delete_object(self.url, self.token,
                                         container_name, name, http_conn=conn)
                else:
                    node = {'ip': self.ip, 'port': self.port, 'device': device}
                    direct_client.direct_delete_object(node, partition,
                                                       self.account,
                                                       container_name, name)
            except client.ClientException, e:
                self.logger.debug(str(e))
                self.failures += 1
        self.complete += 1


class BenchGET(Bench):

    def __init__(self, logger, conf, names):
        Bench.__init__(self, logger, conf, names)
        self.concurrency = self.get_concurrency
        self.total = self.total_gets
        self.msg = 'GETS'

    def _run(self, thread):
        if time.time() - self.heartbeat >= 15:
            self.heartbeat = time.time()
            self._log_status('GETS')
        device, partition, name, container_name = random.choice(self.names)
        with self.connection() as conn:
            try:
                if self.use_proxy:
                    client.get_object(self.url, self.token,
                                      container_name, name, http_conn=conn)
                else:
                    node = {'ip': self.ip, 'port': self.port, 'device': device}
                    direct_client.direct_get_object(node, partition,
                                                    self.account,
                                                    container_name, name)
            except client.ClientException, e:
                self.logger.debug(str(e))
                self.failures += 1
        self.complete += 1


class BenchPUT(Bench):

    def __init__(self, logger, conf, names):
        Bench.__init__(self, logger, conf, names)
        self.concurrency = self.put_concurrency
        self.total = self.total_objects
        self.msg = 'PUTS'
        self.containers = conf.containers

    def _run(self, thread):
        if time.time() - self.heartbeat >= 15:
            self.heartbeat = time.time()
            self._log_status('PUTS')
        name = uuid.uuid4().hex
        if self.object_sources:
            source = random.choice(self.files)
        elif self.upper_object_size > self.lower_object_size:
            source = SourceFile(random.randint(self.lower_object_size,
                                               self.upper_object_size))
        else:
            source = SourceFile(self.object_size)
        device = random.choice(self.devices)
        partition = str(random.randint(1, 3000))
        container_name = random.choice(self.containers)
        with self.connection() as conn:
            try:
                if self.use_proxy:
                    client.put_object(self.url, self.token,
                                      container_name, name, source,
                                      content_length=len(source),
                                      http_conn=conn)
                else:
                    node = {'ip': self.ip, 'port': self.port, 'device': device}
                    direct_client.direct_put_object(node, partition,
                                                    self.account,
                                                    container_name, name,
                                                    source,
                                                    content_length=len(source))
            except client.ClientException, e:
                self.logger.debug(str(e))
                self.failures += 1
            else:
                self.names.append((device, partition, name, container_name))
        self.complete += 1
