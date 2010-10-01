import uuid
import time
import random
from urlparse import urlparse
from contextlib import contextmanager

import eventlet.pools
from eventlet.green.httplib import CannotSendRequest

from swift.common.utils import TRUE_VALUES
from swift.common import client
from swift.common import direct_client


class ConnectionPool(eventlet.pools.Pool):

    def __init__(self, url, size):
        self.url = url
        eventlet.pools.Pool.__init__(self, size, size)

    def create(self):
        return client.http_connection(self.url)


class Bench(object):

    def __init__(self, logger, conf, names):
        self.logger = logger
        self.user = conf.user
        self.key = conf.key
        self.auth_url = conf.auth
        self.use_proxy = conf.use_proxy in TRUE_VALUES
        if self.use_proxy:
            url, token = client.get_auth(self.auth_url, self.user, self.key)
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
        self.container_name = conf.container_name

        self.object_size = int(conf.object_size)
        self.object_sources = conf.object_sources
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
            max(self.put_concurrency, self.get_concurrency,
                self.del_concurrency))

    def _log_status(self, title):
        total = time.time() - self.beginbeat
        self.logger.info('%s %s [%s failures], %.01f/s' % (
                self.complete, title, self.failures,
                (float(self.complete) / total),
            ))

    @contextmanager
    def connection(self):
        try:
            hc = self.conn_pool.get()
            try:
                yield hc
            except CannotSendRequest:
                self.logger.info("CannotSendRequest.  Skipping...")
                try:
                    hc.close()
                except:
                    pass
                self.failures += 1
                hc = self.conn_pool.create()
        finally:
            self.conn_pool.put(hc)

    def run(self):
        pool = eventlet.GreenPool(self.concurrency)
        events = []
        self.beginbeat = self.heartbeat = time.time()
        self.heartbeat -= 13    # just to get the first report quicker
        self.failures = 0
        self.complete = 0
        for i in xrange(self.total):
            pool.spawn_n(self._run, i)
        pool.waitall()
        self._log_status(self.msg + ' **FINAL**')

    def _run(self, thread):
        return


class BenchController(object):

    def __init__(self, logger, conf):
        self.logger = logger
        self.conf = conf
        self.names = []
        self.delete = conf.delete in TRUE_VALUES
        self.gets = int(conf.num_gets)

    def run(self):
        puts = BenchPUT(self.logger, self.conf, self.names)
        puts.run()
        if self.gets:
            gets = BenchGET(self.logger, self.conf, self.names)
            gets.run()
        if self.delete:
            dels = BenchDELETE(self.logger, self.conf, self.names)
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
        device, partition, name = self.names.pop()
        with self.connection() as conn:
            try:
                if self.use_proxy:
                    client.delete_object(self.url, self.token,
                        self.container_name, name, http_conn=conn)
                else:
                    node = {'ip': self.ip, 'port': self.port, 'device': device}
                    direct_client.direct_delete_object(node, partition,
                        self.account, self.container_name, name)
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
        device, partition, name = random.choice(self.names)
        with self.connection() as conn:
            try:
                if self.use_proxy:
                    client.get_object(self.url, self.token,
                        self.container_name, name, http_conn=conn)
                else:
                    node = {'ip': self.ip, 'port': self.port, 'device': device}
                    direct_client.direct_get_object(node, partition,
                        self.account, self.container_name, name)
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
        if self.use_proxy:
            with self.connection() as conn:
                client.put_container(self.url, self.token,
                    self.container_name, http_conn=conn)

    def _run(self, thread):
        if time.time() - self.heartbeat >= 15:
            self.heartbeat = time.time()
            self._log_status('PUTS')
        name = uuid.uuid4().hex
        if self.object_sources:
            source = random.choice(self.files)
        else:
            source = '0' * self.object_size
        device = random.choice(self.devices)
        partition = str(random.randint(1, 3000))
        with self.connection() as conn:
            try:
                if self.use_proxy:
                    client.put_object(self.url, self.token,
                        self.container_name, name, source,
                        content_length=len(source), http_conn=conn)
                else:
                    node = {'ip': self.ip, 'port': self.port, 'device': device}
                    direct_client.direct_put_object(node, partition,
                        self.account, self.container_name, name, source,
                        content_length=len(source))
            except client.ClientException, e:
                self.logger.debug(str(e))
                self.failures += 1
        self.names.append((device, partition, name))
        self.complete += 1
