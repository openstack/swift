import uuid
import time
import random
from urlparse import urlparse

import eventlet.pools
from eventlet.green.httplib import HTTPSConnection, \
    HTTPResponse, CannotSendRequest

from swift.common.bufferedhttp \
    import BufferedHTTPConnection as HTTPConnection
from swift.common.utils import TRUE_VALUES


class ConnectionPool(eventlet.pools.Pool):
    def __init__(self, url, size):
        self.url = url
        self.url_parsed = urlparse(self.url)
        eventlet.pools.Pool.__init__(self, size, size)

    def create(self):
        if self.url_parsed[0] == 'https':
            hc = HTTPSConnection(self.url_parsed[1])
        elif self.url_parsed[0] == 'http':
            hc = HTTPConnection(self.url_parsed[1])
        else:
            raise Exception("Can't handle %s" % self.url_parsed[0])
        return hc


class Bench(object):
    def __init__(self, logger, conf, names):
        self.logger = logger
        self.user = conf.user
        self.key = conf.key
        self.auth_url = conf.auth
        self.use_proxy = conf.use_proxy in TRUE_VALUES
        if self.use_proxy:
            # Get the auth token
            parsed = urlparse(self.auth_url)
            if parsed.scheme == 'http':
                hc = HTTPConnection(parsed.netloc)
            elif parsed.scheme == 'https':
                hc = HTTPSConnection(parsed.netloc)
            else:
                raise ClientException(
                    'Cannot handle protocol scheme %s for url %s' %
                    (parsed.scheme, self.auth_url))
            hc_args = ('GET', parsed.path, None,
                {'X-Auth-User': self.user, 'X-Auth-Key': self.key})
            hc.request(*hc_args)
            hcr = hc.getresponse()
            hcrd = hcr.read()
            if hcr.status != 204:
                raise Exception("Could not authenticate (%s)" % hcr.status)
            self.token = hcr.getheader('x-auth-token')
            self.account = hcr.getheader('x-storage-url').split('/')[-1]
            if conf.url == '':
                self.url = hcr.getheader('x-storage-url')
            else:
                self.url = conf.url
        else:
            self.token = 'SlapChop!'
            self.account = conf.account
            self.url = conf.url
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
        self.url_parsed = urlparse(self.url)
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

    def _create_connection(self):
        if self.url_parsed[0] == 'https':
            hc = HTTPSConnection(self.url_parsed[1])
        elif self.url_parsed[0] == 'http':
            hc = HTTPConnection(self.url_parsed[1])
        else:
            raise Exception("Can't handle %s" % self.url_parsed[0])
        return hc

    def _send_request(self, *args):
        hc = self.conn_pool.get()
        try:
            start = time.time()
            try:
                hc.request(*args)
                hcr = hc.getresponse()
                hcrd = hcr.read()
                hcr.close()
            except CannotSendRequest:
                self.logger.info("CannotSendRequest.  Skipping...")
                try:
                    hc.close()
                except:
                    pass
                self.failures += 1
                hc = self._create_connection()
                return
            total = time.time() - start
            self.logger.debug("%s %s: %04f" %
                (args[0], args[1], total))
            if hcr.status < 200 or hcr.status > 299:
                self.failures += 1
                return False
            else:
                return True
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
        device, partition, path = self.names.pop()
        headers = {
            'X-Timestamp': "%013.05f" % time.time(),
            'X-ID': str(uuid.uuid4()),
            'X-Auth-Token': self.token,
        }
        if self.use_proxy:
            hc_args = ('DELETE', "/v1/%s/%s/%s" %
                (self.account, self.container_name, path), '', headers)
        else:
            hc_args = ('DELETE', "/%s/%s/%s/%s/%s" %
                (device, partition, self.account, self.container_name, path),
                '', headers)
        self._send_request(*hc_args)
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
        headers = {
            'X-Auth-Token': self.token,
            'X-Timestamp': "%013.05f" % time.time(),
        }
        if self.use_proxy:
            hc_args = ('GET', '/v1/%s/%s/%s' %
                (self.account, self.container_name, name), '', headers)
        else:
            hc_args = ('GET', '/%s/%s/%s/%s/%s' %
                (device, partition, self.account, self.container_name, name),
                '', headers)
        self._send_request(*hc_args)
        self.complete += 1


class BenchPUT(Bench):
    def __init__(self, logger, conf, names):
        Bench.__init__(self, logger, conf, names)
        self.concurrency = self.put_concurrency
        self.total = self.total_objects
        self.msg = 'PUTS'
        if self.use_proxy:
            # Create the container
            if self.url.startswith('http://'):
                hc = HTTPConnection(self.url.split('/')[2])
            else:
                hc = HTTPSConnection(self.url.split('/')[2])
            hc_args = ('PUT',
                    '/v1/%s/%s' % (self.account, self.container_name),
                    None, {'X-Auth-Token': self.token})
            hc.request(*hc_args)
            hcr = hc.getresponse()
            hcrd = hcr.read()
            if hcr.status < 200 or hcr.status > 299:
                raise Exception('Could not create container %s: code: %s' %
                        (self.container_name, hcr.status))

    def _run(self, thread):
        if time.time() - self.heartbeat >= 15:
            self.heartbeat = time.time()
            self._log_status('PUTS')
        name = uuid.uuid4().hex
        if self.object_sources:
            source = random.choice(self.files)
        else:
            source = '0' * self.object_size
        headers = {
            'Content-Type': 'application/octet-stream',
            'X-ID': str(uuid.uuid4()),
            'X-Auth-Token': self.token,
            'X-Timestamp': "%013.05f" % time.time(),
        }
        device = random.choice(self.devices)
        partition = str(random.randint(1, 3000))
        if self.use_proxy:
            hc_args = ('PUT', '/v1/%s/%s/%s' %
                (self.account, self.container_name, name), source, headers)
        else:
            hc_args = ('PUT', '/%s/%s/%s/%s/%s' %
                (device, partition, self.account, self.container_name, name),
                source, headers)
        if self._send_request(*hc_args):
            self.names.append((device, partition, name))
        self.complete += 1
