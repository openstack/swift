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

"""
Why our own memcache client?
By Michael Barton

python-memcached doesn't use consistent hashing, so adding or
removing a memcache server from the pool invalidates a huge
percentage of cached items.

If you keep a pool of python-memcached client objects, each client
object has its own connection to every memcached server, only one of
which is ever in use.  So you wind up with n * m open sockets and
almost all of them idle. This client effectively has a pool for each
server, so the number of backend connections is hopefully greatly
reduced.

python-memcache uses pickle to store things, and there was already a
huge stink about Swift using pickles in memcache
(http://osvdb.org/show/osvdb/86581).  That seemed sort of unfair,
since nova and keystone and everyone else use pickles for memcache
too, but it's hidden behind a "standard" library. But changing would
be a security regression at this point.

Also, pylibmc wouldn't work for us because it needs to use python
sockets in order to play nice with eventlet.

Lucid comes with memcached: v1.4.2.  Protocol documentation for that
version is at:

http://github.com/memcached/memcached/blob/1.4.2/doc/protocol.txt
"""

import os
import json
import logging
# the name of 'time' module is changed to 'tm', to avoid changing the
# signatures of member functions in this file.
import time as tm
from bisect import bisect

from eventlet.green import socket, ssl
from eventlet.pools import Pool
from eventlet import Timeout
from configparser import ConfigParser, NoSectionError, NoOptionError
from swift.common import utils
from swift.common.exceptions import MemcacheConnectionError, \
    MemcacheIncrNotFoundError, MemcachePoolTimeout
from swift.common.utils import md5, human_readable, config_true_value, \
    memcached_timing_stats

DEFAULT_MEMCACHED_PORT = 11211

CONN_TIMEOUT = 0.3
POOL_TIMEOUT = 1.0  # WAG
IO_TIMEOUT = 2.0
PICKLE_FLAG = 1
JSON_FLAG = 2
NODE_WEIGHT = 50
TRY_COUNT = 3

# if ERROR_LIMIT_COUNT errors occur in ERROR_LIMIT_TIME seconds, the server
# will be considered failed for ERROR_LIMIT_DURATION seconds.
ERROR_LIMIT_COUNT = 10
ERROR_LIMIT_TIME = ERROR_LIMIT_DURATION = 60
DEFAULT_ITEM_SIZE_WARNING_THRESHOLD = -1

# Different sample rates for emitting Memcached timing stats.
TIMING_SAMPLE_RATE_HIGH = 0.1
TIMING_SAMPLE_RATE_MEDIUM = 0.01
TIMING_SAMPLE_RATE_LOW = 0.001

# The max value of a delta expiration time.
EXPTIME_MAXDELTA = 30 * 24 * 60 * 60


def md5hash(key):
    if not isinstance(key, bytes):
        key = key.encode('utf-8', errors='surrogateescape')
    return md5(key, usedforsecurity=False).hexdigest().encode('ascii')


def sanitize_timeout(timeout):
    """
    Sanitize a timeout value to use an absolute expiration time if the delta
    is greater than 30 days (in seconds). Note that the memcached server
    translates negative values to mean a delta of 30 days in seconds (and 1
    additional second), client beware.
    """
    if timeout > EXPTIME_MAXDELTA:
        timeout += tm.time()
    return int(timeout)


def set_msg(key, flags, timeout, value):
    if not isinstance(key, bytes):
        raise TypeError('key must be bytes')
    if not isinstance(value, bytes):
        raise TypeError('value must be bytes')
    return b' '.join([
        b'set',
        key,
        str(flags).encode('ascii'),
        str(timeout).encode('ascii'),
        str(len(value)).encode('ascii'),
    ]) + (b'\r\n' + value + b'\r\n')


class MemcacheConnPool(Pool):
    """
    Connection pool for Memcache Connections

    The *server* parameter can be a hostname, an IPv4 address, or an IPv6
    address with an optional port. See
    :func:`swift.common.utils.parse_socket_string` for details.
    """

    def __init__(self, server, size, connect_timeout, tls_context=None):
        Pool.__init__(self, max_size=size)
        self.host, self.port = utils.parse_socket_string(
            server, DEFAULT_MEMCACHED_PORT)
        self._connect_timeout = connect_timeout
        self._tls_context = tls_context

    def create(self):
        addrs = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC,
                                   socket.SOCK_STREAM)
        family, socktype, proto, canonname, sockaddr = addrs[0]
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            with Timeout(self._connect_timeout):
                sock.connect(sockaddr)
            if self._tls_context:
                sock = self._tls_context.wrap_socket(sock,
                                                     server_hostname=self.host)
        except (Exception, Timeout):
            sock.close()
            raise
        return (sock.makefile('rwb'), sock)

    def get(self):
        fp, sock = super(MemcacheConnPool, self).get()
        try:
            if fp is None:
                # An error happened previously, so we need a new connection
                fp, sock = self.create()
            return fp, sock
        except MemcachePoolTimeout:
            # This is the only place that knows an item was successfully taken
            # from the pool, so it has to be responsible for repopulating it.
            # Any other errors should get handled in _get_conns(); see the
            # comment about timeouts during create() there.
            self.put((None, None))
            raise


class MemcacheCommand(object):
    """
    Helper class that encapsulates common parameters of a command.

    :param method: the name of the MemcacheRing method that was called.
    :param key: the memcached key.
    """
    __slots__ = ('method', 'key', 'command', 'hash_key')

    def __init__(self, method, key):
        self.method = method
        self.key = key
        self.command = method.encode()
        self.hash_key = md5hash(key)

    @property
    def key_prefix(self):
        # get the prefix of a user provided memcache key by removing the
        # content after the last '/', all current usages within swift are using
        # prefix, such as "shard-updating-v2", "nvratelimit" and etc.
        return self.key.rsplit('/', 1)[0]


class MemcacheRing(object):
    """
    Simple, consistent-hashed memcache client.
    """

    def __init__(
            self, servers, connect_timeout=CONN_TIMEOUT,
            io_timeout=IO_TIMEOUT, pool_timeout=POOL_TIMEOUT,
            tries=TRY_COUNT,
            max_conns=2, tls_context=None, logger=None,
            error_limit_count=ERROR_LIMIT_COUNT,
            error_limit_time=ERROR_LIMIT_TIME,
            error_limit_duration=ERROR_LIMIT_DURATION,
            item_size_warning_threshold=DEFAULT_ITEM_SIZE_WARNING_THRESHOLD):
        self._ring = {}
        self._errors = dict(((serv, []) for serv in servers))
        self._error_limited = dict(((serv, 0) for serv in servers))
        self._error_limit_count = error_limit_count
        self._error_limit_time = error_limit_time
        self._error_limit_duration = error_limit_duration
        for server in sorted(servers):
            for i in range(NODE_WEIGHT):
                self._ring[md5hash('%s-%s' % (server, i))] = server
        self._tries = tries if tries <= len(servers) else len(servers)
        self._sorted = sorted(self._ring)
        self._client_cache = dict((
            (server, MemcacheConnPool(server, max_conns, connect_timeout,
                                      tls_context=tls_context))
            for server in servers))
        self._connect_timeout = connect_timeout
        self._io_timeout = io_timeout
        self._pool_timeout = pool_timeout
        if logger is None:
            self.logger = logging.getLogger()
        else:
            self.logger = logger
        self.item_size_warning_threshold = item_size_warning_threshold

    @property
    def memcache_servers(self):
        return list(self._client_cache.keys())

    def _log_error(self, server, cmd, action, msg):
        self.logger.error(
            "Error %(action)s to memcached: %(server)s"
            ": with key_prefix %(key_prefix)s, method %(method)s: %(msg)s",
            {'action': action, 'server': server, 'key_prefix': cmd.key_prefix,
             'method': cmd.method, 'msg': msg})

    """
    Handles exceptions.

    :param server: a server.
    :param e: an exception.
    :param cmd: an instance of MemcacheCommand.
    :param conn_start_time: the time at which the failed operation started.
    :param action: a verb describing the operation.
    :param sock: an optional socket that needs to be closed by this method.
    :param fp: an optional file pointer that needs to be closed by this method.
    :param got_connection: if ``True``, the server's connection will be reset
        in the cached connection pool.
    """
    def _exception_occurred(self, server, e, cmd, conn_start_time,
                            action='talking', sock=None,
                            fp=None, got_connection=True):
        if isinstance(e, Timeout):
            self.logger.error(
                "Timeout %(action)s to memcached: %(server)s"
                ": with key_prefix %(key_prefix)s, method %(method)s, "
                "config_timeout %(config_timeout)s, time_spent %(time_spent)s",
                {'action': action, 'server': server,
                 'key_prefix': cmd.key_prefix, 'method': cmd.method,
                 'config_timeout': e.seconds,
                 'time_spent': tm.time() - conn_start_time})
            self.logger.timing_since(
                'memcached.' + cmd.method + '.timeout.timing',
                conn_start_time)
        elif isinstance(e, (socket.error, MemcacheConnectionError)):
            self.logger.error(
                "Error %(action)s to memcached: %(server)s: "
                "with key_prefix %(key_prefix)s, method %(method)s, "
                "time_spent %(time_spent)s, %(err)s",
                {'action': action, 'server': server,
                 'key_prefix': cmd.key_prefix, 'method': cmd.method,
                 'time_spent': tm.time() - conn_start_time, 'err': e})
            self.logger.timing_since(
                'memcached.' + cmd.method + '.conn_err.timing',
                conn_start_time)
        else:
            self.logger.exception(
                "Error %(action)s to memcached: %(server)s"
                ": with key_prefix %(key_prefix)s, method %(method)s, "
                "time_spent %(time_spent)s",
                {'action': action, 'server': server,
                 'key_prefix': cmd.key_prefix, 'method': cmd.method,
                 'time_spent': tm.time() - conn_start_time})
            self.logger.timing_since(
                'memcached.' + cmd.method + '.errors.timing', conn_start_time)

        try:
            if fp:
                fp.close()
                del fp
        except Exception:
            pass
        try:
            if sock:
                sock.close()
                del sock
        except Exception:
            pass
        if got_connection:
            # We need to return something to the pool
            # A new connection will be created the next time it is retrieved
            self._return_conn(server, None, None)

        if isinstance(e, MemcacheIncrNotFoundError):
            # these errors can be caused by other greenthreads not yielding to
            # the incr greenthread often enough, rather than a server problem,
            # so don't error limit the server
            return

        if self._error_limit_time <= 0 or self._error_limit_duration <= 0:
            return

        now = tm.time()
        self._errors[server].append(now)
        if len(self._errors[server]) > self._error_limit_count:
            self._errors[server] = [err for err in self._errors[server]
                                    if err > now - self._error_limit_time]
            if len(self._errors[server]) > self._error_limit_count:
                self._error_limited[server] = now + self._error_limit_duration
                self.logger.error('Error limiting server %s', server)

    def _get_conns(self, cmd):
        """
        Retrieves a server conn from the pool, or connects a new one.
        Chooses the server based on a consistent hash of "key".

        :param cmd: an instance of MemcacheCommand.
        :return: generator to serve memcached connection
        """
        pos = bisect(self._sorted, cmd.hash_key)
        served = []
        any_yielded = False
        while len(served) < self._tries:
            pos = (pos + 1) % len(self._sorted)
            server = self._ring[self._sorted[pos]]
            if server in served:
                continue
            served.append(server)
            pool_start_time = tm.time()
            if self._error_limited[server] > pool_start_time:
                continue
            sock = None
            try:
                with MemcachePoolTimeout(self._pool_timeout):
                    fp, sock = self._client_cache[server].get()
                any_yielded = True
                yield server, fp, sock
            except MemcachePoolTimeout as e:
                self._exception_occurred(server, e, cmd, pool_start_time,
                                         action='getting a connection',
                                         got_connection=False)
            except (Exception, Timeout) as e:
                # Typically a Timeout exception caught here is the one raised
                # by the create() method of this server's MemcacheConnPool
                # object.
                self._exception_occurred(server, e, cmd, pool_start_time,
                                         action='connecting', sock=sock)
        if not any_yielded:
            self._log_error('ALL', cmd, 'connecting',
                            'No more memcached servers to try')

    def _return_conn(self, server, fp, sock):
        """Returns a server connection to the pool."""
        self._client_cache[server].put((fp, sock))

    # Sample rates of different memcached operations are based on generic
    # swift usage patterns.
    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_HIGH)
    def set(self, key, value, serialize=True, time=0,
            min_compress_len=0, raise_on_error=False):
        """
        Set a key/value pair in memcache

        :param key: key
        :param value: value
        :param serialize: if True, value is serialized with JSON before sending
                          to memcache
        :param time: the time to live
        :param min_compress_len: minimum compress length, this parameter was
                                 added to keep the signature compatible with
                                 python-memcached interface. This
                                 implementation ignores it.
        :param raise_on_error: if True, propagate Timeouts and other errors.
                               By default, errors are ignored.
        """
        cmd = MemcacheCommand('set', key)
        timeout = sanitize_timeout(time)
        flags = 0
        if serialize:
            if isinstance(value, bytes):
                value = value.decode('utf8')
            value = json.dumps(value).encode('ascii')
            flags |= JSON_FLAG
        elif not isinstance(value, bytes):
            value = str(value).encode('utf-8')

        if 0 <= self.item_size_warning_threshold <= len(value):
            self.logger.warning(
                "Item size larger than warning threshold: "
                "%d (%s) >= %d (%s)", len(value),
                human_readable(len(value)),
                self.item_size_warning_threshold,
                human_readable(self.item_size_warning_threshold))

        for (server, fp, sock) in self._get_conns(cmd):
            conn_start_time = tm.time()
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(set_msg(cmd.hash_key, flags, timeout, value))
                    # Wait for the set to complete
                    msg = fp.readline().strip()
                    if msg != b'STORED':
                        msg = msg.decode('ascii')
                        raise MemcacheConnectionError('failed set: %s' % msg)
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, cmd, conn_start_time,
                                         sock=sock, fp=fp)
        if raise_on_error:
            raise MemcacheConnectionError(
                "No memcached connections succeeded.")

    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_MEDIUM)
    def get(self, key, raise_on_error=False):
        """
        Gets the object specified by key.  It will also unserialize the object
        before returning if it is serialized in memcache with JSON.

        :param key: key
        :param raise_on_error: if True, propagate Timeouts and other errors.
                               By default, errors are treated as cache misses.
        :returns: value of the key in memcache
        """
        cmd = MemcacheCommand('get', key)
        value = None
        for (server, fp, sock) in self._get_conns(cmd):
            conn_start_time = tm.time()
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b'get ' + cmd.hash_key + b'\r\n')
                    line = fp.readline().strip().split()
                    while True:
                        if not line:
                            raise MemcacheConnectionError('incomplete read')
                        if line[0].upper() == b'END':
                            break
                        if (line[0].upper() == b'VALUE' and
                                line[1] == cmd.hash_key):
                            size = int(line[3])
                            value = fp.read(size)
                            if int(line[2]) & PICKLE_FLAG:
                                value = None
                            if int(line[2]) & JSON_FLAG:
                                value = json.loads(value)
                            fp.readline()
                        line = fp.readline().strip().split()
                    self._return_conn(server, fp, sock)
                    return value
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, cmd, conn_start_time,
                                         sock=sock, fp=fp)
        if raise_on_error:
            raise MemcacheConnectionError(
                "No memcached connections succeeded.")

    def _incr_or_decr(self, fp, sock, cmd, delta):
        sock.sendall(b' '.join([cmd.command, cmd.hash_key, delta]) + b'\r\n')
        line = fp.readline().strip().split()
        if not line:
            raise MemcacheConnectionError('incomplete read')
        if line[0].upper() == b'NOT_FOUND':
            return None
        return int(line[0].strip())

    def _add(self, fp, sock, cmd, add_val, timeout):
        sock.sendall(b' '.join([
            b'add', cmd.hash_key, b'0', str(timeout).encode('ascii'),
            str(len(add_val)).encode('ascii')
        ]) + b'\r\n' + add_val + b'\r\n')
        line = fp.readline().strip().split()
        return None if line[0].upper() == b'NOT_STORED' else int(add_val)

    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_LOW)
    def incr(self, key, delta=1, time=0):
        """
        Increments a key which has a numeric value by delta.
        If the key can't be found, it's added as delta or 0 if delta < 0.
        If passed a negative number, will use memcached's decr. Returns
        the int stored in memcached
        Note: The data memcached stores as the result of incr/decr is
        an unsigned int.  decr's that result in a number below 0 are
        stored as 0.

        :param key: key
        :param delta: amount to add to the value of key (or set as the value
                      if the key is not found) will be cast to an int
        :param time: the time to live
        :returns: result of incrementing
        :raises MemcacheConnectionError:
        """
        cmd = MemcacheCommand('incr' if delta >= 0 else 'decr', key)
        delta_val = str(abs(int(delta))).encode('ascii')
        timeout = sanitize_timeout(time)
        for (server, fp, sock) in self._get_conns(cmd):
            conn_start_time = tm.time()
            try:
                with Timeout(self._io_timeout):
                    new_val = self._incr_or_decr(fp, sock, cmd, delta_val)
                    if new_val is None:
                        add_val = b'0' if cmd.method == 'decr' else delta_val
                        new_val = self._add(fp, sock, cmd, add_val, timeout)
                        if new_val is None:
                            new_val = self._incr_or_decr(
                                fp, sock, cmd, delta_val)
                            if new_val is None:
                                # This can happen if this thread takes more
                                # than the TTL to get from the first failed
                                # incr to the second incr, during which time
                                # the key was concurrently added and expired.
                                raise MemcacheIncrNotFoundError(
                                    'expired ttl=%s' % time)
                    self._return_conn(server, fp, sock)
                    return new_val
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, cmd, conn_start_time,
                                         sock=sock, fp=fp)
        raise MemcacheConnectionError("No memcached connections succeeded.")

    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_LOW)
    def decr(self, key, delta=1, time=0):
        """
        Decrements a key which has a numeric value by delta. Calls incr with
        -delta.

        :param key: key
        :param delta: amount to subtract to the value of key (or set the
                      value to 0 if the key is not found) will be cast to
                      an int
        :param time: the time to live
        :returns: result of decrementing
        :raises MemcacheConnectionError:
        """
        return self.incr(key, delta=-delta, time=time)

    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_HIGH)
    def delete(self, key, server_key=None):
        """
        Deletes a key/value pair from memcache.

        :param key: key to be deleted
        :param server_key: key to use in determining which server in the ring
                            is used
        """
        cmd = server_cmd = MemcacheCommand('delete', key)
        if server_key:
            server_cmd = MemcacheCommand('delete', server_key)
        for (server, fp, sock) in self._get_conns(server_cmd):
            conn_start_time = tm.time()
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b'delete ' + cmd.hash_key + b'\r\n')
                    # Wait for the delete to complete
                    fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, cmd, conn_start_time,
                                         sock=sock, fp=fp)

    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_HIGH)
    def set_multi(self, mapping, server_key, serialize=True, time=0,
                  min_compress_len=0):
        """
        Sets multiple key/value pairs in memcache.

        :param mapping: dictionary of keys and values to be set in memcache
        :param server_key: key to use in determining which server in the ring
                            is used
        :param serialize: if True, value is serialized with JSON before sending
                          to memcache.
        :param time: the time to live
        :min_compress_len: minimum compress length, this parameter was added
                           to keep the signature compatible with
                           python-memcached interface. This implementation
                           ignores it
        """
        cmd = MemcacheCommand('set_multi', server_key)
        timeout = sanitize_timeout(time)
        msg = []
        for key, value in mapping.items():
            key = md5hash(key)
            flags = 0
            if serialize:
                if isinstance(value, bytes):
                    value = value.decode('utf8')
                value = json.dumps(value).encode('ascii')
                flags |= JSON_FLAG
            msg.append(set_msg(key, flags, timeout, value))
        for (server, fp, sock) in self._get_conns(cmd):
            conn_start_time = tm.time()
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b''.join(msg))
                    # Wait for the set to complete
                    for line in range(len(mapping)):
                        fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, cmd, conn_start_time,
                                         sock=sock, fp=fp)

    @memcached_timing_stats(sample_rate=TIMING_SAMPLE_RATE_HIGH)
    def get_multi(self, keys, server_key):
        """
        Gets multiple values from memcache for the given keys.

        :param keys: keys for values to be retrieved from memcache
        :param server_key: key to use in determining which server in the ring
                           is used
        :returns: list of values
        """
        cmd = MemcacheCommand('get_multi', server_key)
        hash_keys = [md5hash(key) for key in keys]
        for (server, fp, sock) in self._get_conns(cmd):
            conn_start_time = tm.time()
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b'get ' + b' '.join(hash_keys) + b'\r\n')
                    line = fp.readline().strip().split()
                    responses = {}
                    while True:
                        if not line:
                            raise MemcacheConnectionError('incomplete read')
                        if line[0].upper() == b'END':
                            break
                        if line[0].upper() == b'VALUE':
                            size = int(line[3])
                            value = fp.read(size)
                            if int(line[2]) & PICKLE_FLAG:
                                value = None
                            elif int(line[2]) & JSON_FLAG:
                                value = json.loads(value)
                            responses[line[1]] = value
                            fp.readline()
                        line = fp.readline().strip().split()
                    values = []
                    for key in hash_keys:
                        if key in responses:
                            values.append(responses[key])
                        else:
                            values.append(None)
                    self._return_conn(server, fp, sock)
                    return values
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, cmd, conn_start_time,
                                         sock=sock, fp=fp)


def load_memcache(conf, logger):
    """
    Build a MemcacheRing object from the given config.  It will also use the
    passed in logger.

    :param conf: a dict, the config options
    :param logger: a logger
    """
    memcache_servers = conf.get('memcache_servers')
    try:
        # Originally, while we documented using memcache_max_connections
        # we only accepted max_connections
        max_conns = int(conf.get('memcache_max_connections',
                                 conf.get('max_connections', 0)))
    except ValueError:
        max_conns = 0

    memcache_options = {}
    if (not memcache_servers
            or max_conns <= 0):
        path = os.path.join(conf.get('swift_dir', '/etc/swift'),
                            'memcache.conf')
        memcache_conf = ConfigParser()
        if memcache_conf.read(path):
            # if memcache.conf exists we'll start with those base options
            try:
                memcache_options = dict(memcache_conf.items('memcache'))
            except NoSectionError:
                pass

            if not memcache_servers:
                try:
                    memcache_servers = \
                        memcache_conf.get('memcache', 'memcache_servers')
                except (NoSectionError, NoOptionError):
                    pass
            if max_conns <= 0:
                try:
                    new_max_conns = \
                        memcache_conf.get('memcache',
                                          'memcache_max_connections')
                    max_conns = int(new_max_conns)
                except (NoSectionError, NoOptionError, ValueError):
                    pass

    # while memcache.conf options are the base for the memcache
    # middleware, if you set the same option also in the filter
    # section of the proxy config it is more specific.
    memcache_options.update(conf)
    connect_timeout = float(memcache_options.get(
        'connect_timeout', CONN_TIMEOUT))
    pool_timeout = float(memcache_options.get(
        'pool_timeout', POOL_TIMEOUT))
    tries = int(memcache_options.get('tries', TRY_COUNT))
    io_timeout = float(memcache_options.get('io_timeout', IO_TIMEOUT))
    if config_true_value(memcache_options.get('tls_enabled', 'false')):
        tls_cafile = memcache_options.get('tls_cafile')
        tls_certfile = memcache_options.get('tls_certfile')
        tls_keyfile = memcache_options.get('tls_keyfile')
        tls_context = ssl.create_default_context(
            cafile=tls_cafile)
        if tls_certfile:
            tls_context.load_cert_chain(tls_certfile, tls_keyfile)
    else:
        tls_context = None
    error_suppression_interval = float(memcache_options.get(
        'error_suppression_interval', ERROR_LIMIT_TIME))
    error_suppression_limit = float(memcache_options.get(
        'error_suppression_limit', ERROR_LIMIT_COUNT))
    item_size_warning_threshold = int(memcache_options.get(
        'item_size_warning_threshold', DEFAULT_ITEM_SIZE_WARNING_THRESHOLD))

    if not memcache_servers:
        memcache_servers = '127.0.0.1:11211'
    if max_conns <= 0:
        max_conns = 2

    return MemcacheRing(
        [s.strip() for s in memcache_servers.split(',')
         if s.strip()],
        connect_timeout=connect_timeout,
        pool_timeout=pool_timeout,
        tries=tries,
        io_timeout=io_timeout,
        max_conns=max_conns,
        tls_context=tls_context,
        logger=logger,
        error_limit_count=error_suppression_limit,
        error_limit_time=error_suppression_interval,
        error_limit_duration=error_suppression_interval,
        item_size_warning_threshold=item_size_warning_threshold)
