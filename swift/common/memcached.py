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

import six.moves.cPickle as pickle
import json
import logging
import time
from bisect import bisect
from hashlib import md5

from eventlet.green import socket
from eventlet.pools import Pool
from eventlet import Timeout
from six.moves import range
from swift.common import utils

DEFAULT_MEMCACHED_PORT = 11211

CONN_TIMEOUT = 0.3
POOL_TIMEOUT = 1.0  # WAG
IO_TIMEOUT = 2.0
PICKLE_FLAG = 1
JSON_FLAG = 2
NODE_WEIGHT = 50
PICKLE_PROTOCOL = 2
TRY_COUNT = 3

# if ERROR_LIMIT_COUNT errors occur in ERROR_LIMIT_TIME seconds, the server
# will be considered failed for ERROR_LIMIT_DURATION seconds.
ERROR_LIMIT_COUNT = 10
ERROR_LIMIT_TIME = 60
ERROR_LIMIT_DURATION = 60


def md5hash(key):
    if not isinstance(key, bytes):
        key = key.encode('utf-8')
    return md5(key).hexdigest().encode('ascii')


def sanitize_timeout(timeout):
    """
    Sanitize a timeout value to use an absolute expiration time if the delta
    is greater than 30 days (in seconds). Note that the memcached server
    translates negative values to mean a delta of 30 days in seconds (and 1
    additional second), client beware.
    """
    if timeout > (30 * 24 * 60 * 60):
        timeout += time.time()
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


class MemcacheConnectionError(Exception):
    pass


class MemcachePoolTimeout(Timeout):
    pass


class MemcacheConnPool(Pool):
    """
    Connection pool for Memcache Connections

    The *server* parameter can be a hostname, an IPv4 address, or an IPv6
    address with an optional port. See
    :func:`swift.common.utils.parse_socket_string` for details.
    """

    def __init__(self, server, size, connect_timeout):
        Pool.__init__(self, max_size=size)
        self.host, self.port = utils.parse_socket_string(
            server, DEFAULT_MEMCACHED_PORT)
        self._connect_timeout = connect_timeout

    def create(self):
        addrs = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC,
                                   socket.SOCK_STREAM)
        family, socktype, proto, canonname, sockaddr = addrs[0]
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with Timeout(self._connect_timeout):
            sock.connect(sockaddr)
        return (sock.makefile(), sock)

    def get(self):
        fp, sock = super(MemcacheConnPool, self).get()
        if fp is None:
            # An error happened previously, so we need a new connection
            fp, sock = self.create()
        return fp, sock


class MemcacheRing(object):
    """
    Simple, consistent-hashed memcache client.
    """

    def __init__(self, servers, connect_timeout=CONN_TIMEOUT,
                 io_timeout=IO_TIMEOUT, pool_timeout=POOL_TIMEOUT,
                 tries=TRY_COUNT, allow_pickle=False, allow_unpickle=False,
                 max_conns=2):
        self._ring = {}
        self._errors = dict(((serv, []) for serv in servers))
        self._error_limited = dict(((serv, 0) for serv in servers))
        for server in sorted(servers):
            for i in range(NODE_WEIGHT):
                self._ring[md5hash('%s-%s' % (server, i))] = server
        self._tries = tries if tries <= len(servers) else len(servers)
        self._sorted = sorted(self._ring)
        self._client_cache = dict(((server,
                                    MemcacheConnPool(server, max_conns,
                                                     connect_timeout))
                                  for server in servers))
        self._connect_timeout = connect_timeout
        self._io_timeout = io_timeout
        self._pool_timeout = pool_timeout
        self._allow_pickle = allow_pickle
        self._allow_unpickle = allow_unpickle or allow_pickle

    def _exception_occurred(self, server, e, action='talking',
                            sock=None, fp=None, got_connection=True):
        if isinstance(e, Timeout):
            logging.error("Timeout %(action)s to memcached: %(server)s",
                          {'action': action, 'server': server})
        elif isinstance(e, (socket.error, MemcacheConnectionError)):
            logging.error("Error %(action)s to memcached: %(server)s: %(err)s",
                          {'action': action, 'server': server, 'err': e})
        else:
            logging.exception("Error %(action)s to memcached: %(server)s",
                              {'action': action, 'server': server})
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
        now = time.time()
        self._errors[server].append(time.time())
        if len(self._errors[server]) > ERROR_LIMIT_COUNT:
            self._errors[server] = [err for err in self._errors[server]
                                    if err > now - ERROR_LIMIT_TIME]
            if len(self._errors[server]) > ERROR_LIMIT_COUNT:
                self._error_limited[server] = now + ERROR_LIMIT_DURATION
                logging.error('Error limiting server %s', server)

    def _get_conns(self, key):
        """
        Retrieves a server conn from the pool, or connects a new one.
        Chooses the server based on a consistent hash of "key".
        """
        pos = bisect(self._sorted, key)
        served = []
        while len(served) < self._tries:
            pos = (pos + 1) % len(self._sorted)
            server = self._ring[self._sorted[pos]]
            if server in served:
                continue
            served.append(server)
            if self._error_limited[server] > time.time():
                continue
            sock = None
            try:
                with MemcachePoolTimeout(self._pool_timeout):
                    fp, sock = self._client_cache[server].get()
                yield server, fp, sock
            except MemcachePoolTimeout as e:
                self._exception_occurred(
                    server, e, action='getting a connection',
                    got_connection=False)
            except (Exception, Timeout) as e:
                # Typically a Timeout exception caught here is the one raised
                # by the create() method of this server's MemcacheConnPool
                # object.
                self._exception_occurred(
                    server, e, action='connecting', sock=sock)

    def _return_conn(self, server, fp, sock):
        """Returns a server connection to the pool."""
        self._client_cache[server].put((fp, sock))

    def set(self, key, value, serialize=True, time=0,
            min_compress_len=0):
        """
        Set a key/value pair in memcache

        :param key: key
        :param value: value
        :param serialize: if True, value is serialized with JSON before sending
                          to memcache, or with pickle if configured to use
                          pickle instead of JSON (to avoid cache poisoning)
        :param time: the time to live
        :param min_compress_len: minimum compress length, this parameter was
                                 added to keep the signature compatible with
                                 python-memcached interface. This
                                 implementation ignores it.
        """
        key = md5hash(key)
        timeout = sanitize_timeout(time)
        flags = 0
        if serialize and self._allow_pickle:
            value = pickle.dumps(value, PICKLE_PROTOCOL)
            flags |= PICKLE_FLAG
        elif serialize:
            value = json.dumps(value).encode('ascii')
            flags |= JSON_FLAG
        elif not isinstance(value, bytes):
            value = str(value).encode('utf-8')

        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(set_msg(key, flags, timeout, value))
                    # Wait for the set to complete
                    fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)

    def get(self, key):
        """
        Gets the object specified by key.  It will also unserialize the object
        before returning if it is serialized in memcache with JSON, or if it
        is pickled and unpickling is allowed.

        :param key: key
        :returns: value of the key in memcache
        """
        key = md5hash(key)
        value = None
        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b'get ' + key + b'\r\n')
                    line = fp.readline().strip().split()
                    while True:
                        if not line:
                            raise MemcacheConnectionError('incomplete read')
                        if line[0].upper() == b'END':
                            break
                        if line[0].upper() == b'VALUE' and line[1] == key:
                            size = int(line[3])
                            value = fp.read(size)
                            if int(line[2]) & PICKLE_FLAG:
                                if self._allow_unpickle:
                                    value = pickle.loads(value)
                                else:
                                    value = None
                            elif int(line[2]) & JSON_FLAG:
                                value = json.loads(value)
                            fp.readline()
                        line = fp.readline().strip().split()
                    self._return_conn(server, fp, sock)
                    return value
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)

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
        key = md5hash(key)
        command = b'incr'
        if delta < 0:
            command = b'decr'
        delta = str(abs(int(delta))).encode('ascii')
        timeout = sanitize_timeout(time)
        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b' '.join([
                        command, key, delta]) + b'\r\n')
                    line = fp.readline().strip().split()
                    if not line:
                        raise MemcacheConnectionError('incomplete read')
                    if line[0].upper() == b'NOT_FOUND':
                        add_val = delta
                        if command == b'decr':
                            add_val = b'0'
                        sock.sendall(b' '.join([
                            b'add', key, b'0', str(timeout).encode('ascii'),
                            str(len(add_val)).encode('ascii')
                        ]) + b'\r\n' + add_val + b'\r\n')
                        line = fp.readline().strip().split()
                        if line[0].upper() == b'NOT_STORED':
                            sock.sendall(b' '.join([
                                command, key, delta]) + b'\r\n')
                            line = fp.readline().strip().split()
                            ret = int(line[0].strip())
                        else:
                            ret = int(add_val)
                    else:
                        ret = int(line[0].strip())
                    self._return_conn(server, fp, sock)
                    return ret
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)
        raise MemcacheConnectionError("No Memcached connections succeeded.")

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

    def delete(self, key):
        """
        Deletes a key/value pair from memcache.

        :param key: key to be deleted
        """
        key = md5hash(key)
        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b'delete ' + key + b'\r\n')
                    # Wait for the delete to complete
                    fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)

    def set_multi(self, mapping, server_key, serialize=True, time=0,
                  min_compress_len=0):
        """
        Sets multiple key/value pairs in memcache.

        :param mapping: dictionary of keys and values to be set in memcache
        :param server_key: key to use in determining which server in the ring
                            is used
        :param serialize: if True, value is serialized with JSON before sending
                          to memcache, or with pickle if configured to use
                          pickle instead of JSON (to avoid cache poisoning)
        :param time: the time to live
        :min_compress_len: minimum compress length, this parameter was added
                           to keep the signature compatible with
                           python-memcached interface. This implementation
                           ignores it
        """
        server_key = md5hash(server_key)
        timeout = sanitize_timeout(time)
        msg = []
        for key, value in mapping.items():
            key = md5hash(key)
            flags = 0
            if serialize and self._allow_pickle:
                value = pickle.dumps(value, PICKLE_PROTOCOL)
                flags |= PICKLE_FLAG
            elif serialize:
                value = json.dumps(value).encode('ascii')
                flags |= JSON_FLAG
            msg.append(set_msg(key, flags, timeout, value))
        for (server, fp, sock) in self._get_conns(server_key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b''.join(msg))
                    # Wait for the set to complete
                    for line in range(len(mapping)):
                        fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)

    def get_multi(self, keys, server_key):
        """
        Gets multiple values from memcache for the given keys.

        :param keys: keys for values to be retrieved from memcache
        :param server_key: key to use in determining which server in the ring
                           is used
        :returns: list of values
        """
        server_key = md5hash(server_key)
        keys = [md5hash(key) for key in keys]
        for (server, fp, sock) in self._get_conns(server_key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(b'get ' + b' '.join(keys) + b'\r\n')
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
                                if self._allow_unpickle:
                                    value = pickle.loads(value)
                                else:
                                    value = None
                            elif int(line[2]) & JSON_FLAG:
                                value = json.loads(value)
                            responses[line[1]] = value
                            fp.readline()
                        line = fp.readline().strip().split()
                    values = []
                    for key in keys:
                        if key in responses:
                            values.append(responses[key])
                        else:
                            values.append(None)
                    self._return_conn(server, fp, sock)
                    return values
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)
