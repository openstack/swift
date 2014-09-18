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

import cPickle as pickle
import logging
import time
from bisect import bisect
from swift import gettext_ as _
from hashlib import md5
from distutils.version import StrictVersion

from eventlet.green import socket
from eventlet.pools import Pool
from eventlet import Timeout, __version__ as eventlet_version

from swift.common.utils import json

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
    return md5(key).hexdigest()


def sanitize_timeout(timeout):
    """
    Sanitize a timeout value to use an absolute expiration time if the delta
    is greater than 30 days (in seconds). Note that the memcached server
    translates negative values to mean a delta of 30 days in seconds (and 1
    additional second), client beware.
    """
    if timeout > (30 * 24 * 60 * 60):
        timeout += time.time()
    return timeout


class MemcacheConnectionError(Exception):
    pass


class MemcachePoolTimeout(Timeout):
    pass


class MemcacheConnPool(Pool):
    """Connection pool for Memcache Connections"""

    def __init__(self, server, size, connect_timeout):
        Pool.__init__(self, max_size=size)
        self.server = server
        self._connect_timeout = connect_timeout
        self._parent_class_getter = super(MemcacheConnPool, self).get
        try:
            # call the patched .get() if eventlet is older than 0.9.17
            if StrictVersion(eventlet_version) < StrictVersion('0.9.17'):
                self._parent_class_getter = self._upstream_fixed_get
        except ValueError:
            # "invalid" version number or otherwise error parsing version
            pass

    def create(self):
        if ':' in self.server:
            host, port = self.server.split(':')
        else:
            host = self.server
            port = DEFAULT_MEMCACHED_PORT
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with Timeout(self._connect_timeout):
            sock.connect((host, int(port)))
        return (sock.makefile(), sock)

    def get(self):
        fp, sock = self._parent_class_getter()
        if fp is None:
            # An error happened previously, so we need a new connection
            fp, sock = self.create()
        return fp, sock

    # The following method is from eventlet post 0.9.16. This version
    # properly keeps track of pool size accounting, and therefore doesn't
    # let the pool grow without bound. This patched version is the result
    # of commit f5e5b2bda7b442f0262ee1084deefcc5a1cc0694 in eventlet and is
    # documented at https://bitbucket.org/eventlet/eventlet/issue/91
    def _upstream_fixed_get(self):
        """Return an item from the pool, when one is available.  This may
        cause the calling greenthread to block.
        """
        if self.free_items:
            return self.free_items.popleft()
        self.current_size += 1
        if self.current_size <= self.max_size:
            try:
                created = self.create()
            except:  # noqa
                self.current_size -= 1
                raise
            return created
        self.current_size -= 1  # did not create
        return self.channel.get()


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
            for i in xrange(NODE_WEIGHT):
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
            logging.error(_("Timeout %(action)s to memcached: %(server)s"),
                          {'action': action, 'server': server})
        else:
            logging.exception(_("Error %(action)s to memcached: %(server)s"),
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
                logging.error(_('Error limiting server %s'), server)

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

    def set(self, key, value, serialize=True, timeout=0, time=0,
            min_compress_len=0):
        """
        Set a key/value pair in memcache

        :param key: key
        :param value: value
        :param serialize: if True, value is serialized with JSON before sending
                          to memcache, or with pickle if configured to use
                          pickle instead of JSON (to avoid cache poisoning)
        :param timeout: ttl in memcache, this parameter is now deprecated. It
                        will be removed in next release of OpenStack,
                        use time parameter instead in the future
        :time: equivalent to timeout, this parameter is added to keep the
               signature compatible with python-memcached interface. This
               implementation will take this value and sign it to the
               parameter timeout
        :min_compress_len: minimum compress length, this parameter was added
                           to keep the signature compatible with
                           python-memcached interface. This implementation
                           ignores it.
        """
        key = md5hash(key)
        if timeout:
            logging.warn("parameter timeout has been deprecated, use time")
        timeout = sanitize_timeout(time or timeout)
        flags = 0
        if serialize and self._allow_pickle:
            value = pickle.dumps(value, PICKLE_PROTOCOL)
            flags |= PICKLE_FLAG
        elif serialize:
            value = json.dumps(value)
            flags |= JSON_FLAG
        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall('set %s %d %d %s\r\n%s\r\n' %
                                 (key, flags, timeout, len(value), value))
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
                    sock.sendall('get %s\r\n' % key)
                    line = fp.readline().strip().split()
                    while line[0].upper() != 'END':
                        if line[0].upper() == 'VALUE' and line[1] == key:
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

    def incr(self, key, delta=1, time=0, timeout=0):
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
        :param time: the time to live. This parameter deprecates parameter
                     timeout. The addition of this parameter is to make the
                     interface consistent with set and set_multi methods
        :param timeout: ttl in memcache, deprecated, will be removed in future
                        OpenStack releases
        :returns: result of incrementing
        :raises MemcacheConnectionError:
        """
        if timeout:
            logging.warn("parameter timeout has been deprecated, use time")
        key = md5hash(key)
        command = 'incr'
        if delta < 0:
            command = 'decr'
        delta = str(abs(int(delta)))
        timeout = sanitize_timeout(time or timeout)
        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall('%s %s %s\r\n' % (command, key, delta))
                    line = fp.readline().strip().split()
                    if line[0].upper() == 'NOT_FOUND':
                        add_val = delta
                        if command == 'decr':
                            add_val = '0'
                        sock.sendall('add %s %d %d %s\r\n%s\r\n' %
                                     (key, 0, timeout, len(add_val), add_val))
                        line = fp.readline().strip().split()
                        if line[0].upper() == 'NOT_STORED':
                            sock.sendall('%s %s %s\r\n' % (command, key,
                                                           delta))
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

    def decr(self, key, delta=1, time=0, timeout=0):
        """
        Decrements a key which has a numeric value by delta. Calls incr with
        -delta.

        :param key: key
        :param delta: amount to subtract to the value of key (or set the
                      value to 0 if the key is not found) will be cast to
                      an int
        :param time: the time to live. This parameter depcates parameter
                     timeout. The addition of this parameter is to make the
                     interface consistent with set and set_multi methods
        :param timeout: ttl in memcache, deprecated, will be removed in future
                        OpenStack releases
        :returns: result of decrementing
        :raises MemcacheConnectionError:
        """
        if timeout:
            logging.warn("parameter timeout has been deprecated, use time")

        return self.incr(key, delta=-delta, time=(time or timeout))

    def delete(self, key):
        """
        Deletes a key/value pair from memcache.

        :param key: key to be deleted
        """
        key = md5hash(key)
        for (server, fp, sock) in self._get_conns(key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall('delete %s\r\n' % key)
                    # Wait for the delete to complete
                    fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)

    def set_multi(self, mapping, server_key, serialize=True, timeout=0,
                  time=0, min_compress_len=0):
        """
        Sets multiple key/value pairs in memcache.

        :param mapping: dictionary of keys and values to be set in memcache
        :param servery_key: key to use in determining which server in the ring
                            is used
        :param serialize: if True, value is serialized with JSON before sending
                          to memcache, or with pickle if configured to use
                          pickle instead of JSON (to avoid cache poisoning)
        :param timeout: ttl for memcache. This parameter is now deprecated, it
                        will be removed in next release of OpenStack, use time
                        parameter instead in the future
        :time: equalvent to timeout, this parameter is added to keep the
               signature compatible with python-memcached interface. This
               implementation will take this value and sign it to parameter
               timeout
        :min_compress_len: minimum compress length, this parameter was added
                           to keep the signature compatible with
                           python-memcached interface. This implementation
                           ignores it
        """
        if timeout:
            logging.warn("parameter timeout has been deprecated, use time")

        server_key = md5hash(server_key)
        timeout = sanitize_timeout(time or timeout)
        msg = ''
        for key, value in mapping.iteritems():
            key = md5hash(key)
            flags = 0
            if serialize and self._allow_pickle:
                value = pickle.dumps(value, PICKLE_PROTOCOL)
                flags |= PICKLE_FLAG
            elif serialize:
                value = json.dumps(value)
                flags |= JSON_FLAG
            msg += ('set %s %d %d %s\r\n%s\r\n' %
                    (key, flags, timeout, len(value), value))
        for (server, fp, sock) in self._get_conns(server_key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall(msg)
                    # Wait for the set to complete
                    for _ in range(len(mapping)):
                        fp.readline()
                    self._return_conn(server, fp, sock)
                    return
            except (Exception, Timeout) as e:
                self._exception_occurred(server, e, sock=sock, fp=fp)

    def get_multi(self, keys, server_key):
        """
        Gets multiple values from memcache for the given keys.

        :param keys: keys for values to be retrieved from memcache
        :param servery_key: key to use in determining which server in the ring
                            is used
        :returns: list of values
        """
        server_key = md5hash(server_key)
        keys = [md5hash(key) for key in keys]
        for (server, fp, sock) in self._get_conns(server_key):
            try:
                with Timeout(self._io_timeout):
                    sock.sendall('get %s\r\n' % ' '.join(keys))
                    line = fp.readline().strip().split()
                    responses = {}
                    while line[0].upper() != 'END':
                        if line[0].upper() == 'VALUE':
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
