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

"""
Lucid comes with memcached: v1.4.2.  Protocol documentation for that
version is at:

http://github.com/memcached/memcached/blob/1.4.2/doc/protocol.txt
"""

import cPickle as pickle
import logging
import socket
import time
from bisect import bisect
from hashlib import md5

DEFAULT_MEMCACHED_PORT = 11211

CONN_TIMEOUT = 0.3
IO_TIMEOUT = 2.0
PICKLE_FLAG = 1
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


class MemcacheConnectionError(Exception):
    pass


class MemcacheRing(object):
    """
    Simple, consistent-hashed memcache client.
    """

    def __init__(self, servers, connect_timeout=CONN_TIMEOUT,
                 io_timeout=IO_TIMEOUT, tries=TRY_COUNT):
        self._ring = {}
        self._errors = dict(((serv, []) for serv in servers))
        self._error_limited = dict(((serv, 0) for serv in servers))
        for server in sorted(servers):
            for i in xrange(NODE_WEIGHT):
                self._ring[md5hash('%s-%s' % (server, i))] = server
        self._tries = tries if tries <= len(servers) else len(servers)
        self._sorted = sorted(self._ring.keys())
        self._client_cache = dict(((server, []) for server in servers))
        self._connect_timeout = connect_timeout
        self._io_timeout = io_timeout

    def _exception_occurred(self, server, e, action='talking'):
        if isinstance(e, socket.timeout):
            logging.error(_("Timeout %(action)s to memcached: %(server)s"),
                {'action': action, 'server': server})
        else:
            logging.exception(_("Error %(action)s to memcached: %(server)s"),
                {'action': action, 'server': server})
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
            try:
                fp, sock = self._client_cache[server].pop()
                yield server, fp, sock
            except IndexError:
                try:
                    if ':' in server:
                        host, port = server.split(':')
                    else:
                        host = server
                        port = DEFAULT_MEMCACHED_PORT
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.settimeout(self._connect_timeout)
                    sock.connect((host, int(port)))
                    sock.settimeout(self._io_timeout)
                    yield server, sock.makefile(), sock
                except Exception, e:
                    self._exception_occurred(server, e, 'connecting')

    def _return_conn(self, server, fp, sock):
        """ Returns a server connection to the pool """
        self._client_cache[server].append((fp, sock))

    def set(self, key, value, serialize=True, timeout=0):
        """
        Set a key/value pair in memcache

        :param key: key
        :param value: value
        :param serialize: if True, value is pickled before sending to memcache
        :param timeout: ttl in memcache
        """
        key = md5hash(key)
        if timeout > 0:
            timeout += time.time()
        flags = 0
        if serialize:
            value = pickle.dumps(value, PICKLE_PROTOCOL)
            flags |= PICKLE_FLAG
        for (server, fp, sock) in self._get_conns(key):
            try:
                sock.sendall('set %s %d %d %s noreply\r\n%s\r\n' % \
                              (key, flags, timeout, len(value), value))
                self._return_conn(server, fp, sock)
                return
            except Exception, e:
                self._exception_occurred(server, e)

    def get(self, key):
        """
        Gets the object specified by key.  It will also unpickle the object
        before returning if it is pickled in memcache.

        :param key: key
        :returns: value of the key in memcache
        """
        key = md5hash(key)
        value = None
        for (server, fp, sock) in self._get_conns(key):
            try:
                sock.sendall('get %s\r\n' % key)
                line = fp.readline().strip().split()
                while line[0].upper() != 'END':
                    if line[0].upper() == 'VALUE' and line[1] == key:
                        size = int(line[3])
                        value = fp.read(size)
                        if int(line[2]) & PICKLE_FLAG:
                            value = pickle.loads(value)
                        fp.readline()
                    line = fp.readline().strip().split()
                self._return_conn(server, fp, sock)
                return value
            except Exception, e:
                self._exception_occurred(server, e)

    def incr(self, key, delta=1, timeout=0):
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
        :param timeout: ttl in memcache
        :raises MemcacheConnectionError:
        """
        key = md5hash(key)
        command = 'incr'
        if delta < 0:
            command = 'decr'
        delta = str(abs(int(delta)))
        for (server, fp, sock) in self._get_conns(key):
            try:
                sock.sendall('%s %s %s\r\n' % (command, key, delta))
                line = fp.readline().strip().split()
                if line[0].upper() == 'NOT_FOUND':
                    add_val = delta
                    if command == 'decr':
                        add_val = '0'
                    sock.sendall('add %s %d %d %s\r\n%s\r\n' % \
                                  (key, 0, timeout, len(add_val), add_val))
                    line = fp.readline().strip().split()
                    if line[0].upper() == 'NOT_STORED':
                        sock.sendall('%s %s %s\r\n' % (command, key, delta))
                        line = fp.readline().strip().split()
                        ret = int(line[0].strip())
                    else:
                        ret = int(add_val)
                else:
                    ret = int(line[0].strip())
                self._return_conn(server, fp, sock)
                return ret
            except Exception, e:
                self._exception_occurred(server, e)
        raise MemcacheConnectionError("No Memcached connections succeeded.")

    def decr(self, key, delta=1, timeout=0):
        """
        Decrements a key which has a numeric value by delta. Calls incr with
        -delta.

        :param key: key
        :param delta: amount to subtract to the value of key (or set the
                      value to 0 if the key is not found) will be cast to
                      an int
        :param timeout: ttl in memcache
        :raises MemcacheConnectionError:
        """
        self.incr(key, delta=-delta, timeout=timeout)

    def delete(self, key):
        """
        Deletes a key/value pair from memcache.

        :param key: key to be deleted
        """
        key = md5hash(key)
        for (server, fp, sock) in self._get_conns(key):
            try:
                sock.sendall('delete %s noreply\r\n' % key)
                self._return_conn(server, fp, sock)
                return
            except Exception, e:
                self._exception_occurred(server, e)

    def set_multi(self, mapping, server_key, serialize=True, timeout=0):
        """
        Sets multiple key/value pairs in memcache.

        :param mapping: dictonary of keys and values to be set in memcache
        :param servery_key: key to use in determining which server in the ring
                            is used
        :param serialize: if True, value is pickled before sending to memcache
        :param timeout: ttl for memcache
        """
        server_key = md5hash(server_key)
        if timeout > 0:
            timeout += time.time()
        msg = ''
        for key, value in mapping.iteritems():
            key = md5hash(key)
            flags = 0
            if serialize:
                value = pickle.dumps(value, PICKLE_PROTOCOL)
                flags |= PICKLE_FLAG
            msg += ('set %s %d %d %s noreply\r\n%s\r\n' %
                    (key, flags, timeout, len(value), value))
        for (server, fp, sock) in self._get_conns(server_key):
            try:
                sock.sendall(msg)
                self._return_conn(server, fp, sock)
                return
            except Exception, e:
                self._exception_occurred(server, e)

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
                sock.sendall('get %s\r\n' % ' '.join(keys))
                line = fp.readline().strip().split()
                responses = {}
                while line[0].upper() != 'END':
                    if line[0].upper() == 'VALUE':
                        size = int(line[3])
                        value = fp.read(size)
                        if int(line[2]) & PICKLE_FLAG:
                            value = pickle.loads(value)
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
            except Exception, e:
                self._exception_occurred(server, e)
