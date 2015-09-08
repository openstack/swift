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

import os

from six.moves.configparser import ConfigParser, NoSectionError, NoOptionError

from swift.common.memcached import (MemcacheRing, CONN_TIMEOUT, POOL_TIMEOUT,
                                    IO_TIMEOUT, TRY_COUNT)


class MemcacheMiddleware(object):
    """
    Caching middleware that manages caching in swift.
    """

    def __init__(self, app, conf):
        self.app = app
        self.memcache_servers = conf.get('memcache_servers')
        serialization_format = conf.get('memcache_serialization_support')
        try:
            # Originally, while we documented using memcache_max_connections
            # we only accepted max_connections
            max_conns = int(conf.get('memcache_max_connections',
                                     conf.get('max_connections', 0)))
        except ValueError:
            max_conns = 0

        memcache_options = {}
        if (not self.memcache_servers
                or serialization_format is None
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

                if not self.memcache_servers:
                    try:
                        self.memcache_servers = \
                            memcache_conf.get('memcache', 'memcache_servers')
                    except (NoSectionError, NoOptionError):
                        pass
                if serialization_format is None:
                    try:
                        serialization_format = \
                            memcache_conf.get('memcache',
                                              'memcache_serialization_support')
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

        if not self.memcache_servers:
            self.memcache_servers = '127.0.0.1:11211'
        if max_conns <= 0:
            max_conns = 2
        if serialization_format is None:
            serialization_format = 2
        else:
            serialization_format = int(serialization_format)

        self.memcache = MemcacheRing(
            [s.strip() for s in self.memcache_servers.split(',') if s.strip()],
            connect_timeout=connect_timeout,
            pool_timeout=pool_timeout,
            tries=tries,
            io_timeout=io_timeout,
            allow_pickle=(serialization_format == 0),
            allow_unpickle=(serialization_format <= 1),
            max_conns=max_conns)

    def __call__(self, env, start_response):
        env['swift.cache'] = self.memcache
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def cache_filter(app):
        return MemcacheMiddleware(app, conf)

    return cache_filter
