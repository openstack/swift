# Copyright (c) 2010-2011 OpenStack, LLC.
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

from swift.common.memcached import MemcacheRing


class MemcacheMiddleware(object):
    """
    Caching middleware that manages caching in swift.
    """

    def __init__(self, app, conf):
        self.app = app
        self.memcache = MemcacheRing([s.strip() for s in
            conf.get('memcache_servers', '127.0.0.1:11211').split(',')
            if s.strip()])

    def __call__(self, env, start_response):
        env['swift.cache'] = self.memcache
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def cache_filter(app):
        return MemcacheMiddleware(app, conf)

    return cache_filter
