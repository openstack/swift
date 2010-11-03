# Copyright (c) 2010 OpenStack, LLC.
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

from webob import Request
from webob.exc import HTTPBadRequest
import dns.resolver

from swift.common.utils import cache_from_env


def lookup_cname(domain):  # pragma: no cover
    answer = dns.resolver.query(domain, 'CNAME').rrset
    ttl = answer.ttl
    result = answer.name.to_text()
    return ttl, result


class CNAMELookupMiddleware(object):
    """
    Middleware that translates container and account parts of a domain to
    path parameters that the proxy server understands.

    container.account.storageurl/object gets translated to
    container.account.storageurl/path_root/account/container/object

    account.storageurl/path_root/container/object gets translated to
    account.storageurl/path_root/account/container/object
    """

    def __init__(self, app, conf):
        self.app = app
        self.storage_domain = conf.get('storage_domain', 'example.com')
        self.lookup_depth = int(conf.get('lookup_depth', '1'))
        self.memcache = None

    def __call__(self, env, start_response):
        given_domain = env['HTTP_HOST']
        port = ''
        if ':' in given_domain:
            given_domain, port = given_domain.rsplit(':', 1)
        if not given_domain.endswith(self.storage_domain):
            if self.memcache is None:
                self.memcache = cache_from_env(env)
            error = True
            for tries in xrange(self.lookup_depth):
                found_domain = None
                if self.memcache:
                    found_domain = self.memcache.get(given_domain)
                if not found_domain:
                    ttl, found_domain = lookup_cname(given_domain)
                    if self.memcache:
                        self.memcache.set(given_domain, found_domain,
                                          timeout=ttl)
                if found_domain is None:
                    # something weird happened
                    error = True
                    break
                elif found_domain == given_domain:
                    # we're at the last lookup
                    error = True
                    break
                elif found_domain.endswith(self.storage_domain):
                    # Found it!
                    if port:
                        env['HTTP_HOST'] = ':'.join([found_domain, port])
                    else:
                        env['HTTP_HOST'] = found_domain
                    error = False
                    break
                else:
                    # try one more deep in the chain
                    given_domain = found_domain
            if error:
                if found_domain:
                    msg = 'CNAME lookup failed after %d tries' % \
                            self.lookup_depth
                else:
                    msg = 'CNAME lookup failed to resolve to a valid domain'
                resp = HTTPBadRequest(request=Request(env), body=msg,
                                      content_type='text/plain')
                return resp(env, start_response)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):  # pragma: no cover
    conf = global_conf.copy()
    conf.update(local_conf)

    def cname_filter(app):
        return CNAMELookupMiddleware(app, conf)
    return cname_filter
