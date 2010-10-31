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


def lookup_cname(domain):
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
        if ':' in given_domain:
            given_domain, _ = given_domain.rsplit(':', 1)
        if not given_domain.endswith(self.storage_domain):
            if self.memcache is None:
                self.memcache = cache_from_env(env)
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
                    #TODO: set error and break from loop
                    found_domain = ''
                if found_domain == given_domain:
                    # we're at the last lookup
                    #TODO: set error and break from loop
                if found_domain.endswith(self.storage_domain):
                    break
                else:
                    given_domain = found_domain
            else:
                #TODO: change to error flag check rather than else
                if found_domain:
                    msg = 'CNAME lookup failed after %d tries' % \
                            self.lookup_depth
                else:
                    msg = 'CNAME lookup failed to resolve to a valid domain'
                resp = HTTPBadRequest(request=Request(env), body=msg,
                                      content_type='text/plain')
                return resp(env, start_response)
        return self.app(env, start_response)
        
        if given_domain != self.storage_domain and \
            given_domain.endswith(self.storage_domain):
            parts_to_parse = given_domain[:-len(self.storage_domain)]
            parts_to_parse = parts_to_parse.strip('.').split('.')
            len_parts_to_parse = len(parts_to_parse)
            if len_parts_to_parse == 2:
                container, account = parts_to_parse
            elif len_parts_to_parse == 1:
                container, account = None, parts_to_parse[0]
            else:
                resp = HTTPBadRequest(request=Request(env),
                                      body='Bad domain in host header',
                                      content_type='text/plain')
                return resp(env, start_response)
            if '_' not in account and '-' in account:
                account = account.replace('-', '_', 1)
            path = env['PATH_INFO'].strip('/')
            new_path_parts = ['', self.path_root, account]
            if container:
                new_path_parts.append(container)
            if path.startswith(self.path_root):
                path = path[len(self.path_root):].lstrip('/')
            if path:
                new_path_parts.append(path)
            new_path = '/'.join(new_path_parts)
            env['PATH_INFO'] = new_path
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def cname_filter(app):
        return CNAMELookupMiddleware(app, conf)
    return cname_filter
