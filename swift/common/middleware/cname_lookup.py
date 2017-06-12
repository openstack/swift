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
CNAME Lookup Middleware

Middleware that translates an unknown domain in the host header to
something that ends with the configured storage_domain by looking up
the given domain's CNAME record in DNS.

This middleware will continue to follow a CNAME chain in DNS until it finds
a record ending in the configured storage domain or it reaches the configured
maximum lookup depth. If a match is found, the environment's Host header is
rewritten and the request is passed further down the WSGI chain.
"""

from six.moves import range

from swift import gettext_ as _

try:
    import dns.resolver
    import dns.exception
except ImportError:
    # catch this to allow docs to be built without the dependency
    MODULE_DEPENDENCY_MET = False
else:  # executed if the try block finishes with no errors
    MODULE_DEPENDENCY_MET = True

from swift.common.middleware import RewriteContext
from swift.common.swob import Request, HTTPBadRequest
from swift.common.utils import cache_from_env, get_logger, is_valid_ip, \
    list_from_csv, parse_socket_string, register_swift_info


def lookup_cname(domain, resolver):  # pragma: no cover
    """
    Given a domain, returns its DNS CNAME mapping and DNS ttl.

    :param domain: domain to query on
    :param resolver: dns.resolver.Resolver() instance used for executing DNS
                     queries
    :returns: (ttl, result)
    """
    try:
        answer = resolver.query(domain, 'CNAME').rrset
        ttl = answer.ttl
        result = answer.items[0].to_text()
        result = result.rstrip('.')
        return ttl, result
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
        # As the memcache lib returns None when nothing is found in cache,
        # returning false helps to distinguish between "nothing in cache"
        # (None) and "nothing to cache" (False).
        return 60, False
    except (dns.exception.DNSException):
        return 0, None


class _CnameLookupContext(RewriteContext):
    base_re = r'^(https?://)%s(/.*)?$'


class CNAMELookupMiddleware(object):
    """
    CNAME Lookup Middleware

    See above for a full description.

    :param app: The next WSGI filter or app in the paste.deploy
                chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        if not MODULE_DEPENDENCY_MET:
            # reraise the exception if the dependency wasn't met
            raise ImportError('dnspython is required for this module')
        self.app = app
        storage_domain = conf.get('storage_domain', 'example.com')
        self.storage_domain = ['.' + s for s in
                               list_from_csv(storage_domain)
                               if not s.startswith('.')]
        self.storage_domain += [s for s in list_from_csv(storage_domain)
                                if s.startswith('.')]
        self.lookup_depth = int(conf.get('lookup_depth', '1'))
        nameservers = list_from_csv(conf.get('nameservers'))
        try:
            for i, server in enumerate(nameservers):
                ip_or_host, maybe_port = nameservers[i] = \
                    parse_socket_string(server, None)
                if not is_valid_ip(ip_or_host):
                    raise ValueError
                if maybe_port is not None:
                    int(maybe_port)
        except ValueError:
            raise ValueError('Invalid cname_lookup/nameservers configuration '
                             'found. All nameservers must be valid IPv4 or '
                             'IPv6, followed by an optional :<integer> port.')
        self.resolver = dns.resolver.Resolver()
        if nameservers:
            self.resolver.nameservers = [ip for (ip, port) in nameservers]
            self.resolver.nameserver_ports = {
                ip: int(port) for (ip, port) in nameservers
                if port is not None}
        self.memcache = None
        self.logger = get_logger(conf, log_route='cname-lookup')

    def _domain_endswith_in_storage_domain(self, a_domain):
        a_domain = '.' + a_domain
        for domain in self.storage_domain:
            if a_domain.endswith(domain):
                return True
        return False

    def __call__(self, env, start_response):
        if not self.storage_domain:
            return self.app(env, start_response)
        if 'HTTP_HOST' in env:
            requested_host = given_domain = env['HTTP_HOST']
        else:
            requested_host = given_domain = env['SERVER_NAME']
        port = ''
        if ':' in given_domain:
            given_domain, port = given_domain.rsplit(':', 1)
        if is_valid_ip(given_domain):
            return self.app(env, start_response)
        a_domain = given_domain
        if not self._domain_endswith_in_storage_domain(a_domain):
            if self.memcache is None:
                self.memcache = cache_from_env(env)
            error = True
            for tries in range(self.lookup_depth):
                found_domain = None
                if self.memcache:
                    memcache_key = ''.join(['cname-', a_domain])
                    found_domain = self.memcache.get(memcache_key)
                if found_domain is None:
                    ttl, found_domain = lookup_cname(a_domain, self.resolver)
                    if self.memcache and ttl > 0:
                        memcache_key = ''.join(['cname-', given_domain])
                        self.memcache.set(memcache_key, found_domain,
                                          time=ttl)
                if not found_domain or found_domain == a_domain:
                    # no CNAME records or we're at the last lookup
                    error = True
                    found_domain = None
                    break
                elif self._domain_endswith_in_storage_domain(found_domain):
                    # Found it!
                    self.logger.info(
                        _('Mapped %(given_domain)s to %(found_domain)s') %
                        {'given_domain': given_domain,
                         'found_domain': found_domain})
                    if port:
                        env['HTTP_HOST'] = ':'.join([found_domain, port])
                    else:
                        env['HTTP_HOST'] = found_domain
                    error = False
                    break
                else:
                    # try one more deep in the chain
                    self.logger.debug(
                        _('Following CNAME chain for  '
                          '%(given_domain)s to %(found_domain)s') %
                        {'given_domain': given_domain,
                         'found_domain': found_domain})
                    a_domain = found_domain
            if error:
                if found_domain:
                    msg = 'CNAME lookup failed after %d tries' % \
                        self.lookup_depth
                else:
                    msg = 'CNAME lookup failed to resolve to a valid domain'
                resp = HTTPBadRequest(request=Request(env), body=msg,
                                      content_type='text/plain')
                return resp(env, start_response)
            else:
                context = _CnameLookupContext(self.app, requested_host,
                                              env['HTTP_HOST'])
                return context.handle_request(env, start_response)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):  # pragma: no cover
    conf = global_conf.copy()
    conf.update(local_conf)

    register_swift_info('cname_lookup',
                        lookup_depth=int(conf.get('lookup_depth', '1')))

    def cname_filter(app):
        return CNAMELookupMiddleware(app, conf)
    return cname_filter
