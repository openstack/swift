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

from webob import Request
from webob.exc import HTTPBadRequest


class DomainRemapMiddleware(object):
    """
    Middleware that translates container and account parts of a domain to
    path parameters that the proxy server understands.

    container.account.storageurl/object gets translated to
    container.account.storageurl/path_root/account/container/object

    account.storageurl/path_root/container/object gets translated to
    account.storageurl/path_root/account/container/object

    Browsers can convert a host header to lowercase, so check that reseller
    prefix on the account is the correct case. This is done by comparing the
    items in the reseller_prefixes config option to the found prefix. If they
    match except for case, the item from reseller_prefixes will be used
    instead of the found reseller prefix. The reseller_prefixes list is
    exclusive. If defined, any request with an account prefix not in that list
    will be ignored by this middleware. reseller_prefixes defaults to 'AUTH'.

    Note that this middleware requires that container names and account names
    (except as described above) must be DNS-compatible. This means that the
    account name created in the system and the containers created by users
    cannot exceed 63 characters or have UTF-8 characters. These are
    restrictions over and above what swift requires and are not explicitly
    checked. Simply put, the this middleware will do a best-effort attempt to
    derive account and container names from elements in the domain name and
    put those derived values into the URL path (leaving the Host header
    unchanged).

    Also note that using container sync with remapped domain names is not
    advised. With container sync, you should use the true storage end points as
    sync destinations.
    """

    def __init__(self, app, conf):
        self.app = app
        self.storage_domain = conf.get('storage_domain', 'example.com')
        if self.storage_domain and self.storage_domain[0] != '.':
            self.storage_domain = '.' + self.storage_domain
        self.path_root = conf.get('path_root', 'v1').strip('/')
        prefixes = conf.get('reseller_prefixes', 'AUTH')
        self.reseller_prefixes = [x.strip() for x in prefixes.split(',')
                                  if x.strip()]
        self.reseller_prefixes_lower = [x.lower()
                                        for x in self.reseller_prefixes]

    def __call__(self, env, start_response):
        if not self.storage_domain:
            return self.app(env, start_response)
        given_domain = env['HTTP_HOST']
        port = ''
        if ':' in given_domain:
            given_domain, port = given_domain.rsplit(':', 1)
        if given_domain.endswith(self.storage_domain):
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
            account_reseller_prefix = account.split('_', 1)[0].lower()
            if account_reseller_prefix not in self.reseller_prefixes_lower:
                # account prefix is not in config list. bail.
                return self.app(env, start_response)
            prefix_index = self.reseller_prefixes_lower.index(
                                                    account_reseller_prefix)
            real_prefix = self.reseller_prefixes[prefix_index]
            if not account.startswith(real_prefix):
                account_suffix = account[len(real_prefix):]
                account = real_prefix + account_suffix
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

    def domain_filter(app):
        return DomainRemapMiddleware(app, conf)
    return domain_filter
