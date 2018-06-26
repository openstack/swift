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
Domain Remap Middleware

Middleware that translates container and account parts of a domain to path
parameters that the proxy server understands.

Translation is only performed when the request URL's host domain matches one of
a list of domains. This list may be configured by the option
``storage_domain``, and defaults to the single domain ``example.com``.

If not already present, a configurable ``path_root``, which defaults to ``v1``,
will be added to the start of the translated path.

For example, with the default configuration::

    container.AUTH-account.example.com/object
    container.AUTH-account.example.com/v1/object

would both be translated to::

    container.AUTH-account.example.com/v1/AUTH_account/container/object

and::

    AUTH-account.example.com/container/object
    AUTH-account.example.com/v1/container/object

would both be translated to::

    AUTH-account.example.com/v1/AUTH_account/container/object

Additionally, translation is only performed when the account name in the
translated path starts with a reseller prefix matching one of a list configured
by the option ``reseller_prefixes``, or when no match is found but a
``default_reseller_prefix`` has been configured.

The ``reseller_prefixes`` list defaults to the single prefix ``AUTH``. The
``default_reseller_prefix`` is not configured by default.

Browsers can convert a host header to lowercase, so the middleware checks that
the reseller prefix on the account name is the correct case. This is done by
comparing the items in the ``reseller_prefixes`` config option to the found
prefix. If they match except for case, the item from ``reseller_prefixes`` will
be used instead of the found reseller prefix. The middleware will also replace
any hyphen ('-') in the account name with an underscore ('_').

For example, with the default configuration::

    auth-account.example.com/container/object
    AUTH-account.example.com/container/object
    auth_account.example.com/container/object
    AUTH_account.example.com/container/object

would all be translated to::

    <unchanged>.example.com/v1/AUTH_account/container/object

When no match is found in ``reseller_prefixes``, the
``default_reseller_prefix`` config option is used. When no
``default_reseller_prefix`` is configured, any request with an account prefix
not in the ``reseller_prefixes`` list will be ignored by this middleware.

For example, with ``default_reseller_prefix = AUTH``::

    account.example.com/container/object

would be translated to::

    account.example.com/v1/AUTH_account/container/object

Note that this middleware requires that container names and account names
(except as described above) must be DNS-compatible. This means that the account
name created in the system and the containers created by users cannot exceed 63
characters or have UTF-8 characters. These are restrictions over and above what
Swift requires and are not explicitly checked. Simply put, this middleware
will do a best-effort attempt to derive account and container names from
elements in the domain name and put those derived values into the URL path
(leaving the ``Host`` header unchanged).

Also note that using :doc:`overview_container_sync` with remapped domain names
is not advised. With :doc:`overview_container_sync`, you should use the true
storage end points as sync destinations.
"""

from swift.common.middleware import RewriteContext
from swift.common.swob import Request, HTTPBadRequest
from swift.common.utils import config_true_value, list_from_csv, \
    register_swift_info


class _DomainRemapContext(RewriteContext):
    base_re = r'^(https?://[^/]+)%s(.*)$'


class DomainRemapMiddleware(object):
    """
    Domain Remap Middleware

    See above for a full description.

    :param app: The next WSGI filter or app in the paste.deploy
                chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        self.app = app
        storage_domain = conf.get('storage_domain', 'example.com')
        self.storage_domain = ['.' + s for s in
                               list_from_csv(storage_domain)
                               if not s.startswith('.')]
        self.storage_domain += [s for s in list_from_csv(storage_domain)
                                if s.startswith('.')]
        self.path_root = conf.get('path_root', 'v1').strip('/') + '/'
        prefixes = conf.get('reseller_prefixes', 'AUTH')
        self.reseller_prefixes = list_from_csv(prefixes)
        self.reseller_prefixes_lower = [x.lower()
                                        for x in self.reseller_prefixes]
        self.default_reseller_prefix = conf.get('default_reseller_prefix')
        self.mangle_client_paths = config_true_value(
            conf.get('mangle_client_paths'))

    def __call__(self, env, start_response):
        if not self.storage_domain:
            return self.app(env, start_response)
        if 'HTTP_HOST' in env:
            given_domain = env['HTTP_HOST']
        else:
            given_domain = env['SERVER_NAME']
        port = ''
        if ':' in given_domain:
            given_domain, port = given_domain.rsplit(':', 1)
        storage_domain = next((domain for domain in self.storage_domain
                               if given_domain.endswith(domain)), None)
        if storage_domain:
            parts_to_parse = given_domain[:-len(storage_domain)]
            parts_to_parse = parts_to_parse.strip('.').split('.')
            len_parts_to_parse = len(parts_to_parse)
            if len_parts_to_parse == 2:
                container, account = parts_to_parse
            elif len_parts_to_parse == 1:
                container, account = None, parts_to_parse[0]
            else:
                resp = HTTPBadRequest(request=Request(env),
                                      body=b'Bad domain in host header',
                                      content_type='text/plain')
                return resp(env, start_response)
            if len(self.reseller_prefixes) > 0:
                if '_' not in account and '-' in account:
                    account = account.replace('-', '_', 1)
                account_reseller_prefix = account.split('_', 1)[0].lower()

                if account_reseller_prefix in self.reseller_prefixes_lower:
                    prefix_index = self.reseller_prefixes_lower.index(
                        account_reseller_prefix)
                    real_prefix = self.reseller_prefixes[prefix_index]
                    if not account.startswith(real_prefix):
                        account_suffix = account[len(real_prefix):]
                        account = real_prefix + account_suffix
                elif self.default_reseller_prefix:
                    # account prefix is not in config list. Add default one.
                    account = "%s_%s" % (self.default_reseller_prefix, account)
                else:
                    # account prefix is not in config list. bail.
                    return self.app(env, start_response)

            requested_path = env['PATH_INFO']
            path = requested_path[1:]
            new_path_parts = ['', self.path_root[:-1], account]
            if container:
                new_path_parts.append(container)
            if self.mangle_client_paths and (path + '/').startswith(
                    self.path_root):
                path = path[len(self.path_root):]
            new_path_parts.append(path)
            new_path = '/'.join(new_path_parts)
            env['PATH_INFO'] = new_path

            context = _DomainRemapContext(self.app, requested_path, new_path)
            return context.handle_request(env, start_response)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    register_swift_info(
        'domain_remap',
        default_reseller_prefix=conf.get('default_reseller_prefix'))

    def domain_filter(app):
        return DomainRemapMiddleware(app, conf)
    return domain_filter
