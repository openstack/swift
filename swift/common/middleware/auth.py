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

from time import time

from eventlet.timeout import Timeout
from repoze.what.adapters import BaseSourceAdapter
from repoze.what.middleware import setup_auth
from repoze.what.predicates import in_any_group, NotAuthorizedError
from webob.exc import HTTPForbidden, HTTPUnauthorized

from swift.common.bufferedhttp import http_connect_raw as http_connect
from swift.common.utils import cache_from_env, split_path


class DevAuthorization(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf

    def __call__(self, environ, start_response):
        environ['swift.authorize'] = self.authorize
        environ['swift.clean_acl'] = self.clean_acl
        return self.app(environ, start_response)

    def authorize(self, req):
        version, account, container, obj = split_path(req.path, 1, 4, True)
        if not account:
            return self.denied_response(req)
        groups = [account]
        acl = self.parse_acl(getattr(req, 'acl', None))
        if acl:
            referrers, accounts, users = acl
            if referrers:
                parts = req.referer.split('//', 1)
                allow = False
                if len(parts) == 2:
                    rhost = parts[1].split('/', 1)[0].split(':', 1)[0].lower()
                else:
                    rhost = 'unknown'
                for mhost in referrers:
                    if mhost[0] == '-':
                        mhost = mhost[1:]
                        if mhost == rhost or \
                               (mhost[0] == '.' and rhost.endswith(mhost)):
                            allow = False
                    elif mhost == 'any' or mhost == rhost or \
                            (mhost[0] == '.' and rhost.endswith(mhost)):
                        allow = True
                if allow:
                    return None
            groups.extend(accounts)
            groups.extend(users)
        try:
            in_any_group(*groups).check_authorization(req.environ)
        except NotAuthorizedError:
            return self.denied_response(req)
        return None

    def denied_response(self, req):
        if req.remote_user:
            return HTTPForbidden(request=req)
        else:
            return HTTPUnauthorized(request=req)

    def clean_acl(self, header_name, value):
        values = []
        for raw_value in value.lower().split(','):
            raw_value = raw_value.strip()
            if raw_value:
                if ':' in raw_value:
                    first, second = \
                        (v.strip() for v in raw_value.split(':', 1))
                    if not first:
                        raise ValueError('No value before colon in %s' %
                                         repr(raw_value))
                    if first == '.ref' and 'write' in header_name:
                        raise ValueError('Referrers not allowed in write '
                                         'ACLs: %s' % repr(raw_value))
                    if second:
                        if first == '.ref' and second[0] == '-':
                            second = second[1:].strip()
                            if not second:
                                raise ValueError('No value after referrer '
                                    'deny designation in %s' % repr(raw_value))
                            second = '-' + second
                        values.append('%s:%s' % (first, second))
                    elif first == '.ref':
                        raise ValueError('No value after referrer designation '
                                         'in %s' % repr(raw_value))
                    else:
                        values.append(first)
                else:
                    values.append(raw_value)
        return ','.join(values)

    def parse_acl(self, acl_string):
        if not acl_string:
            return None
        referrers = []
        accounts = []
        users = []
        for value in acl_string.split(','):
            if value.startswith('.ref:'):
                referrers.append(value[len('.ref:'):])
            elif ':' in value:
                users.append(value)
            else:
                accounts.append(value)
        return (referrers, accounts, users)


class DevIdentifier(object):

    def __init__(self, conf):
        self.conf = conf

    def identify(self, env):
        return {'token':
                env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))}

    def remember(self, env, identity):
        return []

    def forget(self, env, identity):
        return []


class DevAuthenticator(object):

    def __init__(self, conf):
        self.conf = conf
        self.auth_host = conf.get('ip', '127.0.0.1')
        self.auth_port = int(conf.get('port', 11000))
        self.ssl = \
            conf.get('ssl', 'false').lower() in ('true', 'on', '1', 'yes')
        self.timeout = int(conf.get('node_timeout', 10))

    def authenticate(self, env, identity):
        token = identity.get('token')
        if not token:
            return None
        memcache_client = cache_from_env(env)
        key = 'devauth/%s' % token
        cached_auth_data = memcache_client.get(key)
        if cached_auth_data:
            start, expiration, user = cached_auth_data
            if time() - start <= expiration:
                return user
        with Timeout(self.timeout):
            conn = http_connect(self.auth_host, self.auth_port, 'GET',
                                '/token/%s' % token, ssl=self.ssl)
            resp = conn.getresponse()
            resp.read()
            conn.close()
        if resp.status == 204:
            expiration = float(resp.getheader('x-auth-ttl'))
            user = resp.getheader('x-auth-user')
            memcache_client.set(key, (time(), expiration, user),
                                timeout=expiration)
            return user
        return None


class DevChallenger(object):

    def __init__(self, conf):
        self.conf = conf

    def challenge(self, env, status, app_headers, forget_headers):
        def no_challenge(env, start_response):
            start_response(str(status), [])
            return []
        return no_challenge


class DevGroupSourceAdapter(BaseSourceAdapter):

    def __init__(self, *args, **kwargs):
        super(DevGroupSourceAdapter, self).__init__(*args, **kwargs)
        self.sections = {}

    def _get_all_sections(self):
        return self.sections

    def _get_section_items(self, section):
        return self.sections[section]

    def _find_sections(self, credentials):
        creds = credentials['repoze.what.userid'].split(':')
        if len(creds) != 3:
            return set()
        rv = set([creds[0], ':'.join(creds[:2]), creds[2]])
        return rv

    def _include_items(self, section, items):
        self.sections[section] |= items

    def _exclude_items(self, section, items):
        for item in items:
            self.sections[section].remove(item)

    def _item_is_included(self, section, item):
        return item in self.sections[section]

    def _create_section(self, section):
        self.sections[section] = set()

    def _edit_section(self, section, new_section):
        self.sections[new_section] = self.sections[section]
        del self.sections[section]

    def _delete_section(self, section):
        del self.sections[section]

    def _section_exists(self, section):
        return self.sections.has_key(section)


class DevPermissionSourceAdapter(BaseSourceAdapter):

    def __init__(self, *args, **kwargs):
        super(DevPermissionSourceAdapter, self).__init__(*args, **kwargs)
        self.sections = {}

    def _get_all_sections(self):
        return self.sections

    def _get_section_items(self, section):
        return self.sections[section]

    def _find_sections(self, group_name):
        return set([n for (n, p) in self.sections.items()
                    if group_name in p])

    def _include_items(self, section, items):
        self.sections[section] |= items

    def _exclude_items(self, section, items):
        for item in items:
            self.sections[section].remove(item)

    def _item_is_included(self, section, item):
        return item in self.sections[section]

    def _create_section(self, section):
        self.sections[section] = set()

    def _edit_section(self, section, new_section):
        self.sections[new_section] = self.sections[section]
        del self.sections[section]

    def _delete_section(self, section):
        del self.sections[section]

    def _section_exists(self, section):
        return self.sections.has_key(section)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    def auth_filter(app):
        return setup_auth(DevAuthorization(app, conf),
            group_adapters={'all_groups': DevGroupSourceAdapter()},
            permission_adapters={'all_perms': DevPermissionSourceAdapter()},
            identifiers=[('devauth', DevIdentifier(conf))],
            authenticators=[('devauth', DevAuthenticator(conf))],
            challengers=[('devauth', DevChallenger(conf))])
    return auth_filter
