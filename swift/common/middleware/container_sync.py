# Copyright (c) 2013 OpenStack Foundation
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

from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.swob import HTTPBadRequest, HTTPUnauthorized, wsgify
from swift.common.utils import (
    config_true_value, get_logger, register_swift_info, streq_const_time)
from swift.proxy.controllers.base import get_container_info


class ContainerSync(object):
    """
    WSGI middleware that validates an incoming container sync request
    using the container-sync-realms.conf style of container sync.
    """

    def __init__(self, app, conf, logger=None):
        self.app = app
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='container_sync')
        self.realms_conf = ContainerSyncRealms(
            os.path.join(
                conf.get('swift_dir', '/etc/swift'),
                'container-sync-realms.conf'),
            self.logger)
        self.allow_full_urls = config_true_value(
            conf.get('allow_full_urls', 'true'))
        # configure current realm/cluster for /info
        self.realm = self.cluster = None
        current = conf.get('current', None)
        if current:
            try:
                self.realm, self.cluster = (p.upper() for p in
                                            current.strip('/').split('/'))
            except ValueError:
                self.logger.error('Invalid current //REALM/CLUSTER (%s)',
                                  current)
        self.register_info()

    def register_info(self):
        dct = {}
        for realm in self.realms_conf.realms():
            clusters = self.realms_conf.clusters(realm)
            if clusters:
                dct[realm] = {'clusters': dict((c, {}) for c in clusters)}
        if self.realm and self.cluster:
            try:
                dct[self.realm]['clusters'][self.cluster]['current'] = True
            except KeyError:
                self.logger.error('Unknown current //REALM/CLUSTER (%s)',
                                  '//%s/%s' % (self.realm, self.cluster))
        register_swift_info('container_sync', realms=dct)

    @wsgify
    def __call__(self, req):
        if not self.allow_full_urls:
            sync_to = req.headers.get('x-container-sync-to')
            if sync_to and not sync_to.startswith('//'):
                raise HTTPBadRequest(
                    body='Full URLs are not allowed for X-Container-Sync-To '
                         'values. Only realm values of the format '
                         '//realm/cluster/account/container are allowed.\n',
                    request=req)
        auth = req.headers.get('x-container-sync-auth')
        if auth:
            valid = False
            auth = auth.split()
            if len(auth) != 3:
                req.environ.setdefault('swift.log_info', []).append(
                    'cs:not-3-args')
            else:
                realm, nonce, sig = auth
                realm_key = self.realms_conf.key(realm)
                realm_key2 = self.realms_conf.key2(realm)
                if not realm_key:
                    req.environ.setdefault('swift.log_info', []).append(
                        'cs:no-local-realm-key')
                else:
                    info = get_container_info(
                        req.environ, self.app, swift_source='CS')
                    user_key = info.get('sync_key')
                    if not user_key:
                        req.environ.setdefault('swift.log_info', []).append(
                            'cs:no-local-user-key')
                    else:
                        # x-timestamp headers get shunted by gatekeeper
                        if 'x-backend-inbound-x-timestamp' in req.headers:
                            req.headers['x-timestamp'] = req.headers.pop(
                                'x-backend-inbound-x-timestamp')

                        expected = self.realms_conf.get_sig(
                            req.method, req.path,
                            req.headers.get('x-timestamp', '0'), nonce,
                            realm_key, user_key)
                        expected2 = self.realms_conf.get_sig(
                            req.method, req.path,
                            req.headers.get('x-timestamp', '0'), nonce,
                            realm_key2, user_key) if realm_key2 else expected
                        if not streq_const_time(sig, expected) and \
                                not streq_const_time(sig, expected2):
                            req.environ.setdefault(
                                'swift.log_info', []).append('cs:invalid-sig')
                        else:
                            req.environ.setdefault(
                                'swift.log_info', []).append('cs:valid')
                            valid = True
            if not valid:
                exc = HTTPUnauthorized(
                    body='X-Container-Sync-Auth header not valid; '
                         'contact cluster operator for support.',
                    headers={'content-type': 'text/plain'},
                    request=req)
                exc.headers['www-authenticate'] = ' '.join([
                    'SwiftContainerSync',
                    exc.www_authenticate().split(None, 1)[1]])
                raise exc
            else:
                req.environ['swift.authorize_override'] = True
                # An SLO manifest will already be in the internal manifest
                # syntax and might be synced before its segments, so stop SLO
                # middleware from performing the usual manifest validation.
                req.environ['swift.slo_override'] = True

        if req.path == '/info':
            # Ensure /info requests get the freshest results
            self.register_info()
        return self.app


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    register_swift_info('container_sync')

    def cache_filter(app):
        return ContainerSync(app, conf)

    return cache_filter
