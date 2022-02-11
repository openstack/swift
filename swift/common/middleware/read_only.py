# Copyright (c) 2010-2015 OpenStack Foundation
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
from swift.common.constraints import check_account_format, valid_api_version
from swift.common.swob import HTTPMethodNotAllowed, Request
from swift.common.utils import get_logger, config_true_value
from swift.common.registry import register_swift_info
from swift.proxy.controllers.base import get_info

"""
=========
Read Only
=========

The ability to make an entire cluster or individual accounts read only is
implemented as pluggable middleware.  When a cluster or an account is in read
only mode, requests that would result in writes to the cluser are not allowed.
A 405 is returned on such requests.  "COPY", "DELETE", "POST", and
"PUT" are the HTTP methods that are considered writes.

-------------
Configuration
-------------

All configuration is optional.

============= ======= ====================================================
Option        Default Description
------------- ------- ----------------------------------------------------
read_only     false   Set to 'true' to put the entire cluster in read only
                      mode.
allow_deletes false   Set to 'true' to allow deletes.
============= ======= ====================================================

---------------------------
Marking Individual Accounts
---------------------------

If a system administrator wants to mark individual accounts as read only,
he/she can set X-Account-Sysmeta-Read-Only on an account to 'true'.

If a system administrator wants to allow writes to individual accounts,
when a cluster is in read only mode, he/she can set
X-Account-Sysmeta-Read-Only on an account to 'false'.

This header will be hidden from the user, because of the gatekeeper middleware,
and can only be set using a direct client to the account nodes.
"""


class ReadOnlyMiddleware(object):
    """
    Middleware that make an entire cluster or individual accounts read only.
    """

    def __init__(self, app, conf, logger=None):
        self.app = app
        self.logger = logger or get_logger(conf, log_route='read_only')
        self.read_only = config_true_value(conf.get('read_only'))
        self.write_methods = {'COPY', 'POST', 'PUT'}
        if not config_true_value(conf.get('allow_deletes')):
            self.write_methods.add('DELETE')

    def __call__(self, env, start_response):
        req = Request(env)

        if req.method not in self.write_methods:
            return self.app(env, start_response)

        try:
            version, account, container, obj = req.split_path(2, 4, True)
            if not valid_api_version(version):
                raise ValueError
        except ValueError:
            return self.app(env, start_response)

        if req.method == 'COPY' and 'Destination-Account' in req.headers:
            dest_account = req.headers.get('Destination-Account')
            account = check_account_format(req, dest_account)

        if self.account_read_only(req, account):
            msg = 'Writes are disabled for this account.'
            return HTTPMethodNotAllowed(body=msg)(env, start_response)

        return self.app(env, start_response)

    def account_read_only(self, req, account):
        """
        Check whether an account should be read-only.

        This considers both the cluster-wide config value as well as the
        per-account override in X-Account-Sysmeta-Read-Only.
        """
        info = get_info(self.app, req.environ, account, swift_source='RO')
        read_only = info.get('sysmeta', {}).get('read-only', '')
        if not read_only:
            return self.read_only
        return config_true_value(read_only)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    if config_true_value(conf.get('read_only')):
        register_swift_info('read_only')

    def read_only_filter(app):
        return ReadOnlyMiddleware(app, conf)

    return read_only_filter
