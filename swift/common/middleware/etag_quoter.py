# Copyright (c) 2010-2020 OpenStack Foundation
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
This middleware fix the Etag header of responses so that it is RFC compliant.
`RFC 7232 <https://tools.ietf.org/html/rfc7232#section-2.3>`__ specifies that
the value of the Etag header must be double quoted.

It must be placed at the beggining of the pipeline, right after cache::

   [pipeline:main]
   pipeline = ...  cache etag-quoter ...

   [filter:etag-quoter]
   use = egg:swift#etag_quoter

Set ``X-Account-Rfc-Compliant-Etags: true`` at the account
level to have any Etags in object responses be double quoted, as in
``"d41d8cd98f00b204e9800998ecf8427e"``. Alternatively, you may
only fix Etags in a single container by setting
``X-Container-Rfc-Compliant-Etags: true`` on the container.
This may be necessary for Swift to work properly with some CDNs.

Either option may also be explicitly *disabled*, so you may enable quoted
Etags account-wide as above but turn them off for individual containers
with ``X-Container-Rfc-Compliant-Etags: false``. This may be
useful if some subset of applications expect Etags to be bare MD5s.
"""

from swift.common.constraints import valid_api_version
from swift.common.http import is_success
from swift.common.swob import Request
from swift.common.utils import config_true_value
from swift.common.registry import register_swift_info
from swift.proxy.controllers.base import get_account_info, get_container_info


class EtagQuoterMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            version, account, container, obj = req.split_path(
                2, 4, rest_with_last=True)
            is_swifty_request = valid_api_version(version)
        except ValueError:
            is_swifty_request = False

        if not is_swifty_request:
            return self.app(env, start_response)

        if not obj:
            typ = 'Container' if container else 'Account'
            client_header = 'X-%s-Rfc-Compliant-Etags' % typ
            sysmeta_header = 'X-%s-Sysmeta-Rfc-Compliant-Etags' % typ
            if client_header in req.headers:
                if req.headers[client_header]:
                    req.headers[sysmeta_header] = config_true_value(
                        req.headers[client_header])
                else:
                    req.headers[sysmeta_header] = ''
            if req.headers.get(client_header.replace('X-', 'X-Remove-', 1)):
                req.headers[sysmeta_header] = ''

            def translating_start_response(status, headers, exc_info=None):
                return start_response(status, [
                    (client_header if h.title() == sysmeta_header else h,
                     v) for h, v in headers
                ], exc_info)

            return self.app(env, translating_start_response)

        container_info = get_container_info(env, self.app, 'EQ')
        if not container_info or not is_success(container_info['status']):
            return self.app(env, start_response)

        flag = container_info.get('sysmeta', {}).get('rfc-compliant-etags')
        if flag is None:
            account_info = get_account_info(env, self.app, 'EQ')
            if not account_info or not is_success(account_info['status']):
                return self.app(env, start_response)

            flag = account_info.get('sysmeta', {}).get(
                'rfc-compliant-etags')

        if flag is None:
            flag = self.conf.get('enable_by_default', 'false')

        if not config_true_value(flag):
            return self.app(env, start_response)

        status, headers, resp_iter = req.call_application(self.app)

        headers = [
            (header, value) if header.lower() != 'etag' or (
                value.startswith(('"', 'W/"')) and value.endswith('"'))
            else (header, '"%s"' % value)
            for header, value in headers]

        start_response(status, headers)
        return resp_iter


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    register_swift_info(
        'etag_quoter', enable_by_default=config_true_value(
            conf.get('enable_by_default', 'false')))

    def etag_quoter_filter(app):
        return EtagQuoterMiddleware(app, conf)
    return etag_quoter_filter
