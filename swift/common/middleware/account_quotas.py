# Copyright (c) 2013 OpenStack Foundation.
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

""" Account quota middleware for Openstack Swift Proxy """

from swift.common.swob import HTTPForbidden, HTTPRequestEntityTooLarge, \
    HTTPBadRequest, wsgify

from swift.proxy.controllers.base import get_account_info


class AccountQuotaMiddleware(object):
    """
    account_quotas is a middleware which blocks write requests (PUT, POST) if a
    given quota (in bytes) is exceeded while DELETE requests are still allowed.

    account_quotas uses the x-account-meta-quota-bytes metadata to store the
    quota. Write requests to this metadata setting are only allowed for
    resellers. There is no quota limit if x-account-meta-quota-bytes is not
    set.

    The following shows an example proxy-server.conf:

    [pipeline:main]
    pipeline = catch_errors cache tempauth account-quotas proxy-server

    [filter:account-quotas]
    use = egg:swift#account_quotas

    """

    def __init__(self, app, *args, **kwargs):
        self.app = app

    @wsgify
    def __call__(self, request):

        if request.method not in ("POST", "PUT"):
            return self.app

        try:
            request.split_path(2, 4, rest_with_last=True)
        except ValueError:
            return self.app

        new_quota = request.headers.get('X-Account-Meta-Quota-Bytes')

        if request.environ.get('reseller_request') is True:
            if new_quota and not new_quota.isdigit():
                return HTTPBadRequest()
            return self.app

        # deny quota set for non-reseller
        if new_quota is not None:
            return HTTPForbidden()

        account_info = get_account_info(request.environ, self.app)
        new_size = int(account_info['bytes']) + (request.content_length or 0)
        quota = int(account_info['meta'].get('quota-bytes', -1))

        if 0 <= quota < new_size:
            return HTTPRequestEntityTooLarge()

        return self.app


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    def account_quota_filter(app):
        return AccountQuotaMiddleware(app)
    return account_quota_filter
