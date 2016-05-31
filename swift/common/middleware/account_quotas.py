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

"""
``account_quotas`` is a middleware which blocks write requests (PUT, POST) if a
given account quota (in bytes) is exceeded while DELETE requests are still
allowed.

``account_quotas`` uses the ``x-account-meta-quota-bytes`` metadata entry to
store the quota. Write requests to this metadata entry are only permitted for
resellers. There is no quota limit if ``x-account-meta-quota-bytes`` is not
set.

The ``account_quotas`` middleware should be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file just after any auth middleware.
For example::

    [pipeline:main]
    pipeline = catch_errors cache tempauth account_quotas proxy-server

    [filter:account_quotas]
    use = egg:swift#account_quotas

To set the quota on an account::

    swift -A http://127.0.0.1:8080/auth/v1.0 -U account:reseller -K secret \
post -m quota-bytes:10000

Remove the quota::

    swift -A http://127.0.0.1:8080/auth/v1.0 -U account:reseller -K secret \
post -m quota-bytes:

The same limitations apply for the account quotas as for the container quotas.

For example, when uploading an object without a content-length header the proxy
server doesn't know the final size of the currently uploaded object and the
upload will be allowed if the current account size is within the quota.
Due to the eventual consistency further uploads might be possible until the
account size has been updated.
"""

from swift.common.swob import HTTPForbidden, HTTPBadRequest, \
    HTTPRequestEntityTooLarge, wsgify
from swift.common.utils import register_swift_info
from swift.proxy.controllers.base import get_account_info


class AccountQuotaMiddleware(object):
    """Account quota middleware

    See above for a full description.

    """
    def __init__(self, app, *args, **kwargs):
        self.app = app

    @wsgify
    def __call__(self, request):

        if request.method not in ("POST", "PUT"):
            return self.app

        try:
            ver, account, container, obj = request.split_path(
                2, 4, rest_with_last=True)
        except ValueError:
            return self.app

        if not container:
            # account request, so we pay attention to the quotas
            new_quota = request.headers.get(
                'X-Account-Meta-Quota-Bytes')
            remove_quota = request.headers.get(
                'X-Remove-Account-Meta-Quota-Bytes')
        else:
            # container or object request; even if the quota headers are set
            # in the request, they're meaningless
            new_quota = remove_quota = None

        if remove_quota:
            new_quota = 0    # X-Remove dominates if both are present

        if request.environ.get('reseller_request') is True:
            if new_quota and not new_quota.isdigit():
                return HTTPBadRequest()
            return self.app

        # deny quota set for non-reseller
        if new_quota is not None:
            return HTTPForbidden()

        if request.method == "POST" or not obj:
            return self.app

        content_length = (request.content_length or 0)

        account_info = get_account_info(request.environ, self.app)
        if not account_info or not account_info['bytes']:
            return self.app
        try:
            quota = int(account_info['meta'].get('quota-bytes', -1))
        except ValueError:
            return self.app
        if quota < 0:
            return self.app

        new_size = int(account_info['bytes']) + content_length
        if quota < new_size:
            resp = HTTPRequestEntityTooLarge(body='Upload exceeds quota.')
            if 'swift.authorize' in request.environ:
                orig_authorize = request.environ['swift.authorize']

                def reject_authorize(*args, **kwargs):
                    aresp = orig_authorize(*args, **kwargs)
                    if aresp:
                        return aresp
                    return resp
                request.environ['swift.authorize'] = reject_authorize
            else:
                return resp

        return self.app


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    register_swift_info('account_quotas')

    def account_quota_filter(app):
        return AccountQuotaMiddleware(app)
    return account_quota_filter
