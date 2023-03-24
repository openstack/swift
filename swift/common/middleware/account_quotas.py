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
store the overall account quota. Write requests to this metadata entry are
only permitted for resellers. There is no overall account quota limit if
``x-account-meta-quota-bytes`` is not set.

Additionally, account quotas may be set for each storage policy, using metadata
of the form ``x-account-quota-bytes-policy-<policy name>``. Again, only
resellers may update these metadata, and there will be no limit for a
particular policy if the corresponding metadata is not set.

.. note::
   Per-policy quotas need not sum to the overall account quota, and the sum of
   all :ref:`container_quotas` for a given policy need not sum to the account's
   policy quota.

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
from swift.common.registry import register_swift_info
from swift.common.storage_policy import POLICIES
from swift.proxy.controllers.base import get_account_info, get_container_info


class AccountQuotaMiddleware(object):
    """Account quota middleware

    See above for a full description.

    """
    def __init__(self, app, *args, **kwargs):
        self.app = app

    def handle_account(self, request):
        if request.method in ("POST", "PUT"):
            # account request, so we pay attention to the quotas
            new_quotas = {}
            new_quotas[None] = request.headers.get(
                'X-Account-Meta-Quota-Bytes')
            if request.headers.get(
                    'X-Remove-Account-Meta-Quota-Bytes'):
                new_quotas[None] = 0  # X-Remove dominates if both are present

            for policy in POLICIES:
                tail = 'Account-Quota-Bytes-Policy-%s' % policy.name
                if request.headers.get('X-Remove-' + tail):
                    new_quotas[policy.idx] = 0
                else:
                    quota = request.headers.pop('X-' + tail, None)
                    new_quotas[policy.idx] = quota

            if request.environ.get('reseller_request') is True:
                if any(quota and not quota.isdigit()
                       for quota in new_quotas.values()):
                    return HTTPBadRequest()
                for idx, quota in new_quotas.items():
                    if idx is None:
                        continue  # For legacy reasons, it's in user meta
                    hdr = 'X-Account-Sysmeta-Quota-Bytes-Policy-%d' % idx
                    request.headers[hdr] = quota
            elif any(quota is not None for quota in new_quotas.values()):
                # deny quota set for non-reseller
                return HTTPForbidden()

        resp = request.get_response(self.app)
        # Non-resellers can't update quotas, but they *can* see them
        for policy in POLICIES:
            infix = 'Quota-Bytes-Policy'
            value = resp.headers.get('X-Account-Sysmeta-%s-%d' % (
                infix, policy.idx))
            if value:
                resp.headers['X-Account-%s-%s' % (infix, policy.name)] = value
        return resp

    @wsgify
    def __call__(self, request):

        try:
            ver, account, container, obj = request.split_path(
                2, 4, rest_with_last=True)
        except ValueError:
            return self.app

        if not container:
            return self.handle_account(request)
        # container or object request; even if the quota headers are set
        # in the request, they're meaningless

        if not (request.method == "PUT" and obj):
            return self.app
        # OK, object PUT

        if request.environ.get('reseller_request') is True:
            # but resellers aren't constrained by quotas :-)
            return self.app

        # Object PUT request
        content_length = (request.content_length or 0)

        account_info = get_account_info(request.environ, self.app,
                                        swift_source='AQ')
        if not account_info:
            return self.app
        try:
            quota = int(account_info['meta'].get('quota-bytes', -1))
        except ValueError:
            quota = -1
        if quota >= 0:
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

        container_info = get_container_info(request.environ, self.app,
                                            swift_source='AQ')
        if not container_info:
            return self.app
        policy_idx = container_info['storage_policy']
        sysmeta_key = 'quota-bytes-policy-%s' % policy_idx
        try:
            policy_quota = int(account_info['sysmeta'].get(sysmeta_key, -1))
        except ValueError:
            policy_quota = -1
        if policy_quota >= 0:
            policy_stats = account_info['storage_policies'].get(policy_idx, {})
            new_size = int(policy_stats.get('bytes', 0)) + content_length
            if policy_quota < new_size:
                resp = HTTPRequestEntityTooLarge(
                    body='Upload exceeds policy quota.')
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
