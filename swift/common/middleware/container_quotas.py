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
The ``container_quotas`` middleware implements simple quotas that can be
imposed on swift containers by a user with the ability to set container
metadata, most likely the account administrator.  This can be useful for
limiting the scope of containers that are delegated to non-admin users, exposed
to ``formpost`` uploads, or just as a self-imposed sanity check.

Any object PUT operations that exceed these quotas return a 413 response
(request entity too large) with a descriptive body.

Quotas are subject to several limitations: eventual consistency, the timeliness
of the cached container_info (60 second ttl by default), and it's unable to
reject chunked transfer uploads that exceed the quota (though once the quota
is exceeded, new chunked transfers will be refused).

Quotas are set by adding meta values to the container, and are validated when
set:

+---------------------------------------------+-------------------------------+
|Metadata                                     | Use                           |
+=============================================+===============================+
| X-Container-Meta-Quota-Bytes                | Maximum size of the           |
|                                             | container, in bytes.          |
+---------------------------------------------+-------------------------------+
| X-Container-Meta-Quota-Count                | Maximum object count of the   |
|                                             | container.                    |
+---------------------------------------------+-------------------------------+

The ``container_quotas`` middleware should be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file just after any auth middleware.
For example::

    [pipeline:main]
    pipeline = catch_errors cache tempauth container_quotas proxy-server

    [filter:container_quotas]
    use = egg:swift#container_quotas
"""
from swift.common.http import is_success
from swift.common.swob import HTTPRequestEntityTooLarge, HTTPBadRequest, \
    wsgify
from swift.common.utils import register_swift_info
from swift.proxy.controllers.base import get_container_info


class ContainerQuotaMiddleware(object):
    def __init__(self, app, *args, **kwargs):
        self.app = app

    def bad_response(self, req, container_info):
        # 401 if the user couldn't have PUT this object in the first place.
        # This prevents leaking the container's existence to unauthed users.
        if 'swift.authorize' in req.environ:
            req.acl = container_info['write_acl']
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        return HTTPRequestEntityTooLarge(body='Upload exceeds quota.')

    @wsgify
    def __call__(self, req):
        try:
            (version, account, container, obj) = req.split_path(3, 4, True)
        except ValueError:
            return self.app

        # verify new quota headers are properly formatted
        if not obj and req.method in ('PUT', 'POST'):
            val = req.headers.get('X-Container-Meta-Quota-Bytes')
            if val and not val.isdigit():
                return HTTPBadRequest(body='Invalid bytes quota.')
            val = req.headers.get('X-Container-Meta-Quota-Count')
            if val and not val.isdigit():
                return HTTPBadRequest(body='Invalid count quota.')

        # check user uploads against quotas
        elif obj and req.method in ('PUT'):
            container_info = get_container_info(
                req.environ, self.app, swift_source='CQ')
            if not container_info or not is_success(container_info['status']):
                # this will hopefully 404 later
                return self.app

            if 'quota-bytes' in container_info.get('meta', {}) and \
                    'bytes' in container_info and \
                    container_info['meta']['quota-bytes'].isdigit():
                content_length = (req.content_length or 0)
                new_size = int(container_info['bytes']) + content_length
                if int(container_info['meta']['quota-bytes']) < new_size:
                    return self.bad_response(req, container_info)

            if 'quota-count' in container_info.get('meta', {}) and \
                    'object_count' in container_info and \
                    container_info['meta']['quota-count'].isdigit():
                new_count = int(container_info['object_count']) + 1
                if int(container_info['meta']['quota-count']) < new_count:
                    return self.bad_response(req, container_info)

        return self.app


def filter_factory(global_conf, **local_conf):
    register_swift_info('container_quotas')

    def container_quota_filter(app):
        return ContainerQuotaMiddleware(app)
    return container_quota_filter
