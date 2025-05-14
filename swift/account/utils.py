# Copyright (c) 2010-2013 OpenStack Foundation
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

import json

from swift.common import constraints
from swift.common.middleware import listing_formats
from swift.common.swob import HTTPOk, HTTPNoContent, str_to_wsgi
from swift.common.utils import Timestamp
from swift.common.storage_policy import POLICIES


class FakeAccountBroker(object):
    """
    Quacks like an account broker, but doesn't actually do anything. Responds
    like an account broker would for a real, empty account with no metadata.
    """
    def get_info(self):
        now = Timestamp.now().internal
        return {'container_count': 0,
                'object_count': 0,
                'bytes_used': 0,
                'created_at': now,
                'put_timestamp': now}

    def list_containers_iter(self, *_, **__):
        return []

    @property
    def metadata(self):
        return {}

    def get_policy_stats(self):
        return {}


def get_response_headers(broker):
    info = broker.get_info()
    resp_headers = {
        'X-Account-Container-Count': info['container_count'],
        'X-Account-Object-Count': info['object_count'],
        'X-Account-Bytes-Used': info['bytes_used'],
        'X-Timestamp': Timestamp(info['created_at']).normal,
        'X-PUT-Timestamp': Timestamp(info['put_timestamp']).normal}
    policy_stats = broker.get_policy_stats()
    for policy_idx, stats in policy_stats.items():
        policy = POLICIES.get_by_index(policy_idx)
        if not policy:
            continue
        header_prefix = 'X-Account-Storage-Policy-%s-%%s' % policy.name
        for key, value in stats.items():
            header_name = header_prefix % key.replace('_', '-')
            resp_headers[header_name] = value
    resp_headers.update((str_to_wsgi(key), str_to_wsgi(value))
                        for key, (value, _timestamp) in
                        broker.metadata.items() if value != '')
    return resp_headers


def account_listing_response(account, req, response_content_type, broker=None,
                             limit=constraints.ACCOUNT_LISTING_LIMIT,
                             marker='', end_marker='', prefix='', delimiter='',
                             reverse=False):
    if broker is None:
        broker = FakeAccountBroker()

    resp_headers = get_response_headers(broker)

    account_list = broker.list_containers_iter(limit, marker, end_marker,
                                               prefix, delimiter, reverse,
                                               req.allow_reserved_names)
    data = []
    for (name, object_count, bytes_used, put_timestamp,
         storage_policy_index, is_subdir) \
            in account_list:
        if is_subdir:
            data.append({'subdir': name})
        else:
            container = {
                'name': name,
                'count': object_count,
                'bytes': bytes_used,
                'last_modified': Timestamp(put_timestamp).isoformat}
            # Add the container's storage policy to the response, unless
            # storage_policy_index was not found in POLICIES, which means
            # the storage policy is missing from the Swift configuration
            # or otherwise could not be determined.
            #
            # The storage policy should always be returned when
            # everything is configured correctly, but clients are
            # expected to be able to handle this case regardless,
            # if only to support older versions of swift.
            if storage_policy_index in POLICIES:
                container['storage_policy'] = (
                    POLICIES[storage_policy_index].name
                )
            data.append(container)
    if response_content_type.endswith('/xml'):
        account_list = listing_formats.account_to_xml(data, account)
        ret = HTTPOk(body=account_list, request=req, headers=resp_headers)
    elif response_content_type.endswith('/json'):
        account_list = json.dumps(data).encode('ascii')
        ret = HTTPOk(body=account_list, request=req, headers=resp_headers)
    elif data:
        account_list = listing_formats.listing_to_text(data)
        ret = HTTPOk(body=account_list, request=req, headers=resp_headers)
    else:
        ret = HTTPNoContent(request=req, headers=resp_headers)
    ret.content_type = response_content_type
    ret.charset = 'utf-8'
    return ret
