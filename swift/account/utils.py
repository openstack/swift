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

import time
from xml.sax import saxutils

from swift.common.swob import HTTPOk, HTTPNoContent
from swift.common.utils import json, Timestamp
from swift.common.storage_policy import POLICIES


class FakeAccountBroker(object):
    """
    Quacks like an account broker, but doesn't actually do anything. Responds
    like an account broker would for a real, empty account with no metadata.
    """
    def get_info(self):
        now = Timestamp(time.time()).internal
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
    resp_headers.update((key, value)
                        for key, (value, timestamp) in
                        broker.metadata.iteritems() if value != '')
    return resp_headers


def account_listing_response(account, req, response_content_type, broker=None,
                             limit='', marker='', end_marker='', prefix='',
                             delimiter=''):
    if broker is None:
        broker = FakeAccountBroker()

    resp_headers = get_response_headers(broker)

    account_list = broker.list_containers_iter(limit, marker, end_marker,
                                               prefix, delimiter)
    if response_content_type == 'application/json':
        data = []
        for (name, object_count, bytes_used, is_subdir) in account_list:
            if is_subdir:
                data.append({'subdir': name})
            else:
                data.append({'name': name, 'count': object_count,
                             'bytes': bytes_used})
        account_list = json.dumps(data)
    elif response_content_type.endswith('/xml'):
        output_list = ['<?xml version="1.0" encoding="UTF-8"?>',
                       '<account name=%s>' % saxutils.quoteattr(account)]
        for (name, object_count, bytes_used, is_subdir) in account_list:
            if is_subdir:
                output_list.append(
                    '<subdir name=%s />' % saxutils.quoteattr(name))
            else:
                item = '<container><name>%s</name><count>%s</count>' \
                       '<bytes>%s</bytes></container>' % \
                       (saxutils.escape(name), object_count, bytes_used)
                output_list.append(item)
        output_list.append('</account>')
        account_list = '\n'.join(output_list)
    else:
        if not account_list:
            resp = HTTPNoContent(request=req, headers=resp_headers)
            resp.content_type = response_content_type
            resp.charset = 'utf-8'
            return resp
        account_list = '\n'.join(r[0] for r in account_list) + '\n'
    ret = HTTPOk(body=account_list, request=req, headers=resp_headers)
    ret.content_type = response_content_type
    ret.charset = 'utf-8'
    return ret
