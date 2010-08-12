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

class StatsLogProcessor(object):

    def __init__(self, conf):
        pass

    def process(self, obj_stream):
        '''generate hourly groupings of data from one stats log file'''
        account_totals = {}
        year, month, day, hour, _ = item.split('/')
        for line in obj_stream:
            if not line:
                continue
            try:
                (account,
                container_count,
                object_count,
                bytes_used,
                created_at) = line.split(',')
                account = account.strip('"')
                if account_name and account_name != account:
                    continue
                container_count = int(container_count.strip('"'))
                object_count = int(object_count.strip('"'))
                bytes_used = int(bytes_used.strip('"'))
                aggr_key = account
                aggr_key = (account, year, month, day, hour)
                d = account_totals.get(aggr_key, {})
                d['count'] = d.setdefault('count', 0) + 1
                d['container_count'] = d.setdefault('container_count', 0) + \
                                       container_count
                d['object_count'] = d.setdefault('object_count', 0) + \
                                    object_count
                d['bytes_used'] = d.setdefault('bytes_used', 0) + \
                                  bytes_used
                d['created_at'] = created_at
                account_totals[aggr_key] = d
            except (IndexError, ValueError):
                # bad line data
                pass
        return account_totals, item
