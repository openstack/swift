# Copyright (c) 2010-2011 OpenStack, LLC.
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

from swift.common.utils import get_logger


class StatsLogProcessor(object):
    """Transform account storage stat logs"""

    def __init__(self, conf):
        self.logger = get_logger(conf)

    def process(self, obj_stream, data_object_account, data_object_container,
                data_object_name):
        '''generate hourly groupings of data from one stats log file'''
        account_totals = {}
        year, month, day, hour, _junk = data_object_name.split('/')
        for line in obj_stream:
            if not line:
                continue
            try:
                (account,
                container_count,
                object_count,
                bytes_used) = line.split(',')
            except (IndexError, ValueError):
                # bad line data
                self.logger.debug(_('Bad line data: %s') % repr(line))
                continue
            account = account.strip('"')
            container_count = int(container_count.strip('"'))
            object_count = int(object_count.strip('"'))
            bytes_used = int(bytes_used.strip('"'))
            aggr_key = (account, year, month, day, hour)
            d = account_totals.get(aggr_key, {})
            d['replica_count'] = d.setdefault('replica_count', 0) + 1
            d['container_count'] = d.setdefault('container_count', 0) + \
                                   container_count
            d['object_count'] = d.setdefault('object_count', 0) + \
                                object_count
            d['bytes_used'] = d.setdefault('bytes_used', 0) + \
                              bytes_used
            account_totals[aggr_key] = d
        return account_totals

    def keylist_mapping(self):
        '''
        returns a dictionary of final keys mapped to source keys
        '''
        keylist_mapping = {
        #   <db key> : <row key> or <set of row keys>
            'bytes_used': 'bytes_used',
            'container_count': 'container_count',
            'object_count': 'object_count',
            'replica_count': 'replica_count',
        }
        return keylist_mapping
