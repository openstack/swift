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

import collections
from urllib import unquote
import copy

from swift.common.utils import split_path, get_logger

month_map = '_ Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()


class AccessLogProcessor(object):
    """Transform proxy server access logs"""

    def __init__(self, conf):
        self.server_name = conf.get('server_name', 'proxy-server')
        self.lb_private_ips = [x.strip() for x in \
                               conf.get('lb_private_ips', '').split(',')\
                               if x.strip()]
        self.service_ips = [x.strip() for x in \
                            conf.get('service_ips', '').split(',')\
                            if x.strip()]
        self.warn_percent = float(conf.get('warn_percent', '0.8'))
        self.logger = get_logger(conf)

    def log_line_parser(self, raw_log):
        '''given a raw access log line, return a dict of the good parts'''
        d = {}
        try:
            (unused,
            server,
            client_ip,
            lb_ip,
            timestamp,
            method,
            request,
            http_version,
            code,
            referrer,
            user_agent,
            auth_token,
            bytes_in,
            bytes_out,
            etag,
            trans_id,
            headers,
            processing_time) = (unquote(x) for x in
                                raw_log[16:].split(' ')[:18])
        except ValueError:
            self.logger.debug(_('Bad line data: %s') % repr(raw_log))
            return {}
        if server != self.server_name:
            # incorrect server name in log line
            self.logger.debug(_('Bad server name: found "%(found)s" ' \
                    'expected "%(expected)s"') %
                    {'found': server, 'expected': self.server_name})
            return {}
        try:
            (version, account, container_name, object_name) = \
                split_path(request, 2, 4, True)
        except ValueError, e:
            self.logger.debug(_('Invalid path: %(error)s from data: %(log)s') %
            {'error': e, 'log': repr(raw_log)})
            return {}
        if container_name is not None:
            container_name = container_name.split('?', 1)[0]
        if object_name is not None:
            object_name = object_name.split('?', 1)[0]
        account = account.split('?', 1)[0]
        query = None
        if '?' in request:
            request, query = request.split('?', 1)
            args = query.split('&')
            # Count each query argument. This is used later to aggregate
            # the number of format, prefix, etc. queries.
            for q in args:
                if '=' in q:
                    k, v = q.split('=', 1)
                else:
                    k = q
                # Certain keys will get summmed in stats reporting
                # (format, path, delimiter, etc.). Save a "1" here
                # to indicate that this request is 1 request for
                # its respective key.
                d[k] = 1
        d['client_ip'] = client_ip
        d['lb_ip'] = lb_ip
        d['method'] = method
        d['request'] = request
        if query:
            d['query'] = query
        d['http_version'] = http_version
        d['code'] = code
        d['referrer'] = referrer
        d['user_agent'] = user_agent
        d['auth_token'] = auth_token
        d['bytes_in'] = bytes_in
        d['bytes_out'] = bytes_out
        d['etag'] = etag
        d['trans_id'] = trans_id
        d['processing_time'] = processing_time
        day, month, year, hour, minute, second = timestamp.split('/')
        d['day'] = day
        month = ('%02s' % month_map.index(month)).replace(' ', '0')
        d['month'] = month
        d['year'] = year
        d['hour'] = hour
        d['minute'] = minute
        d['second'] = second
        d['tz'] = '+0000'
        d['account'] = account
        d['container_name'] = container_name
        d['object_name'] = object_name
        d['bytes_out'] = int(d['bytes_out'].replace('-', '0'))
        d['bytes_in'] = int(d['bytes_in'].replace('-', '0'))
        d['code'] = int(d['code'])
        return d

    def process(self, obj_stream, data_object_account, data_object_container,
                data_object_name):
        '''generate hourly groupings of data from one access log file'''
        hourly_aggr_info = {}
        total_lines = 0
        bad_lines = 0
        for line in obj_stream:
            line_data = self.log_line_parser(line)
            total_lines += 1
            if not line_data:
                bad_lines += 1
                continue
            account = line_data['account']
            container_name = line_data['container_name']
            year = line_data['year']
            month = line_data['month']
            day = line_data['day']
            hour = line_data['hour']
            bytes_out = line_data['bytes_out']
            bytes_in = line_data['bytes_in']
            method = line_data['method']
            code = int(line_data['code'])
            object_name = line_data['object_name']
            client_ip = line_data['client_ip']

            op_level = None
            if not container_name:
                op_level = 'account'
            elif container_name and not object_name:
                op_level = 'container'
            elif object_name:
                op_level = 'object'

            aggr_key = (account, year, month, day, hour)
            d = hourly_aggr_info.get(aggr_key, {})
            if line_data['lb_ip'] in self.lb_private_ips:
                source = 'service'
            else:
                source = 'public'

            if line_data['client_ip'] in self.service_ips:
                source = 'service'

            d[(source, 'bytes_out')] = d.setdefault((
                source, 'bytes_out'), 0) + bytes_out
            d[(source, 'bytes_in')] = d.setdefault((source, 'bytes_in'), 0) + \
                                      bytes_in

            d['format_query'] = d.setdefault('format_query', 0) + \
                                line_data.get('format', 0)
            d['marker_query'] = d.setdefault('marker_query', 0) + \
                                line_data.get('marker', 0)
            d['prefix_query'] = d.setdefault('prefix_query', 0) + \
                                line_data.get('prefix', 0)
            d['delimiter_query'] = d.setdefault('delimiter_query', 0) + \
                                   line_data.get('delimiter', 0)
            path = line_data.get('path', 0)
            d['path_query'] = d.setdefault('path_query', 0) + path

            code = '%dxx' % (code / 100)
            key = (source, op_level, method, code)
            d[key] = d.setdefault(key, 0) + 1

            hourly_aggr_info[aggr_key] = d
        if bad_lines > (total_lines * self.warn_percent):
            name = '/'.join([data_object_account, data_object_container,
                             data_object_name])
            self.logger.warning(_('I found a bunch of bad lines in %(name)s '\
                    '(%(bad)d bad, %(total)d total)') %
                    {'name': name, 'bad': bad_lines, 'total': total_lines})
        return hourly_aggr_info

    def keylist_mapping(self):
        source_keys = 'service public'.split()
        level_keys = 'account container object'.split()
        verb_keys = 'GET PUT POST DELETE HEAD COPY'.split()
        code_keys = '2xx 4xx 5xx'.split()

        keylist_mapping = {
        #   <db key> : <row key> or <set of row keys>
            'service_bw_in': ('service', 'bytes_in'),
            'service_bw_out': ('service', 'bytes_out'),
            'public_bw_in': ('public', 'bytes_in'),
            'public_bw_out': ('public', 'bytes_out'),
            'account_requests': set(),
            'container_requests': set(),
            'object_requests': set(),
            'service_request': set(),
            'public_request': set(),
            'ops_count': set(),
        }
        for verb in verb_keys:
            keylist_mapping[verb] = set()
        for code in code_keys:
            keylist_mapping[code] = set()
        for source in source_keys:
            for level in level_keys:
                for verb in verb_keys:
                    for code in code_keys:
                        keylist_mapping['account_requests'].add(
                                        (source, 'account', verb, code))
                        keylist_mapping['container_requests'].add(
                                        (source, 'container', verb, code))
                        keylist_mapping['object_requests'].add(
                                        (source, 'object', verb, code))
                        keylist_mapping['service_request'].add(
                                        ('service', level, verb, code))
                        keylist_mapping['public_request'].add(
                                        ('public', level, verb, code))
                        keylist_mapping[verb].add(
                                        (source, level, verb, code))
                        keylist_mapping[code].add(
                                        (source, level, verb, code))
                        keylist_mapping['ops_count'].add(
                                        (source, level, verb, code))
        return keylist_mapping
