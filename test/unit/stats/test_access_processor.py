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

# TODO: Tests

import unittest
from swift.stats import access_processor


class TestAccessProcessor(unittest.TestCase):

    def test_log_line_parser_field_count(self):
        p = access_processor.AccessLogProcessor({})
        # too few fields
        log_line = [str(x) for x in range(17)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o'
        log_line = 'x'*16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {}
        self.assertEquals(res, expected)
        # right amount of fields
        log_line = [str(x) for x in range(18)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o'
        log_line = 'x'*16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {'code': 8, 'processing_time': '17', 'auth_token': '11',
                    'month': '01', 'second': '6', 'year': '3', 'tz': '+0000',
                    'http_version': '7', 'object_name': 'o', 'etag': '14',
                    'method': '5', 'trans_id': '15', 'client_ip': '2',
                    'bytes_out': 13, 'container_name': 'c', 'day': '1',
                    'minute': '5', 'account': 'a', 'hour': '4',
                    'referrer': '9', 'request': '/v1/a/c/o',
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3'}
        self.assertEquals(res, expected)
        # too many fields
        log_line = [str(x) for x in range(19)]
        log_line[1] = 'proxy-server'
        log_line[4] = '1/Jan/3/4/5/6'
        log_line[6] = '/v1/a/c/o'
        log_line = 'x'*16 + ' '.join(log_line)
        res = p.log_line_parser(log_line)
        expected = {'code': 8, 'processing_time': '17', 'auth_token': '11',
                    'month': '01', 'second': '6', 'year': '3', 'tz': '+0000',
                    'http_version': '7', 'object_name': 'o', 'etag': '14',
                    'method': '5', 'trans_id': '15', 'client_ip': '2',
                    'bytes_out': 13, 'container_name': 'c', 'day': '1',
                    'minute': '5', 'account': 'a', 'hour': '4',
                    'referrer': '9', 'request': '/v1/a/c/o',
                    'user_agent': '10', 'bytes_in': 12, 'lb_ip': '3'}
        self.assertEquals(res, expected)


if __name__ == '__main__':
    unittest.main()
