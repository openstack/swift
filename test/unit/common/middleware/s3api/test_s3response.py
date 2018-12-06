# Copyright (c) 2014 OpenStack Foundation
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

import unittest

from swift.common.swob import Response
from swift.common.utils import HeaderKeyDict
from swift.common.middleware.s3api.s3response import S3Response
from swift.common.middleware.s3api.utils import sysmeta_prefix


class TestResponse(unittest.TestCase):
    def test_from_swift_resp_slo(self):
        for expected, header_vals in \
                ((True, ('true', '1')), (False, ('false', 'ugahhh', None))):
            for val in header_vals:
                resp = Response(headers={'X-Static-Large-Object': val,
                                         'Etag': 'theetag'})
                s3resp = S3Response.from_swift_resp(resp)
                self.assertEqual(expected, s3resp.is_slo)
                if s3resp.is_slo:
                    self.assertEqual('"theetag-N"', s3resp.headers['ETag'])
                else:
                    self.assertEqual('"theetag"', s3resp.headers['ETag'])

    def test_response_s3api_sysmeta_headers(self):
        for _server_type in ('object', 'container'):
            swift_headers = HeaderKeyDict(
                {sysmeta_prefix(_server_type) + 'test': 'ok'})
            resp = Response(headers=swift_headers)
            s3resp = S3Response.from_swift_resp(resp)
            self.assertEqual(swift_headers, s3resp.sysmeta_headers)

    def test_response_s3api_sysmeta_headers_ignore_other_sysmeta(self):
        for _server_type in ('object', 'container'):
            swift_headers = HeaderKeyDict(
                # sysmeta not leading sysmeta_prefix even including s3api word
                {'x-%s-sysmeta-test-s3api' % _server_type: 'ok',
                 sysmeta_prefix(_server_type) + 'test': 'ok'})
            resp = Response(headers=swift_headers)
            s3resp = S3Response.from_swift_resp(resp)
            expected_headers = HeaderKeyDict(
                {sysmeta_prefix(_server_type) + 'test': 'ok'})
            self.assertEqual(expected_headers, s3resp.sysmeta_headers)

    def test_response_s3api_sysmeta_from_swift3_sysmeta(self):
        for _server_type in ('object', 'container'):
            # swift could return older swift3 sysmeta
            swift_headers = HeaderKeyDict(
                {('x-%s-sysmeta-swift3-' % _server_type) + 'test': 'ok'})
            resp = Response(headers=swift_headers)
            s3resp = S3Response.from_swift_resp(resp)
            expected_headers = HeaderKeyDict(
                {sysmeta_prefix(_server_type) + 'test': 'ok'})
            # but Response class should translates as s3api sysmeta
            self.assertEqual(expected_headers, s3resp.sysmeta_headers)

    def test_response_swift3_sysmeta_does_not_overwrite_s3api_sysmeta(self):
        for _server_type in ('object', 'container'):
            # same key name except sysmeta prefix
            swift_headers = HeaderKeyDict(
                {('x-%s-sysmeta-swift3-' % _server_type) + 'test': 'ng',
                 sysmeta_prefix(_server_type) + 'test': 'ok'})
            resp = Response(headers=swift_headers)
            s3resp = S3Response.from_swift_resp(resp)
            expected_headers = HeaderKeyDict(
                {sysmeta_prefix(_server_type) + 'test': 'ok'})
            # but only s3api sysmeta remains in the response sysmeta_headers
            self.assertEqual(expected_headers, s3resp.sysmeta_headers)


if __name__ == '__main__':
    unittest.main()
