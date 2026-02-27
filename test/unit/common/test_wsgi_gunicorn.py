# Copyright (c) 2010-2026 OpenStack Foundation
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
from unittest.mock import MagicMock

from swift.common.concurrency import USE_EVENTLET

if not USE_EVENTLET:
    from swift.common.wsgi_gunicorn import ChunkedInput


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestChunkedInput(unittest.TestCase):
    def test_read(self):
        body = MagicMock()
        sock = MagicMock()
        req = MagicMock()

        ci = ChunkedInput(body, sock, req)
        ci.set_hundred_continue_response_headers(
            [('X-Obj-Multiphase-Commit', 'yes'),])
        body.read.return_value = b'data'

        # First read
        result = ci.read()
        self.assertEqual(result, b'data')

        # Test if headers were sent once
        sent = sock.sendall.call_args[0][0]
        self.assertIn(b'HTTP/1.1 100 Continue\r\n', sent)
        self.assertIn(b'X-Obj-Multiphase-Commit: yes', sent)
        sock.sendall.assert_called_once()

        # Second read, headers should not been sent again
        result = ci.read()
        self.assertEqual(result, b'data')
        sock.sendall.assert_called_once()

        # Test if body is new after send_hundred_continue_response()
        ci.send_hundred_continue_response()
        self.assertIsNot(ci.body, body)
