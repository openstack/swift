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

import socket
import time
import unittest

from swift.common.concurrency import USE_EVENTLET, Timeout, spawn


@unittest.skipIf(USE_EVENTLET, "threading Timeout only")
class TestTimeoutRestore(unittest.TestCase):
    def _socketpair(self):
        rd, wr = socket.socketpair()
        self.addCleanup(rd.close)
        self.addCleanup(wr.close)
        return rd, wr

    def test_restores_blocking_socket(self):
        rd, _ = self._socketpair()
        rd.setblocking(True)
        self.assertIsNone(rd.gettimeout())
        with Timeout(seconds=5, socket=rd):
            self.assertEqual(rd.gettimeout(), 5)
        # must be put back to blocking, not left with our 5s timeout
        self.assertIsNone(rd.gettimeout())

    def test_restores_previous_timeout(self):
        rd, _ = self._socketpair()
        rd.settimeout(30)
        with Timeout(seconds=5, socket=rd):
            self.assertEqual(rd.gettimeout(), 5)
        self.assertEqual(rd.gettimeout(), 30)

    def test_timeout_zero_arms_and_restores(self):
        rd, _ = self._socketpair()
        rd.settimeout(30)
        with Timeout(seconds=0, socket=rd):
            # 0 wins the min(): the socket is armed non-blocking
            self.assertEqual(rd.gettimeout(), 0)
        self.assertEqual(rd.gettimeout(), 30)

    def test_closed_socket_on_enter_is_ignored(self):
        rd, _ = self._socketpair()
        rd.close()
        # arming an already-closed socket must not raise, and there is
        # nothing to restore afterwards
        with Timeout(seconds=5, socket=rd):
            pass

    def test_socket_timeout_is_converted(self):
        rd, _ = self._socketpair()
        # a stdlib socket.timeout raised in the block surfaces as our Timeout
        with self.assertRaises(Timeout):
            with Timeout(seconds=5, socket=rd):
                raise socket.timeout()

    def test_check_time_enforces_deadline(self):
        to = Timeout(seconds=5)
        with to:
            to.check_time()  # deadline is in the future: no raise
            to.deadline = time.monotonic() - 1
            with self.assertRaises(Timeout):
                to.check_time()

    def test_restore_timeout_clears_deadline(self):
        to = Timeout(seconds=5)
        with to:
            to.deadline = time.monotonic() - 1
            to.restore_timeout()
            # deadline cleared, so check_time is now a no-op
            to.check_time()

    def test_nested_timeout_takes_min(self):
        rd, _ = self._socketpair()
        rd.settimeout(30)
        with Timeout(seconds=30, socket=rd):
            self.assertEqual(rd.gettimeout(), 30)
            with Timeout(seconds=5, socket=rd):
                self.assertEqual(rd.gettimeout(), 5)  # min(5, 30)
            self.assertEqual(rd.gettimeout(), 30)  # restored

    def test_nested_timeout_not_loosened(self):
        rd, _ = self._socketpair()
        with Timeout(seconds=5, socket=rd):
            self.assertEqual(rd.gettimeout(), 5)
            with Timeout(seconds=30, socket=rd):
                # inner must not loosen the bound past the outer 5s
                self.assertEqual(rd.gettimeout(), 5)  # min(30, 5)
            self.assertEqual(rd.gettimeout(), 5)


class TestSpawn(unittest.TestCase):
    def test_with_args(self):
        f = lambda x, y: x * y
        result = spawn(f, 6, 7)
        self.assertEqual(result.wait(), 42)

    def test_with_kwargs(self):
        def dummy(a, b=1):
            return (a, b)

        result = spawn(dummy, 0, b=2)
        self.assertEqual(result.wait(), (0, 2))

    def test_exception(self):
        def fail():
            raise Exception('reason')

        result = spawn(fail)
        with self.assertRaises(Exception) as ctx:
            result.wait()
        self.assertEqual(str(ctx.exception), 'reason')
