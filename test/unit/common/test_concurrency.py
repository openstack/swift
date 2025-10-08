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

import threading
from swift.common.concurrency import Pool, USE_EVENTLET, Timeout, spawn, tpool


@unittest.skipIf(USE_EVENTLET, "Only tested when eventlet is disabled")
class TestPool(unittest.TestCase):

    def _make_pool(self, max_size=2):
        class TestPool(Pool):
            created = 0

            def create(self):
                TestPool.created += 1
                return TestPool.created

        return TestPool(max_size=max_size)

    def test_get_returns_new_item(self):
        # get() on an empty pool creates a fresh item
        pool = self._make_pool(max_size=2)
        self.assertEqual(pool.get(), 1)

    def test_get_returns_cached_item(self):
        # a put() item is handed back by the next get(), with no new create
        pool = self._make_pool(max_size=2)
        pool.put('cached')
        self.assertEqual(pool.get(), 'cached')

    def test_concurrent_get_put(self):
        pool = self._make_pool(max_size=2)
        results = []
        errors = []

        def worker():
            try:
                item = pool.get()
                results.append(item)
                pool.put(item)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(len(results), 4, results)

    def test_get_timeout_on_exhausted_pool(self):
        pool = self._make_pool(max_size=1)
        item = pool.get()  # exhaust pool
        with self.assertRaises(Timeout):
            pool.get(timeout=0.01)
        pool.put(item)
        self.assertEqual(pool.get(timeout=0.01), item)

    def test_put_notifies_waiting_get(self):
        pool = self._make_pool(max_size=1)
        first = pool.get()  # exhaust pool
        result = []
        e = threading.Event()

        def getter():
            e.set()
            result.append(pool.get())

        t = threading.Thread(target=getter)
        t.start()
        e.wait()
        self.assertTrue(t.is_alive())
        self.assertEqual(result, [])
        pool.put(first)  # return item, unblock getter
        t.join(timeout=5)
        self.assertEqual(result, [first])


class TestTpool(unittest.TestCase):
    def test_with_args(self):
        f = lambda x, y: x * y
        result = tpool.execute(f, 6, 7)
        self.assertEqual(result, 42)

    def test_with_kwargs(self):
        def dummy(a, b=1):
            return (a, b)

        result = tpool.execute(dummy, 0, b=2)
        self.assertEqual(result, (0, 2))

    def test_exception(self):
        class DummyException(Exception):
            pass

        def fail():
            raise DummyException('reason')

        with self.assertRaises(DummyException):
            tpool.execute(fail)

    @unittest.skipIf(USE_EVENTLET, "tests the threading tpool shim")
    def test_set_num_threads_is_a_no_op(self):
        self.assertIsNone(tpool.set_num_threads(10))
        self.assertIsNone(tpool.set_num_threads(num_threads=5))


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


@unittest.skipIf(USE_EVENTLET, "threading Pool only")
class TestPoolCreateFailure(unittest.TestCase):
    def test_create_failure_wakes_capacity_waiter(self):
        first_in_create = threading.Event()
        release_first = threading.Event()

        class P(Pool):
            attempts = 0

            def create(self):
                P.attempts += 1
                if P.attempts == 1:
                    # First caller reserves the only slot, then fails once B
                    # is waiting for capacity.
                    first_in_create.set()
                    release_first.wait(5)
                    raise RuntimeError('boom')
                return 'conn'

        pool = P(max_size=1)
        a_err = []

        def worker_a():
            try:
                pool.get()
            except RuntimeError as e:
                a_err.append(e)

        ta = threading.Thread(target=worker_a)
        ta.start()
        self.assertTrue(first_in_create.wait(5))

        b_res = []

        def worker_b():
            b_res.append(pool.get(timeout=5))

        tb = threading.Thread(target=worker_b)
        tb.start()
        time.sleep(0.1)  # let B reach the capacity wait
        release_first.set()  # A fails: frees the slot and must notify B

        ta.join(5)
        tb.join(5)
        self.assertEqual(len(a_err), 1)
        # B must create once the slot frees, not hang until its timeout
        self.assertEqual(b_res, ['conn'])
