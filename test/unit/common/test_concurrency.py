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

import itertools
import socket
import time
import unittest

import threading
from swift.common.concurrency import (
    Pool, USE_EVENTLET, Timeout, spawn, tpool, SwiftPool, sleep, reset_pool,
    SwiftPile)


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


@unittest.skipIf(USE_EVENTLET, "Only tested when eventlet is disabled")
class TestSwiftPool(unittest.TestCase):
    def test_waitall(self):
        results = []

        def append_val(val):
            results.append(val)

        pool = SwiftPool(size=4)
        for i in range(5):
            pool.spawn_n(append_val, i)
        pool.waitall()
        self.assertEqual(sorted(results), [0, 1, 2, 3, 4])
        pool.shutdown(wait=True)

    def test_default_is_non_blocking(self):
        # Backpressure is off by default: no bounding semaphore, so a recursive
        # or producer-loop caller can't deadlock on a blocked spawn. Opt-in
        # restores the semaphore.
        self.assertIsNone(SwiftPool(size=2)._sem)
        self.assertIsNotNone(SwiftPool(size=2, backpressure=True)._sem)

    def test_backpressure_blocks_spawn_when_full(self):
        # With backpressure on, a producer that outruns the workers blocks on
        # spawn rather than queueing unboundedly (the #4 OOM guard for the
        # expirer/reconciler/updater scan loops).
        pool = SwiftPool(size=1, backpressure=True)
        started = threading.Event()
        release = threading.Event()

        def blocker():
            started.set()
            release.wait()

        pool.spawn_n(blocker)            # occupies the only slot
        self.assertTrue(started.wait(2))

        spawned = threading.Event()

        def do_spawn():
            pool.spawn_n(lambda: None)   # must block: pool is full
            spawned.set()

        t = threading.Thread(target=do_spawn)
        t.start()
        try:
            # the second spawn is blocked on the backpressure semaphore
            self.assertFalse(spawned.wait(0.2))
            release.set()                # free the slot
            self.assertTrue(spawned.wait(2))
        finally:
            release.set()
            t.join()
            pool.waitall()
            pool.shutdown(wait=True)

    def test_reset_pool_unblocks_producer_wedged_on_backpressure(self):
        # #1 deadlock recovery: a wedged worker never releases its backpressure
        # permit, so a producer blocked on spawn hangs forever. reset_pool()
        # must top the semaphore back up AND abandon the wedged worker so a
        # fresh one runs the requeued work.
        pool = SwiftPool(size=1, backpressure=True)
        started = threading.Event()
        wedge = threading.Event()

        def wedged():
            started.set()
            wedge.wait()                 # never freed until teardown

        pool.spawn_n(wedged)             # occupies and wedges the only slot
        self.assertTrue(started.wait(2))

        spawned = threading.Event()
        ran = threading.Event()

        def do_spawn():
            pool.spawn_n(ran.set)        # blocks: pool is full, worker wedged
            spawned.set()

        t = threading.Thread(target=do_spawn)
        t.start()
        try:
            self.assertFalse(spawned.wait(0.2))   # producer is wedged
            reset_pool(pool)             # recovery: permit + fresh worker
            self.assertTrue(spawned.wait(2))      # producer now proceeds
            # the real test: a fresh worker must actually RUN the requeued job,
            # even though the original worker is still wedged.
            self.assertTrue(ran.wait(2))
        finally:
            wedge.set()
            t.join()
            pool.shutdown(wait=True)

    def test_reset_pool_retires_revived_workers(self):
        # #1: a wedged worker abandoned by reset_pool() must retire when it
        # later returns, not resume consuming from the shared queue -- else
        # worker generations accumulate and exceed the configured concurrency.
        pool = SwiftPool(size=1, backpressure=True)
        started = threading.Event()
        wedge = threading.Event()

        def wedged():
            started.set()
            wedge.wait(5)

        pool.spawn_n(wedged)
        self.assertTrue(started.wait(2))
        reset_pool(pool)              # abandon + retire generation 0
        wedge.set()                   # revive the abandoned worker
        sleep(0.1)

        idents = set()
        lock = threading.Lock()

        def rec():
            with lock:
                idents.add(threading.current_thread().ident)
            sleep(0.02)

        futs = [pool.spawn(rec) for _ in range(20)]
        for f in futs:
            f.result(5)
        # size=1: every task must run on the single live worker; a revived
        # abandoned worker would show up as a second ident (over-concurrency).
        self.assertLessEqual(len(idents), pool.size)
        pool.shutdown(wait=True)

    def test_waitall_catches_descendants(self):
        # waitall() must await work spawned by running tasks (recursive
        # spawn_n), not just the futures present when it was called -- and must
        # not deadlock doing so on a pool smaller than the recursion breadth.
        lock = threading.Lock()
        count = [0]
        pool = SwiftPool(size=2)

        def task(depth):
            with lock:
                count[0] += 1
            if depth < 3:
                for _ in range(3):
                    pool.spawn_n(task, depth + 1)

        for _ in range(3):
            pool.spawn_n(task, 1)
        pool.waitall()
        # full closure: 3 + 9 + 27
        self.assertEqual(count[0], 39)
        pool.shutdown(wait=True)

    def test_completed_futures_are_cleaned_up(self):
        # Completed futures must not accumulate even without a waitall() call.
        pool = SwiftPool(size=4)
        futures = [pool.spawn_n(lambda x: x, i) for i in range(100)]
        for future in futures:
            future.result()
        # done callbacks may still be firing; give them a moment
        for _ in range(100):
            if not pool.futures:
                break
            sleep(0.01)
        self.assertEqual(pool.futures, set())
        pool.shutdown(wait=True)

    def test_free_and_running(self):
        pool = SwiftPool(size=4)
        self.assertEqual(pool.free(), 4)
        self.assertEqual(pool.running(), 0)
        pool.shutdown(wait=True)

    def test_imap_returns_results_in_order(self):
        def double(x):
            return x * 2

        pool = SwiftPool(size=4)
        results = list(pool.imap(double, [1, 2, 3, 4, 5]))
        self.assertEqual(results, [2, 4, 6, 8, 10])
        pool.shutdown(wait=True)

    def test_starmap_returns_results_in_order(self):
        def multiply(a, b):
            return a * b

        pool = SwiftPool(size=4)
        results = list(pool.starmap(multiply, [(2, 3), (4, 5), (6, 7)]))
        self.assertEqual(results, [6, 20, 42])
        pool.shutdown(wait=True)

    def test_runs_in_separate_thread(self):
        main_thread_id = threading.current_thread().ident
        pool = SwiftPool(size=2)
        future = pool.spawn(lambda: threading.current_thread().ident)
        worker_thread_id = future.result()
        self.assertNotEqual(main_thread_id, worker_thread_id)
        pool.shutdown(wait=True)

    def test_workers_are_daemonic_and_unregistered(self):
        # Workers must be daemonic and absent from concurrent.futures'
        # _threads_queues, so a blocked worker is abandoned at interpreter
        # exit instead of hanging the join (see _adjust_threads).
        import concurrent.futures.thread as cf_thread
        pool = SwiftPool(size=2)
        started = threading.Event()
        try:
            pool.spawn_n(started.set)
            self.assertTrue(started.wait(5))
            self.assertTrue(pool._threads)
            for t in pool._threads:
                self.assertTrue(t.daemon)
                self.assertNotIn(t, cf_thread._threads_queues)
        finally:
            pool.shutdown(wait=True)

    def test_imap_pulls_lazily(self):
        # The source advances only as results are consumed, bounded by the
        # pool size; abandoning the iterator stops pulling.
        pool = SwiftPool(size=2)
        pulled = []

        def source():
            for i in range(100):
                pulled.append(i)
                yield i

        try:
            it = pool.imap(lambda x: x, source())
            self.assertEqual(0, next(it))
            self.assertLessEqual(len(pulled), 4)
            it.close()
            self.assertLessEqual(len(pulled), 4)
        finally:
            pool.shutdown(wait=True)

    def test_imap_infinite_input(self):
        # An infinite source must not be drained up front.
        pool = SwiftPool(size=2)
        try:
            it = pool.imap(lambda x: x, itertools.count())
            self.assertEqual([0, 1, 2, 3, 4],
                             [next(it) for _ in range(5)])
            it.close()
        finally:
            pool.shutdown(wait=True)


@unittest.skipIf(USE_EVENTLET, "Only tested when eventlet is disabled")
class TestSwiftPile(unittest.TestCase):
    def test_results_in_order(self):
        # Test that a slow func result is still returned in order
        pile = SwiftPile(4)

        def slow():
            sleep(0.1)
            return 0
        pile.spawn(slow)

        for i in range(1, 5):
            pile.spawn(lambda x=i: x)

        self.assertEqual(list(pile), [0, 1, 2, 3, 4])

    def test_runs_in_separate_thread(self):
        main_thread_id = threading.current_thread().ident
        pile = SwiftPile(2)
        pile.spawn(lambda: threading.current_thread().ident)
        worker_thread_id = list(pile)[0]
        self.assertNotEqual(main_thread_id, worker_thread_id)

    def test_empty_pile_yields_nothing(self):
        pile = SwiftPile(2)
        self.assertEqual(list(pile), [])


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
