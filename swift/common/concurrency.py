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

"""Concurrency primitives for Swift.

All modules that need eventlet functionality should import from here
rather than importing directly from eventlet.
"""

import collections
import importlib.util
import os
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from socket import timeout as socket_timeout


def config_false_value(value):
    return value is False or (
        isinstance(value, str)
        and value.lower() in {'false', '0', 'no', 'off', 'f', 'n'})


# Use eventlet by default if it is installed
USE_EVENTLET = importlib.util.find_spec('eventlet') is not None

# Check if eventlet is manually disabled even if installed
if USE_EVENTLET:
    if config_false_value(os.environ.get('USE_EVENTLET')):
        USE_EVENTLET = False

# config_false_value exists only to establish USE_EVENTLET above.
del config_false_value


import eventlet  # noqa: E402
import eventlet.debug
import eventlet.greenio
import eventlet.greenthread
import eventlet.hubs
import eventlet.patcher
import eventlet.queue
import eventlet.semaphore
import eventlet.wsgi
from eventlet import GreenPile
from eventlet import greenio, greenpool, hubs, patcher, queue, wsgi
from eventlet import debug, listen, timeout, websocket
from eventlet import greenthread
from eventlet.event import Event
from eventlet.green import socket, ssl, subprocess
from eventlet.green import os as green_os
from eventlet.green import threading as green_threading
from eventlet.green.http import client as green_http_client
from eventlet.green.http.client import CONTINUE, HTTPConnection, \
    HTTPResponse, HTTPSConnection, ImproperConnectionState, _UNKNOWN
from eventlet.green.urllib import request as urllib_request
from eventlet.greenthread import getcurrent
from eventlet.hubs import trampoline  # noqa: F401
from eventlet.queue import Empty, LightQueue, Queue
from eventlet.semaphore import Semaphore
from eventlet.support.greenlets import GreenletExit
import eventlet.green.profile as eprofile  # noqa: F401
hub_exceptions = eventlet.debug.hub_exceptions
hub_prevent_multiple_readers = eventlet.debug.hub_prevent_multiple_readers
monkey_patch = eventlet.patcher.monkey_patch
shutdown_safe = eventlet.greenio.shutdown_safe
ChunkReadError = eventlet.wsgi.ChunkReadError

if USE_EVENTLET:
    from eventlet import tpool
    from eventlet.pools import Pool
    from eventlet import sleep

    from eventlet import Timeout as _Timeout
    from eventlet import GreenPool as _GreenPool

    class SwiftPool(_GreenPool):
        # GreenPool already blocks spawn when full, so the threading-only
        # ``backpressure`` flag is accepted and ignored here.
        def __init__(self, size=1024, backpressure=False):
            super(SwiftPool, self).__init__(size)

    # No real lock needed under eventlet (cooperative scheduling)
    from contextlib import nullcontext
    CooperativeLock = nullcontext

    class Timeout(_Timeout):
        def __init__(self, *args, **kwargs):
            # Drop the socket kwarg; eventlet's Timeout doesn't accept it.
            kwargs.pop('socket', None)
            super(Timeout, self).__init__(*args, **kwargs)

        def check_time(self):
            # Only needed without eventlet
            pass

    # Helper functions to replace eventlet spawn with a threading equivalent
    class EventletResult(object):
        """Wrapper to support timeout arg when using eventlet """
        def __init__(self, gt):
            self._gt = gt

        @property
        def dead(self):
            return self._gt.dead

        def wait(self, timeout=None):
            if timeout is not None:
                with Timeout(timeout):
                    return self._gt.wait()
            return self._gt.wait()

        def kill(self):
            self._gt.kill()

    def spawn(func, *args, **kwargs):
        return EventletResult(eventlet.spawn(func, *args, **kwargs))

    # spawn_n is not used with a kwarg, just use the unwrapped function
    spawn_n = eventlet.spawn_n

    def reset_pool(pool):
        # Stop the pool's in-flight work: kill the greenthreads still running
        # in it (the pool object itself stays usable). coroutines_running
        # clears itself as they die.
        for coro in list(pool.coroutines_running):
            try:
                coro.kill(GreenletExit)
            except GreenletExit:
                pass

else:
    import os as green_os  # noqa: F811
    import socket  # noqa: F811
    import ssl  # noqa: F811
    import threading as green_threading  # noqa: F811
    import urllib.request as urllib_request  # noqa: F811
    from queue import Empty, Queue as _StdQueue  # noqa: F811
    from threading import Lock as CooperativeLock

    class Queue(_StdQueue):  # noqa: F811
        def resize(self, size):
            # Match eventlet's LightQueue.resize(): set maxsize and wake
            # blocked putters. stdlib Queue has no resize(); callers use it
            # to guarantee room for a sentinel.
            with self.mutex:
                self.maxsize = size
                self.not_full.notify_all()

    LightQueue = Queue  # noqa: F811

    try:
        from greenlet import GreenletExit  # noqa: F811
    except ImportError:
        class GreenletExit(BaseException):
            pass

    class Timeout(BaseException):
        # Distinguishes "no timeout captured" from a captured None (a socket
        # that was blocking), so restore_timeout always puts the old value
        # back instead of leaving our timeout installed.
        _UNSET = object()

        def __init__(self, seconds=None, exception=None, socket=None):
            self.seconds = seconds
            self.exception = exception
            self.socket = socket
            self.old_timeout = self._UNSET
            self.deadline = None

        def __enter__(self):
            if self.seconds is not None:
                if self.seconds > 0:
                    self.deadline = time.monotonic() + self.seconds
            if self.seconds is not None and self.socket is not None:
                try:
                    self.old_timeout = self.socket.gettimeout()
                    self.socket.settimeout(
                        min(t for t in (self.seconds, self.old_timeout)
                            if t is not None))
                except OSError:
                    # socket already closed; nothing to bound and nothing to
                    # restore, so leave old_timeout unset.
                    self.old_timeout = self._UNSET
            return self

        def check_time(self):
            if self.deadline is not None and time.monotonic() > self.deadline:
                raise self

        def restore_timeout(self):
            self.deadline = None
            if self.old_timeout is not self._UNSET and self.socket is not None:
                try:
                    self.socket.settimeout(self.old_timeout)
                except OSError:
                    pass
                self.old_timeout = self._UNSET

        def __exit__(self, exc_type, exc_value, exc_traceback):
            self.restore_timeout()
            if exc_type is socket_timeout:
                raise self
            return False

        def __str__(self):
            if self.seconds is not None:
                if self.seconds == 1:
                    suffix = ''
                else:
                    suffix = 's'
                return '%s second%s' % (self.seconds, suffix)
            return ''

        # Only used in tests, but just in case restore timeouts
        def cancel(self):
            self.restore_timeout()

    def sleep(seconds=0):
        if seconds:
            time.sleep(seconds)

    # Helper functions to replace eventlet spawn with a threading equivalent
    _spawn_kill_local = threading.local()

    class ThreadResult(object):
        def __init__(self, func, args, kwargs):
            self.result = None
            self.exc = None
            self._kill_hook = None
            self.thread = threading.Thread(
                target=self.run, args=(func, args, kwargs))
            self.thread.daemon = True
            self.thread.start()

        def run(self, func, args, kwargs):
            _spawn_kill_local.handle = self
            try:
                self.result = func(*args, **kwargs)
            except BaseException as e:
                self.exc = e
            finally:
                _spawn_kill_local.handle = None

        def wait(self, timeout=None):
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                raise Timeout(timeout)
            if self.exc:
                raise self.exc
            return self.result

        @property
        def dead(self):
            return not self.thread.is_alive()

        def kill(self):
            # Real threads can't be interrupted, but a stoppable callable can
            # register a stop hook via register_kill_hook() -- the threading
            # analogue of GreenThread.kill(). With no hook the daemon thread
            # keeps running: it won't block interpreter exit, but it may
            # still mutate shared state or hold resources until it returns.
            if self._kill_hook is not None:
                self._kill_hook()

    class Executor:
        """Drop-in replacement for eventlet.tpool running in the current
        thread.

        All calls to execute will run in the current thread and not in a
        separate thread pool. Eventlet uses a threadpool to be able to yield
        to other coros and not block the current one, but without eventlet
        this is not needed - it is already running in a thread.

        Note this has no concurrency limit, where eventlet.tpool bounds
        concurrent calls to its threadpool size. Swift does not rely on that
        bound - the work it hands to tpool is per-request disk I/O, already
        capped by the worker's thread count - but out-of-tree code that used
        tpool to cap expensive work (auth middleware limiting concurrent
        password hashing, say) must not swap to this: the cap is gone, and a
        long call blocks the worker thread it runs on. Bound such work with
        its own pool, or with the server's ``threads`` setting.
        """
        # No-op to be compatible with eventlet call
        def set_num_threads(self, *args, **kwargs):
            pass

        @staticmethod
        def execute(func, *args, **kwargs):
            return func(*args, **kwargs)

    def spawn(func, *args, **kwargs):
        return ThreadResult(func, args, kwargs)

    # eventlet spawn_n is spawn without a return value; reuse spawn here.
    spawn_n = spawn

    def reset_pool(pool):
        # Stop the pool's in-flight work (the pool object itself stays
        # usable): real threads can't be interrupted, so cancel any
        # not-yet-started work, then clear the tracking set so the pool can be
        # reused.
        for future in list(pool.futures):
            future.cancel()
        pool.futures = set()
        # Order matters: clear _threads and reset _idle BEFORE topping up the
        # permit, so a producer that wakes on the freed permit sees an empty
        # pool and _adjust_threads starts a fresh worker (release the permit
        # first and it could enqueue work, still see a full _threads, and spawn
        # no replacement). Bumping _generation retires abandoned workers: a
        # wedged one that later returns exits instead of consuming more work,
        # so they can't accumulate past `size`. Abandoned threads are daemon,
        # so they die at exit like eventlet's killed greenthreads.
        with pool._threads_lock:
            pool._threads = set()
            pool._generation += 1
        pool._idle = threading.Semaphore(0)
        # A wedged native thread can't be interrupted to release its
        # backpressure permit, so a producer blocked in submit() would hang
        # forever (an eventlet greenthread, by contrast, is killed here and
        # unwinds through its release). Top the semaphore back up to its full
        # budget so the producer wakes; BoundedSemaphore caps the count, so a
        # wedged worker that finishes later and releases again is a safe no-op.
        sem = getattr(pool, '_sem', None)
        if sem is not None:
            for _ in range(pool.size):
                try:
                    sem.release()
                except ValueError:
                    break

    class Pool(object):
        """
        Thread-safe connection pool replacement for eventlet.pools.Pool.

        This code is very similar to eventlet/eventlet/pools.py, but uses
        threading.Condition to maintain thread-safety.
        """
        def __init__(self, min_size=0, max_size=4, create=None):
            self.min_size = min_size
            self.max_size = max_size
            self.current_size = 0
            self.free_items = collections.deque()
            self.available = threading.Condition()

            if create is not None:
                self.create = create

            for x in range(min_size):
                self.current_size += 1
                self.free_items.append(self.create())

        def get(self, timeout=None):
            deadline = None
            if timeout is not None:
                deadline = time.monotonic() + timeout
            with self.available:
                while True:
                    if self.free_items:
                        return self.free_items.popleft()
                    if self.current_size < self.max_size:
                        # Reserve a slot and create below, outside the lock.
                        self.current_size += 1
                        break
                    # At capacity; wait for an item to be returned or for a
                    # reserved slot to be freed (e.g. a failed create()).
                    remaining = None
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise Timeout(timeout)
                    self.available.wait(remaining)

            # We hold a reserved slot. create() may block (e.g. opening a
            # socket); run it without the lock so up to max_size create
            # concurrently (like eventlet.pools.Pool).
            try:
                return self.create()
            except BaseException:
                with self.available:
                    self.current_size -= 1
                    # The slot we reserved is free again; wake a waiter so it
                    # can create in our place instead of blocking until its
                    # pool_timeout even though capacity is now available.
                    self.available.notify()
                raise

        def put(self, item):
            with self.available:
                if self.current_size > self.max_size:
                    # max_size never changes, so get() cannot hand out more
                    # than it: this item did not come from this pool.
                    self.current_size -= 1
                    raise RuntimeError(
                        'put() called on a pool that is over capacity '
                        '(%d > %d)' % (self.current_size + 1, self.max_size))

                self.free_items.append(item)

                # Notify self.available.wait() in get() to re-acquire lock
                self.available.notify()

        def create(self):
            raise NotImplementedError()

        # dispersion_populate and dispersion_report require this
        @contextmanager
        def item(self):
            item = self.get()
            try:
                yield item
            finally:
                self.put(item)
    # No need for a threadpool when already running in threads.
    tpool = Executor()

    # Imported lazily so the eventlet path never triggers
    # concurrent.futures.thread's module-level threading.Lock() before
    # eventlet.monkey_patch() runs.
    import weakref
    from concurrent.futures import Future, wait

    _POOL_SENTINEL = None

    def _swiftpool_worker(pool_ref, work_queue, work_sem, generation):
        # Daemon worker for SwiftPool. Pulls (future, func, args, kwargs) items
        # and runs them, releasing the backpressure permit (if any) when done
        # and marking itself idle so SwiftPool reuses it. pool_ref is a weakref
        # whose callback enqueues a sentinel when the pool is garbage
        # collected, so an idle worker on get() doesn't keep the pool alive and
        # exits cleanly (same lifecycle as ThreadPoolExecutor, but public APIs
        # only).
        while True:
            item = work_queue.get()
            if item is _POOL_SENTINEL:
                # pool gc'd or shut down; wake the next worker and exit.
                work_queue.put(_POOL_SENTINEL)
                return
            future, func, args, kwargs = item
            del item
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        result = func(*args, **kwargs)
                    except BaseException as exc:
                        future.set_exception(exc)
                        del exc
                    else:
                        future.set_result(result)
                        del result
            finally:
                if work_sem is not None:
                    # BoundedSemaphore: a wedged worker finishing after
                    # reset_pool() topped the budget up would over-release;
                    # ignore it -- the permit was already reclaimed.
                    try:
                        work_sem.release()
                    except ValueError:
                        pass
                del future, func, args, kwargs
            pool = pool_ref()
            if pool is None:
                return
            if pool._generation != generation:
                # reset_pool() retired this generation (we were likely a wedged
                # worker that has now returned). Exit instead of consuming more
                # work, so abandoned workers can't accumulate and push the live
                # worker count past `size`.
                return
            pool._idle.release()
            del pool

    class SwiftPool(object):
        """GreenPool-compatible pool of daemon worker threads.

        A Swift-owned replacement for eventlet.GreenPool, with the same API so
        callers don't need per-method ``if USE_EVENTLET`` branches. A
        ThreadPoolExecutor subclass can't provide what Swift needs without
        copying CPython internals, so this builds on public APIs only:

          * daemon worker threads, never registered for the interpreter-exit
            join, so a stuck task is abandoned at exit instead of hanging it
            (like GreenPool greenthreads);
          * an optional BoundedSemaphore(size) bounding outstanding work, so
            spawn()/submit() block when the pool is full (backpressure) rather
            than queueing unboundedly. Off by default: the worker count already
            caps concurrency to ``size``, and a blocking spawn deadlocks a
            recursive caller (e.g. the account auditor) -- a worker that spawns
            onto a full pool would wait on a slot only another worker can free.
            Opt in for a non-recursive producer loop that must throttle a
            genuinely unbounded input stream (e.g. the DB/object replicators);
            a stuck worker can't be interrupted to free its slot (unlike an
            eventlet greenthread), so reset_pool() tops the budget back up to
            let a lockup detector unblock the producer;
          * concurrent.futures.Future results;
          * a locked tracking set so reset_pool()/waitall() see in-flight work.

        Idle workers block on the work queue; a weakref callback enqueues a
        sentinel when the pool is garbage collected so they exit. Workers are
        started lazily (up to ``size``) and reused while idle.
        """

        def __init__(self, size=1024, backpressure=False):
            self.size = size
            self.futures = set()
            self._futures_lock = threading.Lock()
            self._work = Queue()
            # bounds outstanding (queued + running) work for backpressure
            self._sem = threading.BoundedSemaphore(size) if backpressure \
                else None
            # counts idle workers available for reuse
            self._idle = threading.Semaphore(0)
            self._threads_lock = threading.Lock()
            self._threads = set()
            # bumped by reset_pool() to retire the current worker generation
            self._generation = 0

            def shutdown_cb(_, q=self._work):
                q.put(_POOL_SENTINEL)
            self._ref = weakref.ref(self, shutdown_cb)

        def _adjust_threads(self):
            # Reuse an idle worker if one is available, otherwise start a new
            # daemon worker (up to size).
            if self._idle.acquire(timeout=0):
                return
            with self._threads_lock:
                if len(self._threads) >= self.size:
                    return
                t = threading.Thread(
                    target=_swiftpool_worker,
                    args=(self._ref, self._work, self._sem, self._generation),
                    name='swift-pool-worker')
                t.daemon = True
                self._threads.add(t)
                t.start()

        def submit(self, func, *args, **kwargs):
            if self._sem is not None:
                self._sem.acquire()  # backpressure: block while pool is full
            future = Future()
            self._work.put((future, func, args, kwargs))
            self._adjust_threads()
            return future

        def _track(self, future):
            with self._futures_lock:
                self.futures.add(future)
            # Runs immediately if the future is already done
            future.add_done_callback(self._untrack)
            return future

        def _untrack(self, future):
            with self._futures_lock:
                self.futures.discard(future)

        def spawn(self, func, *args, **kwargs):
            return self._track(self.submit(func, *args, **kwargs))

        def spawn_n(self, func, *args, **kwargs):
            return self._track(self.submit(func, *args, **kwargs))

        def waitall(self, timeout=None):
            # Wait until the pool is idle. Re-snapshot after each batch so work
            # a running task spawns (recursive spawn_n, e.g. the account
            # auditor descending account/container/object) is also awaited,
            # matching GreenPool.waitall. A child is tracked before its
            # parent's future completes, so an empty snapshot means the pool is
            # genuinely idle.
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                with self._futures_lock:
                    futures = list(self.futures)
                if not futures:
                    return
                if deadline is None:
                    remaining = None
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise Timeout()
                done, not_done = wait(futures, timeout=remaining)
                if not_done:
                    # The timeout elapsed with work still running. A real
                    # thread can't be interrupted, so raise and let the caller
                    # recover (reset_pool); the stuck worker runs on but the
                    # caller (e.g. the reconstructor's lockup recovery) stops
                    # hanging.
                    raise Timeout()

        def running(self):
            with self._futures_lock:
                return len([f for f in self.futures if f.running()])

        def free(self):
            return self.size - self.running()

        def imap(self, func, *iterables):
            # Lazy, ordered and bounded like eventlet.GreenPool.imap: keep at
            # most ``size`` calls in flight, pulling from the source only as
            # results are consumed, so a large or infinite input doesn't queue
            # unboundedly. Abandoning the iterator cancels not-yet-started
            # work (the worker honours Future.cancel).
            pending = collections.deque()
            try:
                for args in zip(*iterables):
                    pending.append(self.submit(func, *args))
                    if len(pending) >= self.size:
                        yield pending.popleft().result()
                while pending:
                    yield pending.popleft().result()
            finally:
                for fut in pending:
                    fut.cancel()

        def map(self, func, *iterables):
            return self.imap(func, *iterables)

        def starmap(self, func, iterable):
            return self.imap(lambda args: func(*args), iterable)

        def shutdown(self, wait=True):
            # Drain queued work, then wake workers to exit (each propagates the
            # sentinel to the next) and optionally join them.
            with self._threads_lock:
                threads = list(self._threads)
                self._threads = set()
            self._work.put(_POOL_SENTINEL)
            if wait:
                for t in threads:
                    t.join()


def report_worker_exception():
    # Print an unhandled worker exception. eventlet's hub prints it when
    # debug_exceptions is on; without eventlet there is no hub, so print here.
    if not USE_EVENTLET or eventlet.hubs.get_hub().debug_exceptions:
        traceback.print_exception(*sys.exc_info())


def clear_connect_timeout(sock):
    # Once a backend connection is established (the connect was bounded by the
    # conn_timeout passed to http_connect), the socket must not keep that short
    # timeout, or it would also bound body reads and fire before the intended
    # node_timeout. Under eventlet the greenthread Watchdog bounds body reads,
    # so clear it (matching pre-threading, where eventlet set no backend socket
    # timeout). Without eventlet the per-read WatchdogTimeout sets it, so
    # leave.
    if USE_EVENTLET and sock is not None:
        sock.settimeout(None)


# flake8 raises a F401 without this
__all__ = [
    'USE_EVENTLET',
    'reset_pool',
    'debug',
    'greenio',
    'greenthread',
    'hubs',
    'patcher',
    'queue',
    'wsgi',
    'GreenPile',
    'SwiftPool',
    'Timeout',
    'greenpool',
    'tpool',
    'listen',
    'sleep',
    'spawn',
    'timeout',
    'websocket',
    'CooperativeLock',
    'Event',
    'socket',
    'ssl',
    'subprocess',
    'green_os',
    'green_threading',
    'green_http_client',
    'CONTINUE',
    'HTTPConnection',
    'HTTPResponse',
    'HTTPSConnection',
    'ImproperConnectionState',
    '_UNKNOWN',
    'urllib_request',
    'getcurrent',
    'Pool',
    'Empty',
    'LightQueue',
    'Queue',
    'Semaphore',
    'hub_exceptions',
    'hub_prevent_multiple_readers',
    'monkey_patch',
    'shutdown_safe',
    'spawn_n',
    'ChunkReadError',
]
