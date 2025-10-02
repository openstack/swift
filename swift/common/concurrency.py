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
import threading
import time
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
from eventlet import GreenPile, GreenPool  # noqa: F401
from eventlet import greenio, greenpool, hubs, patcher, queue, tpool, wsgi
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
from eventlet.queue import Empty, LightQueue, Queue  # noqa: F401
from eventlet.semaphore import Semaphore
from eventlet.support.greenlets import GreenletExit  # noqa: F401
import eventlet.green.profile as eprofile  # noqa: F401
hub_exceptions = eventlet.debug.hub_exceptions
hub_prevent_multiple_readers = eventlet.debug.hub_prevent_multiple_readers
monkey_patch = eventlet.patcher.monkey_patch
shutdown_safe = eventlet.greenio.shutdown_safe
ChunkReadError = eventlet.wsgi.ChunkReadError

if USE_EVENTLET:
    from eventlet.pools import Pool
    from eventlet import sleep

    from eventlet import Timeout as _Timeout

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

else:
    import os as green_os  # noqa: F811
    import socket  # noqa: F811
    import ssl  # noqa: F811
    import threading as green_threading  # noqa: F811
    import urllib.request as urllib_request  # noqa: F811

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

    def spawn(func, *args, **kwargs):
        return ThreadResult(func, args, kwargs)

    # eventlet spawn_n is spawn without a return value; reuse spawn here.
    spawn_n = spawn

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
    'debug',
    'greenio',
    'greenthread',
    'hubs',
    'patcher',
    'queue',
    'wsgi',
    'GreenPile',
    'Timeout',
    'greenpool',
    'tpool',
    'listen',
    'sleep',
    'spawn',
    'timeout',
    'websocket',
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
    'Semaphore',
    'hub_exceptions',
    'hub_prevent_multiple_readers',
    'monkey_patch',
    'shutdown_safe',
    'ChunkReadError',
]
