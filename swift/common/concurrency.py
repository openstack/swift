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

import importlib.util
import os
import time
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
from eventlet import (  # noqa: F401
    debug, listen, sleep, spawn, timeout, websocket)
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
from eventlet.pools import Pool  # noqa: F401
from eventlet.queue import Empty, LightQueue, Queue  # noqa: F401
from eventlet.semaphore import Semaphore
from eventlet.support.greenlets import GreenletExit  # noqa: F401
import eventlet.green.profile as eprofile  # noqa: F401
hub_exceptions = eventlet.debug.hub_exceptions
hub_prevent_multiple_readers = eventlet.debug.hub_prevent_multiple_readers
monkey_patch = eventlet.patcher.monkey_patch
shutdown_safe = eventlet.greenio.shutdown_safe
spawn_n = eventlet.spawn_n
ChunkReadError = eventlet.wsgi.ChunkReadError

if USE_EVENTLET:

    from eventlet import Timeout as _Timeout

    class Timeout(_Timeout):
        def __init__(self, *args, **kwargs):
            # Drop the socket kwarg; eventlet's Timeout doesn't accept it.
            kwargs.pop('socket', None)
            super(Timeout, self).__init__(*args, **kwargs)

        def check_time(self):
            # Only needed without eventlet
            pass


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
    'Empty',
    'LightQueue',
    'Semaphore',
    'hub_exceptions',
    'hub_prevent_multiple_readers',
    'monkey_patch',
    'shutdown_safe',
    'ChunkReadError',
]
