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

import eventlet
import eventlet.debug
import eventlet.greenio
import eventlet.greenthread
import eventlet.hubs
import eventlet.patcher
import eventlet.queue
import eventlet.semaphore
import eventlet.wsgi

from eventlet import GreenPile, GreenPool, Timeout
from eventlet import greenio, greenpool, hubs, patcher, queue, tpool, wsgi
from eventlet import debug, listen, sleep, spawn, timeout, websocket
from eventlet import greenthread

from eventlet.event import Event
from eventlet.green import socket, ssl, subprocess
from eventlet.green import os as green_os
from eventlet.green import threading as green_threading
from eventlet.green.http import client as green_http_client
from eventlet.green.http.client import CONTINUE, HTTPConnection, \
    HTTPResponse, HTTPSConnection, ImproperConnectionState, _UNKNOWN
from eventlet.green.urllib import request as urllib_request
from eventlet.greenthread import getcurrent, spawn as greenthread_spawn
from eventlet.hubs import trampoline
from eventlet.pools import Pool
from eventlet.queue import Empty, LightQueue, Queue
from eventlet.semaphore import Semaphore
from eventlet.support.greenlets import GreenletExit
import eventlet.green.profile as eprofile

hub_exceptions = eventlet.debug.hub_exceptions
hub_prevent_multiple_readers = eventlet.debug.hub_prevent_multiple_readers
monkey_patch = eventlet.patcher.monkey_patch
shutdown_safe = eventlet.greenio.shutdown_safe
spawn_n = eventlet.spawn_n
ChunkReadError = eventlet.wsgi.ChunkReadError

# flake8 raises a F401 without this
__all__ = [
    'debug',
    'greenio',
    'greenthread',
    'hubs',
    'patcher',
    'queue',
    'wsgi',
    'GreenPile',
    'GreenPool',
    'Timeout',
    'greenio',
    'greenpool',
    'hubs',
    'patcher',
    'queue',
    'tpool',
    'wsgi',
    'debug',
    'listen',
    'sleep',
    'spawn',
    'timeout',
    'websocket',
    'greenthread',
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
    'greenthread_spawn',
    'trampoline',
    'Pool',
    'Empty',
    'LightQueue',
    'Queue',
    'Semaphore',
    'GreenletExit',
    'eprofile',
    'hub_exceptions',
    'hub_prevent_multiple_readers',
    'monkey_patch',
    'shutdown_safe',
    'spawn_n',
    'ChunkReadError',
]
